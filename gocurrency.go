package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"gocurrency/appsinstalled_pb2"
	"github.com/golang/protobuf/proto"
	"github.com/zeayes/gomemcache"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"strconv"
	"time"
)

const (
	Buffer          int     = 200
	NormalErrorRate float64 = 0.01
	Pattern         string  = "/home/assig/pysrc/otus/12_concurrency/concurrency/data2/*.tsv.gz"
)

type AppsInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

type InsertApp struct {
	appsInstalled AppsInstalled
	packed        []byte
	ua            appsinstalled_pb2.UserApps
	filename      string
}

type Statistic struct {
	processed int64
	errors    int64
}

func dotRename(fp string, head string, fn string) {
		os.Rename(fp, filepath.Join(head, "."+fn))
}

// Create connection to memcached
func generateClients(connections map[string]string) map[string]gomemcache.Client {
	var clients = make(map[string]gomemcache.Client)
	for i, addr := range connections {
		client, err := gomemcache.NewClient([]string{addr})
		if err != nil {
			log.Fatalf("init client error: %v", err)
		}
		clients[i] = *client
	}
	return clients
}

// Serialized Data with protobuf
func serializeData(appsinstalled *AppsInstalled) (*[]byte, *appsinstalled_pb2.UserApps, error) {
	ua := appsinstalled_pb2.UserApps{}
	ua.Lat = &appsinstalled.lat
	ua.Lon = &appsinstalled.lon
	ua.Apps = appsinstalled.apps
	packed, err := proto.Marshal(&ua)
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("marshaling error: %s", err))
	}
	return &packed, &ua, nil
}

// Worker for connection to memcached
func insertAppsWorker(
	insertAppChannel <-chan *InsertApp,
	client gomemcache.Client,
	isProcessedMap map[string]chan bool,
	memcAddr string,
	retryCount int,
	dry bool) {
	for {
		insertApp := <-insertAppChannel
		key := fmt.Sprintf("%s:%s", insertApp.appsInstalled.devType, insertApp.appsInstalled.devId)
		if dry {
			log.Printf("%s - %s ", memcAddr, key)
			isProcessedMap[insertApp.filename] <- true
		} else {
			item := &gomemcache.Item{Key: key, Flags: 9, Expiration: 60 * 60, Value: []byte(insertApp.packed)}
			if err := client.Set(item); err != nil {
				for i:=0; i< retryCount; i++ {
					if err := client.Set(item); err == nil {
						isProcessedMap[insertApp.filename] <- true
						break
					}
				}
				isProcessedMap[insertApp.filename] <- false
				log.Printf("Canntot write to: %v ", err)
			} else {
				isProcessedMap[insertApp.filename] <- true
			}
		}
	}
}

// Parsed file line and append line data to Appinstalled
func parseAppsinstalled(line string) (*AppsInstalled, error) {
	words := strings.Split(strings.TrimSpace(line), "\t")
	if len(words) < 5 {
		return nil, errors.New("Invalid string line.")
	}
	devType := words[0]
	devId := words[1]
	if len(devType) == 0 || len(devId) == 0 {
		return nil, errors.New("Don't have divId or devType")
	}
	lat, err := strconv.ParseFloat(words[2], 64)
	if err != nil {
		lat = 0
		log.Printf("invalid lat of geocoords: `%s`", line)
	}
	lon, err := strconv.ParseFloat(words[2], 64)
	if err != nil {
		lon = 0
		log.Printf("invalid lon of geocoords: `%s`", line)
	}
	appsLine := strings.Split(strings.TrimSpace(words[4]), ",")

	apps := make([]uint32, 0, 30)
	isAppsNotAllDigits := false
	for _, app := range appsLine {
		if number, err := strconv.ParseUint(app, 10, 32); err == nil {
			apps = append(apps, uint32(number))
		} else {
			isAppsNotAllDigits = true
		}
		if isAppsNotAllDigits {
			log.Printf("not all user apps are digits: `%s`", line)
		}
	}
	return &AppsInstalled{
		devType: devType,
		devId:   devId,
		lat:     lat,
		lon:     lon,
		apps:    apps,
	}, nil
}

// File worker
func worker(
	file string,
	insertAppChannels map[string]chan *InsertApp,
	statisticChan chan *Statistic,
	isProcessedChan chan bool,
	NormalErrorRate float64) {
	start := time.Now()
	statistic := &Statistic{processed: 0, errors: 0}

	f, err := os.Open(file)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	gf, err := gzip.NewReader(f)
	if err != nil {
		log.Println(err)
	}
	defer gf.Close()
	scanner := bufio.NewScanner(gf)

	for scanner.Scan() {
		appsinstalled, err := parseAppsinstalled(scanner.Text())
		if err != nil {
			log.Println(err)
			statistic.errors += 1
			continue
		}
		packed, ua, err := serializeData(appsinstalled)
		if err != nil {
			fmt.Println(err)
		}
		insertApp := &InsertApp{appsInstalled: *appsinstalled, packed: *packed, ua: *ua, filename: file}
		insertAppChannels[appsinstalled.devType] <- insertApp

		isProcessed := <-isProcessedChan
		if isProcessed {
			statistic.processed += 1
		} else {
			statistic.errors += 1
		}
	}
	errRate := float64(statistic.errors) / float64(statistic.processed)
	if errRate < NormalErrorRate {
		log.Printf("Acceptable error rate (%.3f). Successfull load", errRate)
	} else {
		log.Printf("High error rate (%.3f > %.3f). Failed load", errRate, NormalErrorRate)
	}
	log.Printf("Worker with file %s end at %s", file, time.Since(start))
	statisticChan <- statistic
}

func main() {
	idfa := flag.String("idfa", "127.0.0.1:33013", "memcash  ip:port for iphone ids")
	gaid := flag.String("gaid", "127.0.0.1:33014", "memcash  ip:port for android gaid")
	adid := flag.String("adid", "127.0.0.1:33015", "memcash  ip:port for android adid")
	dvid := flag.String("dvid", "127.0.0.1:33016", "memcash  ip:port for android dvid")
	dry := flag.Bool("dry", false, "Debug mode")
	retryCount := flag.Int("retryCount", 5, "count of retry set data in memcache")
	pattern := flag.String("pattern", Pattern, "Pattern for files path")
	flag.Parse()
	deviceMemc := map[string]string{
		"idfa": *idfa,
		"gaid": *gaid,
		"adid": *adid,
		"dvid": *dvid,
	}
	log.Printf("Programm Start")
	files, _ := filepath.Glob(*pattern)
	sort.Strings(files)
	connections := make(map[string]string)
	connections["idfa"] = *idfa
	connections["gaid"] = *gaid
	connections["adid"] = *adid
	connections["dvid"] = *dvid
	clients := generateClients(connections)

	isProcessedMap := make(map[string]chan bool)
	for _, file := range files {
		isProcessedChan := make(chan bool)
		isProcessedMap[file] = isProcessedChan
	}
	insertAppChannels := make(map[string](chan *InsertApp))
	for key, client := range clients {
		insertAppChannels[key] = make(chan *InsertApp, Buffer)
		go insertAppsWorker(insertAppChannels[key], client, isProcessedMap, deviceMemc[key], *retryCount, *dry)
	}

	Statistics := make(map[string](chan *Statistic))
	for _, file := range files {
		_, fn := path.Split(file)
		if !strings.HasPrefix(fn, ".") {
			log.Printf("start worker")
			statisticChan := make(chan *Statistic)
			Statistics[file] = statisticChan
			go worker(file, insertAppChannels, statisticChan, isProcessedMap[file], NormalErrorRate)
			log.Printf(file)
		}
	}
	for _, file := range files{
		head, fn := path.Split(file)
		if !strings.HasPrefix(fn, ".") {
			statistic := <-Statistics[file]
			log.Printf("For  %s file proccessed: %d, errors: %d ", file, statistic.processed, statistic.errors)
			dotRename(file, head, fn)
		}
	}
	log.Printf("Program Exit")
}