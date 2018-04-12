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
	"sync"
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

func dotRename(fp string) {
	head, fn := path.Split(fp)
	os.Rename(fp, filepath.Join(head, "."+fn))
}

// Create connection to memcached
func generateClients(connections map[string]string) map[string]*gomemcache.Client {
	var clients = make(map[string]*gomemcache.Client)
	for i, addr := range connections {
		client, err := gomemcache.NewClient([]string{addr})
		if err != nil {
			log.Fatalf("init client error: %v", err)
		}
		client.SetMaxIdleConns(10)
		clients[i] = client
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

//  Insert Apps to memcache
func insertAppsWorker(
	insertAppChannel <-chan *InsertApp,
	client gomemcache.Client,
	memcAddr string,
	retryCount int,
	dry bool) {

	for insertApp := range insertAppChannel {
		isProcessed := true
		key := fmt.Sprintf("%s:%s", insertApp.appsInstalled.devType, insertApp.appsInstalled.devId)
		if dry {
			log.Printf("%s - %s ", memcAddr, key)
		} else {
			item := &gomemcache.Item{Key: key, Flags: 9, Expiration: 60 * 60, Value: []byte(insertApp.packed)}
			if err := client.Set(item); err != nil {
				isProcessed = false
				for i := 0; i < retryCount; i++ {
					if err := client.Set(item); err == nil {
						isProcessed = true
						break
					}
				}
				if !isProcessed {
					log.Printf("Canntot write to: %v ", err)
				}
			}
		}
	}
}

// Parsed file line and append line data to Appinstalled
func parseAppsinstalled(line string) (*AppsInstalled, error) {
	words := strings.Split(strings.TrimSpace(line), "\t")
	if len(words) < 5 {
		return nil, errors.New("Invalid string line.\t")
	}
	devType := words[0]
	devId := words[1]
	if len(devType) == 0 || len(devId) == 0 {
		return nil, errors.New("Don't have divId or devType\t")
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

// Reading file and log statistics
func fileWorker(
	file string,
	insertAppChannels map[string]chan *InsertApp,
	wg *sync.WaitGroup,
	NormalErrorRate float64) {
	start := time.Now()
	processedNum := 0
	errorsNum := 0
	defer wg.Done()
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
			errorsNum += 1
			continue
		}
		packed, ua, err := serializeData(appsinstalled)
		if err != nil {
			log.Println(err)
			errorsNum += 1
			continue
		}
		insertApp := &InsertApp{appsInstalled: *appsinstalled, packed: *packed, ua: *ua, filename: file}
		insertAppChannels[appsinstalled.devType] <- insertApp
		processedNum += 1
	}

	log.Printf("Worker with file %s end at %s", file, time.Since(start))
	log.Printf("For  %s file proccessed: %d, errors: %d ", file, processedNum, errorsNum)
	errRate := float64(errorsNum) / float64(processedNum)
	if errRate < NormalErrorRate {
		log.Printf("Acceptable error rate (%.5f). Successfull load", errRate)
	} else {
		log.Printf("High error rate (%.5f > %.5f). Failed load", errRate, NormalErrorRate)
	}
}

func main() {
	log.Printf("Programm Start")
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
	clients := generateClients(deviceMemc)

	allFiles, _ := filepath.Glob(*pattern)
	files := make([]string, 0)
	for _, file := range allFiles {
		_, fn := path.Split(file)
		if !strings.HasPrefix(fn, ".") {
			files = append(files, file)
		}
	}

	if len(files) == 0 {
		log.Println("Files not found")
		log.Printf("Program Exit")
		os.Exit(3)
	}
	insertAppChannels := make(map[string](chan *InsertApp))
	for key, client := range clients {
		insertAppChannels[key] = make(chan *InsertApp, Buffer)
		go insertAppsWorker(insertAppChannels[key], *client, deviceMemc[key], *retryCount, *dry)
		}
	sort.Strings(files)
	var wg sync.WaitGroup
	for _, file := range files {
		log.Printf("start worker for %s", file)
		wg.Add(1)
		go fileWorker(file, insertAppChannels, &wg, NormalErrorRate)
	}
	wg.Wait()
	for _, file := range files {
		dotRename(file)
	}
	log.Printf("Program Exit")
}