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
func insertApps(
	insertApp *InsertApp,
	client gomemcache.Client,
	memcAddr string,
	retryCount int,
	dry bool)(bool) {
	isProcessed := true
	key := fmt.Sprintf("%s:%s", insertApp.appsInstalled.devType, insertApp.appsInstalled.devId)
	if dry {
		log.Printf("%s - %s ", memcAddr, key)
	} else {
		item := &gomemcache.Item{Key: key, Flags: 9, Expiration: 60 * 60, Value: []byte(insertApp.packed)}
		if err := client.Set(item); err != nil {
			isProcessed = false
			for i:=0; i< retryCount; i++ {
				if err := client.Set(item); err == nil {
					isProcessed =  false
					break
				}
			}
			if !isProcessed {
				log.Printf("Canntot write to: %v ", err)
			}
		}
	}
	return isProcessed

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
	clients map[string]*gomemcache.Client,
	deviceMemc  map[string]string,
	wg *sync.WaitGroup,
	NormalErrorRate float64,
	retryCount int,
	dry bool) {
	start := time.Now()
	processed := 0
	errorsNum := 0
	f, err := os.Open(file)
	if err != nil {
		log.Println(err)
		wg.Done()
	}
	defer f.Close()
	gf, err := gzip.NewReader(f)
	if err != nil {
		log.Println(err)
		wg.Done()
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
			fmt.Println(err)
		}
		insertApp := &InsertApp{appsInstalled: *appsinstalled, packed: *packed, ua: *ua, filename: file}


		isProcessed := insertApps(
			insertApp, *clients[appsinstalled.devType],
			deviceMemc[appsinstalled.devType],
			retryCount,
			dry)
		if isProcessed {
			processed += 1
		} else {
			errorsNum += 1
		}
	}
	log.Printf("Worker with file %s end at %s", file, time.Since(start))
	log.Printf("For  %s file proccessed: %d, errors: %d ", file, processed, errorsNum)
	errRate := float64(errorsNum) / float64(processed)
	if errRate < NormalErrorRate {
		log.Printf("Acceptable error rate (%.5f). Successfull load", errRate)
	} else {
		log.Printf("High error rate (%.5f > %.5f). Failed load", errRate, NormalErrorRate)
	}
	wg.Done()
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

	if len(files) == 0{
		log.Println("Files not found")
	} else {
		sort.Strings(files)
		var wg sync.WaitGroup
		for _, file := range files {
			log.Printf("start worker for %s", file)
			wg.Add(1)
			go fileWorker(file, clients, deviceMemc,  &wg, NormalErrorRate, *retryCount, *dry)
		}
		wg.Wait()
		for _, file := range files {
			dotRename(file)
		}
	}
	log.Printf("Program Exit")
}