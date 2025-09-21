package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	NormalErrRate = 0.01
	MaxWorkers    = 10
	SocketTimeout = 3 * time.Second
)

type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []int32
}

type Options struct {
	Test    bool
	Log     string
	Dry     bool
	Pattern string
	Idfa    string
	Gaid    string
	Adid    string
	Dvid    string
}

var memcClients = make(map[string]*memcache.Client)
var memcMutex sync.RWMutex

func getMemcClient(memcAddr string) *memcache.Client {
	memcMutex.Lock()
	defer memcMutex.Unlock()
	if client, exists := memcClients[memcAddr]; exists {
		return client
	}
	client := memcache.New(memcAddr)
	client.Timeout = SocketTimeout
	memcClients[memcAddr] = client
	return client
}

func dotRename(path string) error {
	dir, fn := filepath.Split(path)
	return os.Rename(path, filepath.Join(dir, "."+fn))
}

func insertAppsinstalled(memcAddr string, appsinstalled AppsInstalled, dryRun bool, maxRetries, retryTimeout int) bool {
	ua := &UserApps{
		Lat:  appsinstalled.Lat,
		Lon:  appsinstalled.Lon,
		Apps: make([]uint32, len(appsinstalled.Apps)),
	}
	for i, app := range appsinstalled.Apps {
		ua.Apps[i] = uint32(app)
	}
	key := fmt.Sprintf("%s:%s", appsinstalled.DevType, appsinstalled.DevID)
	packed, err := ua.Marshal()
	if err != nil {
		log.Printf("Protobuf marshal error: %v", err)
		return false
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if dryRun {
			log.Printf("%s - %s -> %s", memcAddr, key, ua.String())
			return true
		}
		memc := getMemcClient(memcAddr)
		err := memc.Set(&memcache.Item{Key: key, Value: packed})
		if err == nil {
			return true
		}
		log.Printf("Memcache set error, attempt %d, key=%s, memc_addr=%s: %v", attempt, key, memcAddr, err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(retryTimeout) * time.Second)
		}
	}
	return false
}

func parseAppsinstalled(line string) (*AppsInstalled, error) {
	parts := strings.Split(strings.TrimSpace(line), "\t")
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid line format")
	}
	devType, devID, latStr, lonStr, rawApps := parts[0], parts[1], parts[2], parts[3], parts[4]
	if devType == "" || devID == "" {
		return nil, fmt.Errorf("missing dev_type or dev_id")
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid lat: %v", err)
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid lon: %v", err)
	}
	appStrs := strings.Split(rawApps, ",")
	var apps []int32
	for _, a := range appStrs {
		if id, err := strconv.Atoi(strings.TrimSpace(a)); err == nil {
			apps = append(apps, int32(id))
		} else {
			log.Printf("Not all user apps are digits: %s", a)
		}
	}
	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     lat,
		Lon:     lon,
		Apps:    apps,
	}, nil
}

func processFile(fn string, deviceMemc map[string]string, dryRun bool) (int, int) {
	processed := 0
	errors := 0
	log.Printf("Processing %s", fn)

	file, err := os.Open(fn)
	if err != nil {
		log.Printf("Error opening file %s: %v", fn, err)
		return 0, 1
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("Error creating gzip reader for %s: %v", fn, err)
		return 0, 1
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	var wg sync.WaitGroup
	sem := make(chan struct{}, MaxWorkers)
	var mu sync.Mutex

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		appsinstalled, err := parseAppsinstalled(line)
		if err != nil {
			log.Printf("Parse error: %v", err)
			mu.Lock()
			errors++
			mu.Unlock()
			continue
		}
		memcAddr, exists := deviceMemc[appsinstalled.DevType]
		if !exists {
			log.Printf("Unknown device type: %s", appsinstalled.DevType)
			mu.Lock()
			errors++
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(memcAddr string, ai AppsInstalled) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if insertAppsinstalled(memcAddr, ai, dryRun, 3, 1) {
				mu.Lock()
				processed++
				mu.Unlock()
			} else {
				mu.Lock()
				errors++
				mu.Unlock()
			}
		}(memcAddr, *appsinstalled)
	}

	wg.Wait()
	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error: %v", err)
		mu.Lock()
		errors++
		mu.Unlock()
	}

	return processed, errors
}

func mainFunc(options Options) {
	deviceMemc := map[string]string{
		"idfa": options.Idfa,
		"gaid": options.Gaid,
		"adid": options.Adid,
		"dvid": options.Dvid,
	}

	files, err := filepath.Glob(options.Pattern)
	if err != nil {
		log.Fatalf("Glob error: %v", err)
	}
	sort.Strings(files)

	for _, fn := range files {
		processed, errors := processFile(fn, deviceMemc, options.Dry)
		if processed == 0 {
			if err := dotRename(fn); err != nil {
				log.Printf("Dot rename error: %v", err)
			}
			continue
		}

		errRate := float64(errors) / float64(processed)
		if errRate < NormalErrRate {
			log.Printf("Acceptable error rate %.4f. Successful load", errRate)
		} else {
			log.Printf("High error rate (%.4f > %.4f). Failed load", errRate, NormalErrRate)
		}
		if err := dotRename(fn); err != nil {
			log.Printf("Dot rename error: %v", err)
		}
	}
}

func prototest() {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	for _, line := range strings.Split(sample, "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) < 5 {
			continue
		}
		devType, devID, latStr, lonStr, rawApps := parts[0], parts[1], parts[2], parts[3], parts[4]
		lat, _ := strconv.ParseFloat(latStr, 64)
		lon, _ := strconv.ParseFloat(lonStr, 64)
		appStrs := strings.Split(rawApps, ",")
		var apps []int32
		for _, a := range appStrs {
			if id, err := strconv.Atoi(strings.TrimSpace(a)); err == nil {
				apps = append(apps, int32(id))
			}
		}
		ua := &UserApps{
			Lat:  lat,
			Lon:  lon,
			Apps: make([]uint32, len(apps)),
		}
		for i, app := range apps {
			ua.Apps[i] = uint32(app)
		}
		packed, _ := ua.Marshal()
		unpacked := &UserApps{}
		unpacked.Unmarshal(packed)
		if ua.String() != unpacked.String() {
			log.Fatalf("Protobuf test failed")
		}
	}
	log.Println("Protobuf test passed")
}

func main() {
	opts := Options{}
	flag.BoolVar(&opts.Test, "test", false, "Run protobuf test")
	flag.StringVar(&opts.Log, "log", "", "Log file")
	flag.BoolVar(&opts.Dry, "dry", false, "Dry run")
	flag.StringVar(&opts.Pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "File pattern")
	flag.StringVar(&opts.Idfa, "idfa", "127.0.0.1:33013", "Idfa memcached address")
	flag.StringVar(&opts.Gaid, "gaid", "127.0.0.1:33014", "Gaid memcached address")
	flag.StringVar(&opts.Adid, "adid", "127.0.0.1:33015", "Adid memcached address")
	flag.StringVar(&opts.Dvid, "dvid", "127.0.0.1:33016", "Dvid memcached address")
	flag.Parse()

	if opts.Log != "" {
		file, err := os.OpenFile(opts.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Log file error: %v", err)
		}
		log.SetOutput(file)
	}

	if opts.Test {
		prototest()
		os.Exit(0)
	}

	log.Printf("Memc loader started with options: %+v", opts)
	defer func() {
		memcMutex.Lock()
		for _, client := range memcClients {
			client.Close()
		}
		memcMutex.Unlock()
	}()

	mainFunc(opts)
}
