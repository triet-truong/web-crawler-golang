package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	rpm := 2000
	var rps = rpm / 60
	const fileMaxSizeInMb = 100
	RequestToCDN(
		"./input-test",
		"./output/failed/failed-url.txt",
		rps,
		fileMaxSizeInMb)
}

func RequestToCDN(inputUrlDir, failedUrlDir string, rps int, fileMaxSizeInMb int) {
	//Start BoltDB
	db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return
	}

	//Create bucket
	urlBucketName := "UrlBucket"
	fileBucketName := "FileBucket"
	db.Update(func(tx *bolt.Tx) error {
		// tx.DeleteBucket([]byte(urlBucketName))
		_, err := tx.CreateBucketIfNotExists([]byte(urlBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(fileBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100
	var netClient = &http.Client{
		Timeout:   time.Second * 10,
		Transport: t,
	}
	rqResChan := make(chan [2]string, 100)
	rqFailedChan := make(chan string, 100)
	fileCheckpointChannel := make(chan [2]string, 100)
	quitCh := make(chan int)

	inputFiles, e := ioReadDir(inputUrlDir)
	check(e)

	go func() {
		fileTotal := len(inputFiles)
		for i, file := range inputFiles {
			printProgressBar("Overall progress", uint(i+1), uint(fileTotal), uint(fileTotal))
			fmt.Println("")
			scanFileAndRequest(netClient, db, urlBucketName, fileBucketName, file, rps, rqResChan, rqFailedChan, fileCheckpointChannel)
			fmt.Printf("%v done\n", file)
		}
		quitCh <- 1
	}()

	failedWebpUrlLogger := createLogger(failedUrlDir, fileMaxSizeInMb)

	for {
		select {
		case urlRespTime := <-rqResChan:
			putKey(db, urlBucketName, urlRespTime[0], urlRespTime[1])
		case failedUrl := <-rqFailedChan:
			failedWebpUrlLogger.Println(failedUrl)
		case fileCheckpoint := <-fileCheckpointChannel:
			putKey(db, fileBucketName, fileCheckpoint[0], fileCheckpoint[1])
		case <-quitCh:
			return
		}
	}
}

func requestWebpUrl(netClient *http.Client, imgUrl string, ch chan [2]string, rqFailedChan chan string) {
	var elapsedTime int64
	var e error
	for retries, maxRetry := 0, 3; retries <= maxRetry; retries++ {
		start := time.Now()
		_, e = netClient.Head(imgUrl)
		elapsedTime = time.Since(start).Milliseconds()
		if e == nil {
			break
		}
		fmt.Printf("Retry %v: %v\n", retries, imgUrl)
		time.Sleep(500 * time.Millisecond)
	}

	if e != nil {
		check(e)
		rqFailedChan <- imgUrl
		return
	}

	ch <- [2]string{imgUrl, strconv.FormatInt(elapsedTime, 10)}
}

func check(e error) {
	if e != nil {
		fmt.Println(e.Error())
	}
}

func handleFileErr(e error) {
	if e != nil {
		fmt.Printf("error opening file: %v\n", e)
		os.Exit(1)
	}
}

func printProgressBar(message string, current, target, scale uint) {
	finished := int(float64(current) / float64(target) * float64(scale))
	// fmt.Printf("%d/%d\n", current, target)
	completedPercent := uint((float32(current) / float32(target)) * 100)
	var sharps strings.Builder
	for i := 1; i <= finished; i++ {
		sharps.WriteString("#")
	}
	var minus strings.Builder
	for i := uint(finished + 1); i <= scale; i++ {
		minus.WriteString("_")
	}

	fmt.Printf("\r%v %v%% [%v%v] (%v/%v)", message, completedPercent, sharps.String(), minus.String(), current, target)
}

func ioReadDir(root string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(root)
	if err != nil {
		return files, err
	}
	for _, file := range fileInfo {
		if file != nil || file.Name() != "" {
			filePath := fmt.Sprintf("%v/%v", root, file.Name())
			files = append(files, filePath)
		}

	}
	return files, nil
}

func scanFileAndRequest(netClient *http.Client, db *bolt.DB, urlBucketName, fileBucketName, filePath string, rps int, rqResChan chan [2]string, rqFailedChan chan string, fileCheckpointChan chan [2]string) {
	urlFile, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	handleFileErr(err)
	scanner := bufio.NewScanner(urlFile)
	fileLines := calculateFileLines(filePath)

	start, err := strconv.ParseUint(getKey(db, fileBucketName, filePath), 10, 64)
	if err != nil {
		start = uint64(0)
	}

	i := uint64(0)
	updateCacheThreshold := uint64(rps * 3)
	for ; scanner.Scan(); i++ {
		if i >= start {
			url := scanner.Text()
			if getKey(db, urlBucketName, url) == "" {
				go requestWebpUrl(netClient, url, rqResChan, rqFailedChan)
				time.Sleep(time.Duration(1000/rps) * time.Millisecond)
			}
			if i%updateCacheThreshold == 0 {
				fileCheckpointChan <- [2]string{filePath, strconv.FormatUint(i, 10)}
			}
			printProgressBar("File progress", uint(i), fileLines, uint(20))
		}

	}

	fileCheckpointChan <- [2]string{filePath, strconv.FormatUint(i, 10)}
}

func createLogger(fileName string, fileMaxSizeInMb int) *log.Logger {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	handleFileErr(err)
	fileLogger := log.New(file, "", log.Lmsgprefix)
	fileLogger.SetOutput(&lumberjack.Logger{
		Filename: fileName,
		MaxSize:  fileMaxSizeInMb, // megabytes after which new file is created
	})
	return fileLogger
}

func calculateFileLines(inputFilePath string) uint {
	inputFile, e := os.OpenFile(inputFilePath, os.O_RDONLY, fs.FileMode(os.O_RDONLY))
	if e != nil {
		fmt.Println("Error calculating file lines")
		return 0
	}
	scanner := bufio.NewScanner(inputFile)
	fileLines := uint(0)
	for scanner.Scan() {
		fileLines++
	}
	return fileLines
}

func getKey(db *bolt.DB, bucket, key string) string {
	rs := ""
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		rs = string(b.Get([]byte(key)))
		return nil
	})
	return rs
}

func putKey(db *bolt.DB, bucket, key, value string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put([]byte(key), []byte(value))
		return err
	})
}
