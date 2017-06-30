package filter

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	inputFile     string
	filteredFile  string
	outputFile    string
	requestSorted []string
	requests      []Request
	videoCount    map[string]int
)

type Request struct {
	time    int
	userID  string
	videoID string
}

type sortedMap struct {
	m map[string]int
	s []string
}

func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] < sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]int, order string) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key := range m {
		sm.s[i] = key
		i++
	}
	if order == "ASC" {
		sort.Stable(sm)
	} else if order == "DESC" {
		sort.Stable(sort.Reverse(sm))
	}
	return sm.s
}

func ReadArgs(args []string) (err error, path string, inputFileName string, comma rune, column []int, isURL bool, fileLimit int, timeThreshold int) {
	if len(args) < 11 {
		err = errors.New("Not Enough Arguments")
		return
	}
	path = args[2]
	inputFileName = args[3]
	comma = []rune(args[4])[0]
	colTime, err := strconv.Atoi(args[5])
	if err != nil {
		panic(err)
	}
	colClient, err := strconv.Atoi(args[6])
	if err != nil {
		panic(err)
	}
	colFile, err := strconv.Atoi(args[7])
	if err != nil {
		panic(err)
	}
	column = []int{colTime, colClient, colFile}
	isURL, err = strconv.ParseBool(args[8])
	if err != nil {
		panic(err)
	}
	fileLimit, err = strconv.Atoi(args[9])
	if err != nil {
		panic(err)
	}
	timeThreshold, err = strconv.Atoi(args[10])
	if err != nil {
		panic(err)
	}
	return
}

func FilterLog(path string, inputFileName string, comma rune, column []int, isURL bool, fileLimit int, timeThreshold int) {
	inputFile = path + inputFileName
	filteredFile = path + inputFileName + "_filtered.csv"
	outputFile = path + inputFileName + "_" + strconv.Itoa(fileLimit) + "_" + strconv.Itoa(timeThreshold) + ".csv"
	if _, err := os.Stat(filteredFile); os.IsNotExist(err) {
		readInputFile(comma, column, isURL)
	}
	readFilteredFile(timeThreshold)
	writeOutputFile(fileLimit)
}

func readInputFile(comma rune, column []int, isURL bool) {
	file, _ := os.Open(inputFile)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = comma
	colTime, colClient, colFile := column[0], column[1], column[2]
	requestTime := make(map[string]int)

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		videoID := rec[colFile]
		if isURL {
			if !strings.Contains(videoID, "youtube.com/watch?v=") {
				continue
			}
			videoID = strings.Split(videoID, "v=")[1]
		}
		if strings.Index(videoID, "&") != -1 {
			videoID = strings.Split(videoID, "&")[0]
		}
		if strings.Index(videoID, "#") != -1 {
			videoID = strings.Split(videoID, "#")[0]
		}

		visitTime, err := strconv.ParseFloat(rec[colTime], 64)
		if err != nil {
			panic(err)
		}
		visitTimeInt := int(visitTime)

		userID := rec[colClient]
		request := strconv.Itoa(visitTimeInt) + "\t" + userID + "\t" + videoID + "\n"
		requestTime[request] = visitTimeInt
	}

	requestSorted = sortedKeys(requestTime, "ASC")
	writeFilteredFile()
}

func writeFilteredFile() {
	writer, _ := os.Create(filteredFile)
	defer writer.Close()
	for _, request := range requestSorted {
		writer.WriteString(request)
	}
}

func readFilteredFile(threshold int) {
	file, _ := os.Open(filteredFile)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = 3

	requests = make([]Request, 0)
	lastRequestTime := make(map[string]int)
	videoCount = make(map[string]int)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		requestTime, _ := strconv.Atoi(record[0])
		userID := record[1]
		videoID := record[2]

		if requestTime-lastRequestTime[userID+videoID] < threshold {
			continue
		}
		lastRequestTime[userID+videoID] = requestTime
		requests = append(requests, Request{requestTime, userID, videoID})
		videoCount[videoID]++
	}
}

func writeOutputFile(limit int) {
	videoSorted := sortedKeys(videoCount, "DESC")
	isPopular := make(map[string]bool)
	numberPopular := limit
	for i := 0; i < numberPopular; i++ {
		isPopular[videoSorted[i]] = true
	}
	for videoCount[videoSorted[numberPopular]] == videoCount[videoSorted[numberPopular-1]] {
		isPopular[videoSorted[numberPopular]] = true
		numberPopular++
	}

	videoCount = make(map[string]int)
	userCount := make(map[string]int)
	requestCount := 0

	writer, _ := os.Create(outputFile)
	defer writer.Close()

	for _, request := range requests {
		videoId := request.videoID
		if !isPopular[videoId] {
			continue
		}

		requestTime := request.time
		userId := request.userID
		log := strconv.Itoa(requestTime) + "\t" + userId + "\t" + videoId + "\n"
		fmt.Print(log)
		writer.WriteString(log)
		videoCount[videoId]++
		userCount[userId]++
		requestCount++
	}
	for videoId, count := range videoCount {
		fmt.Printf("%s: %d\n", videoId, count)
	}
	fmt.Printf("User: %d\nVideo: %d\nRequest: %d\n", len(userCount), len(videoCount), requestCount)
}
