package generator

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

type Video struct {
	ID        string
	ParseTime []int
	ViewCount []int
}

func (v Video) Append(time, count int) Video {
	v.ParseTime = append(v.ParseTime, time)
	v.ViewCount = append(v.ViewCount, count)
	return v
}

func readParsedVideos(fileDir string) []Video {
	videoMap := map[string]Video{}

	files, err := ioutil.ReadDir(fileDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if file.Name() == "users" {
			continue
		}
		f, err := os.Open(fileDir + "/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var dat map[string]interface{}
			if err := json.Unmarshal(scanner.Bytes(), &dat); err != nil {
				panic(err)
			}
			id := dat["videoID"].(string)
			time := int(dat["_time"].(float64))
			count, _ := strconv.Atoi(dat["viewCount"].(string))
			if video, exist := videoMap[id]; !exist {
				videoMap[id] = Video{id, []int{time}, []int{count}}
			} else {
				videoMap[id] = video.Append(time, count)
			}
		}
	}

	videos := make([]Video, 0, len(videoMap))
	for _, v := range videoMap {
		sort.Stable(sort.IntSlice(v.ParseTime))
		sort.Stable(sort.IntSlice(v.ViewCount))
		videos = append(videos, v)
	}

	return videos
}

func readUserDist(fileDir string) []int {
	file, _ := os.Open(fileDir + "/users")
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = 3

	userCount := make(map[string]int)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		userID := record[1]

		userCount[userID]++
	}

	count := make([]int, len(userCount))
	i := 0
	for _, value := range userCount {
		count[i] = value
		i++
	}
	sort.Slice(count, func(i, j int) bool {
		return count[i] < count[j]
	})

	return count
}
