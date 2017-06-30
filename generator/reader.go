package generator

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

type Video struct {
	ID        string
	Title     string
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
			title := dat["videoName"].(string)
			time := int(dat["_time"].(float64))
			count, _ := strconv.Atoi(dat["viewCount"].(string))
			if video, exist := videoMap[id]; !exist {
				videoMap[id] = Video{id, title, []int{time}, []int{count}}
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
