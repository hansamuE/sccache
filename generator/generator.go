package generator

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func GenerateRequests(fileDir string, userNum int) {
	seed := rand.NewSource(time.Now().UnixNano())
	ran := rand.New(seed)

	videos := readParsedVideos(fileDir)
	videoNum := len(videos)
	timeMap := make(map[int][]int)
	for id, v := range videos {
		for i := 0; i < len(v.ParseTime)-1; i++ {
			for j := 0; j < (v.ViewCount[i+1]-v.ViewCount[i])/1000; j++ {
				t := v.ParseTime[i] + ran.Intn(v.ParseTime[i+1]-v.ParseTime[i])
				if req, exist := timeMap[t]; !exist {
					vid := make([]int, 0)
					timeMap[t] = append(vid, id)
				} else {
					timeMap[t] = append(req, id)
				}
			}
		}
	}

	totals := make([]int, videoNum)
	userPref := make([][]int, userNum)
	for i := range userPref {
		userPref[i] = make([]int, videoNum)
	}
	zipf := rand.NewZipf(ran, 3, 5, uint64(videoNum))
	userDist := make(map[int]int)
	for i := 0; i < 1000000; i++ {
		userDist[int(zipf.Uint64())]++
	}
	u := 0
	for i := range userDist {
		for ; u < userNum*userDist[i]/1000000; u++ {
			for j, v := range ran.Perm(videoNum) {
				if j < i+1 {
					userPref[u][v] = 60 + rand.Intn(40)
				} else {
					userPref[u][v] = rand.Intn(10)
				}
				totals[v] += userPref[u][v]
			}
		}
	}

	f, err := os.Create(fileDir + "requests_" + strconv.Itoa(videoNum) + "_" + strconv.Itoa(userNum) + ".csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	lastReq := make([]int, userNum)
	start := 9999999999
	end := 0
	for _, v := range videos {
		if v.ParseTime[0] < start {
			start = v.ParseTime[0]
		}
		if v.ParseTime[len(v.ParseTime)-1] > end {
			end = v.ParseTime[len(v.ParseTime)-1]
		}
	}
	for t := start; t < end; t++ {
		if req, exist := timeMap[t]; exist {
			order := ran.Perm(len(req))
			for _, i := range order {
				v := req[i]
				r := ran.Float64() * float64(totals[v])
				for u, pref := range userPref {
					r -= float64(pref[v])
					if r < 0 && t-lastReq[u] >= 60 {
						lastReq[u] = t
						f.WriteString(strconv.Itoa(t) + "\t" + strconv.Itoa(v) + "\t" + strconv.Itoa(u) + "\n")
						break
					}
				}
			}
		}
		if t%3600 == 0 {
			fmt.Println(t)
		}
	}
}
