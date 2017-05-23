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
			for j := 0; j < v.ViewCount[i+1]-v.ViewCount[i]; j++ {
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
	userDist := []float64{0.0, 0.62, 1.24, 13.66, 18.01, 24.84, 37.89, 51.55, 70.19, 100.0}
	u := 0
	for i := range userDist {
		for ; u < int(float64(userNum)*userDist[i]/100); u++ {
			for j, v := range ran.Perm(videoNum) {
				if j < videoNum-i {
					userPref[u][v] = 9
				} else {
					userPref[u][v] = 1
				}
				totals[v] += userPref[u][v]
			}
		}
	}

	f, _ := os.Create(fileDir + "outputData/output_" + strconv.Itoa(videoNum) + "_" + strconv.Itoa(userNum) + ".csv")
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
					if r < 0 && t-lastReq[u] > 180 {
						lastReq[u] = t
						fmt.Println(t, v, u)
						f.WriteString(strconv.Itoa(t) + "\t" + strconv.Itoa(v) + "\t" + strconv.Itoa(u) + "\n")
						break
					}
				}
			}
		}
	}
}
