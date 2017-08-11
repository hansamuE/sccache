package generator

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/leesper/go_rng"
)

func GenerateRequests(fileDir string, userNum int, proportion float64) {
	seed := rand.NewSource(time.Now().UnixNano())
	ran := rand.New(seed)
	prng := rng.NewPoissonGenerator(time.Now().UnixNano())

	videos := readParsedVideos(fileDir)
	videoNum := len(videos)
	timeMap := make(map[int][]int)
	for id, v := range videos {
		for i := 0; i < len(v.ParseTime)-1; i++ {
			if v.ViewCount[i+1]-v.ViewCount[i] == 0 {
				continue
			}
			lambda := float64(v.ViewCount[i+1]-v.ViewCount[i]) * proportion / (float64(v.ParseTime[i+1]-v.ParseTime[i]) / 600)
			for t := v.ParseTime[i]; t < v.ParseTime[i+1]; t += 600 {
				for j := 0; j < int(prng.Poisson(lambda)); j++ {
					rt := t + ran.Intn(600)
					if req, exist := timeMap[rt]; !exist {
						vid := make([]int, 0)
						timeMap[rt] = append(vid, id)
					} else {
						timeMap[rt] = append(req, id)
					}
				}
			}
			fmt.Println(v.ParseTime[i+1])
		}
		fmt.Println(id)
	}

	userDist := readUserDist(fileDir)
	if userNum > len(userDist) {
		userNum = len(userDist)
	}
	userDist = userDist[:userNum]
	userDistTotal := 0
	for _, value := range userDist {
		userDistTotal += value
	}

	err := os.MkdirAll(fileDir+"output", os.ModePerm)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(fileDir + "output/requests_" + strconv.Itoa(videoNum) + "_" + strconv.Itoa(userNum) + "_" + strconv.FormatFloat(proportion, 'f', 7, 64) + ".csv")
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
				r := ran.Float64() * float64(userDistTotal)
				for u, count := range userDist {
					r -= float64(count)
					if r < 0 && t-lastReq[u] >= 0 {
						lastReq[u] = t
						f.WriteString(strconv.Itoa(t) + "\t" + strconv.Itoa(u) + "\t" + strconv.Itoa(v) + "\n")
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
