package simulator

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/cdipaolo/goml/cluster"
)

var (
	periods     periodList
	files       fileMap
	filesList   fileList
	clients     clientMap
	smallCells  smallCellList
	configs     configList
	configJSONs configJSONList
)

type fileMap map[string]*file
type clientMap map[string]*client
type clientList []*client
type periodList []*period
type smallCellList []*smallCell

type file struct {
	id                    string
	size                  int
	popularityPeriod      []int
	popularityAccumulated []int
}

type client struct {
	id                    string
	smallCell             *smallCell
	popularityPeriod      []popularities
	popularityAccumulated []popularities
}

type request struct {
	time   time.Time
	file   *file
	client *client
}

type period struct {
	id                      int
	end                     time.Time
	requests                []request
	clients                 clientMap
	popularities            popularities
	popularFiles            fileList
	popularFilesAccumulated fileList
	newClients              clientList
	stats
}

type smallCell struct {
	id                      int
	clients                 map[string]*client
	popularitiesPeriod      []popularities
	popularitiesAccumulated []popularities
	cacheStorage            *cacheStorage
	periodStats             []stats
}

func readConfigs(reader io.Reader) {
	dec := json.NewDecoder(reader)
	for dec.More() {
		err := dec.Decode(&configJSONs)
		if err != nil {
			panic(err)
		}
	}
	configs = configJSONs.toConfig()
}

func readRequests(reader io.Reader, duration time.Duration, column []int, comma rune) {
	var colTime, colFile, colClient = 0, 1, 2
	if len(column) != 0 {
		colTime, colFile, colClient = column[0], column[1], column[2]
	}
	var pend time.Time
	var p int
	var f *file
	var c *client
	var ok bool
	periods = make(periodList, 0)
	files = make(fileMap)
	clients = make(clientMap)
	r := csv.NewReader(reader)
	r.Comma = comma
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if f, ok = files[rec[colFile]]; !ok {
			files[rec[colFile]] = &file{
				id:                    rec[colFile],
				popularityPeriod:      make([]int, 1),
				popularityAccumulated: make([]int, 1),
			}
			f = files[rec[colFile]]
		}
		if c, ok = clients[rec[colClient]]; !ok {
			clients[rec[colClient]] = &client{
				id:                    rec[colClient],
				popularityPeriod:      []popularities{make(popularities)},
				popularityAccumulated: []popularities{make(popularities)},
			}
			c = clients[rec[colClient]]
		}
		tf, err := strconv.ParseFloat(rec[colTime], 64)
		ti := int64(tf)
		if err != nil {
			panic(err)
		}
		t := time.Unix(ti, 0)
		if pend.IsZero() {
			p = 0
			pend = t.Round(duration)
			periods = append(periods, &period{
				id:           p,
				end:          pend,
				requests:     make([]request, 0),
				clients:      make(clientMap),
				popularities: make(popularities),
				newClients:   make(clientList, 0),
			})
		} else {
			for t.After(pend) {
				p = len(periods)
				pend = pend.Add(duration)
				periods = append(periods, &period{
					id:           p,
					end:          pend,
					requests:     make([]request, 0),
					clients:      make(clientMap),
					popularities: make(popularities),
					newClients:   make(clientList, 0),
				})
			}
		}
		periods[p].requests = append(periods[p].requests, request{t, f, c})
		periods[p].clients[c.id] = c

		for _, fp := range files {
			for len(fp.popularityPeriod)-1 < p {
				fp.popularityPeriod = append(fp.popularityPeriod, 0)
				fp.popularityAccumulated = append(fp.popularityAccumulated, fp.popularityAccumulated[len(fp.popularityAccumulated)-1])
			}
		}
		for _, cp := range clients {
			for len(cp.popularityPeriod)-1 < p {
				cp.popularityPeriod = append(cp.popularityPeriod, make(popularities))
				cp.popularityAccumulated = append(cp.popularityAccumulated, make(popularities))
				for k, v := range cp.popularityAccumulated[len(cp.popularityAccumulated)-2] {
					cp.popularityAccumulated[len(cp.popularityAccumulated)-1][k] = v
				}
			}
		}
		f.popularityPeriod[p]++
		f.popularityAccumulated[p]++
		c.popularityPeriod[p][f]++
		c.popularityAccumulated[p][f]++
		periods[p].popularities[f]++
	}
	periods.setPopularFiles(files)
	filesList = files.getFileList()
}

func readClientsAssignment(reader io.Reader) {
	smallCells = make(smallCellList, 0)
	r := csv.NewReader(reader)
	r.Comma = '\t'
	r.FieldsPerRecord = -1
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		smallCells = append(smallCells, &smallCell{id: len(smallCells),
			clients:                 make(map[string]*client),
			popularitiesPeriod:      []popularities{make(popularities)},
			popularitiesAccumulated: []popularities{make(popularities)},
		})
		for _, cid := range rec {
			clients[cid].assignTo(smallCells[len(smallCells)-1])
		}
	}
}

func readClusteringResult(model string, result io.Reader) {
	clusteringModel = cluster.NewKMeans(0, maxIterations, nil)
	clusteringModel.RestoreFromFile(model)
	smallCells = newSmallCells(len(clusteringModel.Centroids))
	r := csv.NewReader(result)
	r.Comma = '\t'
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		scNo, err := strconv.Atoi(rec[1])
		if err != nil {
			panic(err)
		}
		clients[rec[0]].assignTo(smallCells[scNo])
	}
}

func readCooperationResult(result io.Reader) [][]int {
	group := make([][]int, 0)
	r := csv.NewReader(result)
	r.Comma = '\t'
	r.FieldsPerRecord = -1
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		group = append(group, make([]int, 0))
		for _, value := range rec {
			scID, err := strconv.Atoi(value)
			if err != nil {
				panic(err)
			}
			group[len(group)-1] = append(group[len(group)-1], scID)
		}
	}
	return group
}

func newSmallCells(n int) smallCellList {
	scs := make(smallCellList, n)
	for i := 0; i < n; i++ {
		scs[i] = &smallCell{
			id:                      i,
			clients:                 make(clientMap),
			popularitiesPeriod:      []popularities{make(popularities)},
			popularitiesAccumulated: []popularities{make(popularities)},
			periodStats:             make([]stats, len(periods)),
		}
	}
	return scs
}

func (c *client) assignTo(sc *smallCell) {
	if c.smallCell != nil {
		c.removeFrom(c.smallCell)
	}
	sc.clients[c.id] = c
	c.smallCell = sc

	for p, fp := range c.popularityAccumulated {
		if len(sc.popularitiesAccumulated)-1 < p {
			sc.popularitiesAccumulated = append(sc.popularitiesAccumulated, make(popularities))
			sc.popularitiesPeriod = append(sc.popularitiesPeriod, make(popularities))
		}
		for k, v := range fp {
			sc.popularitiesAccumulated[p][k] += v
			if pv, ok := c.popularityPeriod[p][k]; ok {
				sc.popularitiesPeriod[p][k] += pv
			}
			if sc.cacheStorage != nil {
				sc.cacheStorage.popularitiesAccumulated[p][k] += v
			}
		}
	}
}

func (c *client) removeFrom(sc *smallCell) {
	c.smallCell = nil
	delete(sc.clients, c.id)
	for p, fp := range c.popularityAccumulated {
		for k, v := range fp {
			sc.popularitiesAccumulated[p][k] -= v
			if pv, ok := c.popularityPeriod[p][k]; ok {
				sc.popularitiesPeriod[p][k] -= pv
			}
			if sc.cacheStorage != nil {
				sc.cacheStorage.popularitiesAccumulated[p][k] -= v
			}
		}
	}
}
