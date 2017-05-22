package simulator

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"time"
)

var (
	periods    periodList
	files      map[string]*file
	clients    map[string]*client
	smallCells smallCellList
	Configs    []Config
)

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
	popularities            popularities
	popularFiles            filePopularityList
	popularFilesAccumulated filePopularityList
	newClients              clientList
	stats
}

type smallCell struct {
	clients                 map[string]*client
	popularitiesAccumulated []popularities
	cacheStorage            *cacheStorage
}

func readConfigs(reader io.Reader) {
	dec := json.NewDecoder(reader)
	ConfigJSONs := make(ConfigJSONList, 0)
	for dec.More() {
		err := dec.Decode(&ConfigJSONs)
		if err != nil {
			panic(err)
		}
	}
	Configs = ConfigJSONs.toConfig()
}

func readRequests(reader io.Reader, duration time.Duration) {
	var pend time.Time
	var p int
	var f *file
	var c *client
	var ok bool
	periods = make(periodList, 0)
	files = make(map[string]*file)
	clients = make(map[string]*client)
	r := csv.NewReader(reader)
	r.Comma = '\t'
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if f, ok = files[rec[1]]; !ok {
			files[rec[1]] = &file{id: rec[1], popularityPeriod: make([]int, 1), popularityAccumulated: make([]int, 1)}
			f = files[rec[1]]
		}
		if c, ok = clients[rec[2]]; !ok {
			clients[rec[2]] = &client{id: rec[2], popularityPeriod: []popularities{make(popularities)}, popularityAccumulated: []popularities{make(popularities)}}
			c = clients[rec[2]]
		}
		ti, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			panic(err)
		}
		t := time.Unix(ti, 0)
		if pend.IsZero() {
			p = 0
			pend = t.Round(duration)
			periods = append(periods, &period{id: p, end: pend, requests: make([]request, 0), popularities: make(popularities), newClients: make(clientList, 0)})
		} else {
			for t.After(pend) {
				p = len(periods)
				pend = pend.Add(duration)
				periods = append(periods, &period{id: p, end: pend, requests: make([]request, 0), popularities: make(popularities), newClients: make(clientList, 0)})
			}
		}
		periods[p].requests = append(periods[p].requests, request{t, f, c})

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

		smallCells = append(smallCells, &smallCell{clients: make(map[string]*client), popularitiesAccumulated: []popularities{make(popularities)}})
		for _, cid := range rec {
			clients[cid].assignTo(smallCells[len(smallCells)-1])
		}
	}
}

func (c *client) assignTo(sc *smallCell) {
	osc := c.smallCell
	if osc != nil {
		delete(c.smallCell.clients, c.id)
	}
	sc.clients[c.id] = c
	c.smallCell = sc

	for p, fp := range c.popularityAccumulated {
		if len(sc.popularitiesAccumulated)-1 < p {
			sc.popularitiesAccumulated = append(sc.popularitiesAccumulated, make(popularities))
		}
		for k, v := range fp {
			if osc != nil {
				osc.popularitiesAccumulated[p][k] -= v
				if osc.cacheStorage != nil {
					osc.cacheStorage.popAcm[p][k] -= v
				}
			}
			sc.popularitiesAccumulated[p][k] += v
			if sc.cacheStorage != nil {
				sc.cacheStorage.popAcm[p][k] += v
			}
		}
	}
}
