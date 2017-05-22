package sccache

import (
	"time"
	"encoding/csv"
	"io"
	"strconv"
)

type filePop map[*file]int

type filePopNorm map[*file]float64

type fileList []*file

type popFile struct {
	file *file
	pop int
}

type popFileList []popFile

type clientList []*client

type periodList []*period

type smallCellList []*smallCell

type file struct {
	id string
	size int
	popPrd []int
	popAcm []int
}

type client struct {
	id        string
	smallCell *smallCell
	popPrd []filePop
	popAcm []filePop
}

type request struct {
	time   time.Time
	file   *file
	client *client
}

type period struct {
	end      time.Time
	requests []request
	pop      filePop
	popFiles popFileList
	popFilesAcm popFileList
	newClients clientList
	stats
}

type smallCell struct {
	clients map[string]*client
	popAcm []filePop
	cacheStorage *cacheStorage
}

var (
	periods	periodList
	files	map[string]*file
	clients	map[string]*client
	smallCells	smallCellList
)

func (c *client) assignTo(sc *smallCell) {
	osc := c.smallCell
	if osc != nil {
		delete(c.smallCell.clients, c.id)
	}
	sc.clients[c.id] = c
	c.smallCell = sc

	for p, fp := range c.popAcm {
		if len(sc.popAcm) - 1 < p {
			sc.popAcm = append(sc.popAcm, make(filePop))
		}
		for k, v := range fp {
			if osc != nil {
				osc.popAcm[p][k] -= v
				if osc.cacheStorage != nil {
					osc.cacheStorage.popAcm[p][k] -= v
				}
			}
			sc.popAcm[p][k] += v
			if sc.cacheStorage != nil {
				sc.cacheStorage.popAcm[p][k] += v
			}
		}
	}
}

func readRequests(reader io.Reader, duration time.Duration) (periodList, map[string]*file, map[string]*client) {
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
			files[rec[1]] = &file{id: rec[1], popPrd: make([]int, 1), popAcm: make([]int, 1)}
			f = files[rec[1]]
		}
		if c, ok = clients[rec[2]]; !ok {
			clients[rec[2]] = &client{id: rec[2], popPrd: []filePop{make(filePop)}, popAcm: []filePop{make(filePop)}}
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
			periods = append(periods, &period{end: pend, requests: make([]request, 0), pop: make(filePop), newClients: make(clientList, 0)})
		} else {
			for t.After(pend) {
				p = len(periods)
				pend = pend.Add(duration)
				periods = append(periods, &period{end: pend, requests: make([]request, 0), pop: make(filePop), newClients: make(clientList, 0)})
			}
		}
		periods[p].requests = append(periods[p].requests, request{t, f, c})

		for _, fp := range files {
			for len(fp.popPrd) - 1 < p {
				fp.popPrd = append(fp.popPrd, 0)
				fp.popAcm = append(fp.popAcm, fp.popAcm[len(fp.popAcm) - 1])
			}
		}
		for _, cp := range clients {
			for len(cp.popPrd) - 1 < p {
				cp.popPrd = append(cp.popPrd, make(filePop))
				cp.popAcm = append(cp.popAcm, make(filePop))
				for k, v := range cp.popAcm[len(cp.popAcm) - 2] {
					cp.popAcm[len(cp.popAcm) - 1][k] = v
				}
			}
		}
		f.popPrd[p]++
		f.popAcm[p]++
		c.popPrd[p][f]++
		c.popAcm[p][f]++
		periods[p].pop[f]++
	}
	periods.setPopFiles(files)

	return periods, files, clients
}

func readClientsAssignment(reader io.Reader, clients map[string]*client) smallCellList {
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

		smallCells = append(smallCells, &smallCell{clients: make(map[string]*client), popAcm: []filePop{make(filePop)}})
		for _, cid := range rec{
			clients[cid].assignTo(smallCells[len(smallCells) - 1])
		}
	}

	return smallCells
}