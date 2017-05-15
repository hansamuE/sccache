package sccache

import (
	"time"
	"encoding/csv"
	"io"
	"strconv"
)

type file struct {
	id string
	popPrd []int
	popAcm []int
}

type client struct {
	id        string
	smallCell *smallCell
	popPrd []map[*file]int
	popAcm []map[*file]int
}

type request struct {
	time   time.Time
	file   *file
	client *client
}

type period struct {
	end      time.Time
	requests []request
	pop map[*file]int
}

type smallCell struct {
	clients map[string]*client
	popAcm []map[*file]int
}

func (c *client) assignTo(sc *smallCell) {
	if c.smallCell != nil {
		delete(c.smallCell.clients, c.id)
	}
	sc.clients[c.id] = c
	c.smallCell = sc
}

func ReadRequests(reader io.Reader, duration time.Duration) ([]period, map[string]*file, map[string]*client) {
	var pend time.Time
	var p int
	var f *file
	var c *client
	var ok bool
	periods := make([]period, 0)
	files := make(map[string]*file)
	clients := make(map[string]*client)
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
			clients[rec[2]] = &client{id: rec[2], popPrd: []map[*file]int{make(map[*file]int)}, popAcm: []map[*file]int{make(map[*file]int)}}
			c = clients[rec[2]]
		}
		ti, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			panic(err)
		}
		t := time.Unix(ti, 0)
		if pend.IsZero() {
			pend = t.Round(duration)
			periods = append(periods, period{end: pend, requests: make([]request, 0), pop: make(map[*file]int)})
			p = 0
		} else {
			for t.After(pend) {
				pend = pend.Add(duration)
				periods = append(periods, period{end: pend, requests: make([]request, 0), pop: make(map[*file]int)})
				p = len(periods) - 1
				f.popPrd = append(f.popPrd, 0)
				f.popAcm = append(f.popAcm, f.popAcm[p - 1])
				c.popPrd = append(c.popPrd, make(map[*file]int))
				c.popAcm = append(c.popAcm, make(map[*file]int))
				for k, v := range c.popAcm[p - 1] {
					c.popAcm[p][k] = v
				}
			}
		}
		periods[p].requests = append(periods[p].requests, request{t, f, c})

		f.popPrd[p]++
		f.popAcm[p]++
		c.popPrd[p][f]++
		c.popAcm[p][f]++
		periods[p].pop[f]++
	}

	return periods, files, clients
}

func ReadClientsAssignment(reader io.Reader, clients map[string]*client) []*smallCell {
	smallCells := make([]*smallCell, 0)
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

		smallCells = append(smallCells, &smallCell{clients: make(map[string]*client), popAcm: []map[*file]int{make(map[*file]int)}})
		sc := len(smallCells) - 1
		for _, cid := range rec{
			clients[cid].assignTo(smallCells[sc])
			for p, m := range clients[cid].popAcm {
				for k, v := range m {
					smallCells[sc].popAcm[p][k] += v
				}
				smallCells[sc].popAcm = append(smallCells[sc].popAcm, make(map[*file]int))
			}
		}
	}

	return smallCells
}