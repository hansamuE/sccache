package sccache

import (
	"time"
	"encoding/csv"
	"io"
	"strconv"
)

type file struct {
	id string
}

type client struct {
	id        string
	smallCell *smallCell
}

type request struct {
	time   time.Time
	file   *file
	client *client
}

type period struct {
	end      time.Time
	requests []request
}

type smallCell struct {
	clients map[string]*client
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

		ti, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			panic(err)
		}
		t := time.Unix(ti, 0)
		if pend.IsZero() {
			pend = t.Round(duration)
			periods = append(periods, period{end: pend, requests: make([]request, 0)})
		} else {
			for t.After(pend) {
				pend = pend.Add(duration)
				periods = append(periods, period{end: pend, requests: make([]request, 0)})
			}
		}
		if _, ok := files[rec[1]]; !ok {
			files[rec[1]] = &file{id: rec[1]}
		}
		f := files[rec[1]]
		if _, ok := clients[rec[2]]; !ok {
			clients[rec[2]] = &client{id: rec[2]}
		}
		c := clients[rec[2]]
		periods[len(periods) - 1].requests = append(periods[len(periods) - 1].requests, request{t, f, c})
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

		smallCells = append(smallCells, &smallCell{clients: make(map[string]*client)})
		for _, cid := range rec{
			clients[cid].assignTo(smallCells[len(smallCells) - 1])
		}
	}

	return smallCells
}