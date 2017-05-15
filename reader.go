package sccache

import (
	"time"
	"encoding/csv"
	"io"
	"strconv"
)

type File struct {
	Name string
}

type Client struct {
	Name string
}

type Request struct {
	Time time.Time
	File *File
	Client *Client
}

type Period struct {
	End time.Time
	Requests []Request
}

type SmallCell struct {
	Clients map[string]*Client
}

func ReadRequests(reader io.Reader, duration time.Duration) ([]Period, map[string]*File, map[string]*Client) {
	var pend time.Time
	periods := make([]Period, 0)
	files := make(map[string]*File)
	clients := make(map[string]*Client)
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
			periods = append(periods, Period{End: pend, Requests: make([]Request, 0)})
		} else {
			for t.After(pend) {
				pend = pend.Add(duration)
				periods = append(periods, Period{End: pend, Requests: make([]Request, 0)})
			}
		}
		if _, ok := files[rec[1]]; !ok {
			files[rec[1]] = &File{Name: rec[1]}
		}
		f := files[rec[1]]
		if _, ok := clients[rec[2]]; !ok {
			clients[rec[2]] = &Client{Name: rec[2]}
		}
		c := clients[rec[2]]
		periods[len(periods) - 1].Requests = append(periods[len(periods) - 1].Requests, Request{t, f, c})
	}

	return periods, files, clients
}

func ReadClientsAssignment(reader io.Reader, clients map[string]*Client) []SmallCell {
	smallCells := make([]SmallCell, 0)
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

		smallCells = append(smallCells, SmallCell{Clients: make(map[string]*Client)})
		for _, cid := range rec{
			smallCells[len(smallCells) - 1].Clients[cid] = clients[cid]
		}
	}

	return smallCells
}