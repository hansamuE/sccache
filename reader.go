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

func ReadRequests(reader io.Reader, duration time.Duration) ([]Period, map[string]File, map[string]Client) {
	var pend time.Time
	p := 0
	periods := make([]Period, 1)
	periods[p].Requests = make([]Request, 0)
	files := make(map[string]File)
	clients := make(map[string]Client)
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
			periods[p].End = pend
		} else {
			for t.After(pend) {
				p++
				pend = pend.Add(duration)
				periods = append(periods, Period{End: pend, Requests: make([]Request, 0)})
			}
		}
		if _, ok := files[rec[1]]; !ok {
			files[rec[1]] = File{Name: rec[1]}
		}
		f := files[rec[1]]
		if _, ok := clients[rec[2]]; !ok {
			clients[rec[2]] = Client{Name: rec[2]}
		}
		c := clients[rec[2]]
		periods[p].Requests = append(periods[p].Requests, Request{t, &f, &c})
	}

	return periods, files, clients
}