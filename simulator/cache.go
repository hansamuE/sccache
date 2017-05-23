package simulator

import (
	"sort"
	"time"
)

type cachePolicy func([]*cache) []*cache

type cacheStorageList []*cacheStorage

type cacheStorage struct {
	smallCells              smallCellList
	popularitiesAccumulated []popularities
	popularFiles            []fileList
	caches                  []*cache
	size                    int
	space                   int
	stats
}

type cache struct {
	file    *file
	size    int
	fixed   bool
	count   int
	lastReq time.Time
}

func leastFrequentlyUsed(cl []*cache) []*cache {
	sort.Slice(cl, func(i, j int) bool { return cl[i].count < cl[j].count })
	return cl
}

func leastRecentlyUsed(cl []*cache) []*cache {
	sort.Slice(cl, func(i, j int) bool { return cl[i].lastReq.Before(cl[j].lastReq) })
	return cl
}

func (cs *cacheStorage) cacheFile(f *file, cp cachePolicy) (int, *cache) {
	sizeNotCached := f.size
	ok, cf := cs.hasFile(f)
	if ok {
		sizeNotCached -= cf.size
	} else {
		cf = &cache{file: f}
	}
	sizeCached := cf.size
	if !ok || cf.size != f.size {
		if cs.space >= sizeNotCached {
			cs.space -= sizeNotCached
			sizeNotCached = 0
		} else {
			sizeNotCached -= cs.space
			cs.space = 0
			di := make([]int, 0)
			for i, v := range cs.caches {
				if v == cf || v.fixed {
					continue
				}
				sizeNotCached -= v.size
				if sizeNotCached <= 0 {
					if sizeNotCached == 0 {
						di = append(di, i)
					} else {
						v.size = -sizeNotCached
						sizeNotCached = 0
					}
					break
				}
				di = append(di, i)
			}
			cs.deleteCache(di)
		}
		cf.size = f.size - sizeNotCached
		if cf.size != 0 {
			cs.caches = append(cs.caches, cf)
		}
	}
	return sizeCached, cf
}

func (cs *cacheStorage) deleteCache(di []int) {
	for i, v := range di {
		cs.caches = append(cs.caches[:v-i], cs.caches[v-i+1:]...)
	}
}

func (cs *cacheStorage) hasFile(f *file) (bool, *cache) {
	for _, c := range cs.caches {
		if c.file == f {
			return true, c
		}
	}
	return false, nil
}

func (csl cacheStorageList) smallCellsHasFile(f *file) smallCellList {
	scl := make(smallCellList, 0)
	for _, cs := range csl {
		if ok, _ := cs.hasFile(f); ok {
			scl = append(scl, cs.smallCells...)
			break
		}
	}
	return scl
}
