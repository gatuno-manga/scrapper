package scraper

import (
	"container/list"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/playwright-community/playwright-go"
)

type CachedImage struct {
	URL         string
	Data        []byte
	TempPath    string
	ContentType string
	LastAccess  time.Time
}

type NetworkInterceptor struct {
	mu          sync.Mutex
	cache       map[string]*list.Element
	lru         *list.List
	maxSize     int64
	currentSize int64
	largeThres  int64
	
	tempFiles   []string
	intercepting bool
	blacklist   []string
	whitelist   []string
}

func NewNetworkInterceptor(maxSizeBytes int64) *NetworkInterceptor {
	return &NetworkInterceptor{
		cache:      make(map[string]*list.Element),
		lru:        list.New(),
		maxSize:    maxSizeBytes,
		largeThres: 5 * 1024 * 1024, // 5MB threshold for temp file offloading
	}
}

func (ni *NetworkInterceptor) SetFilters(blacklist, whitelist []string) {
	ni.mu.Lock()
	defer ni.mu.Unlock()
	ni.blacklist = blacklist
	ni.whitelist = whitelist
}

func (ni *NetworkInterceptor) Start(page playwright.Page) {
	ni.mu.Lock()
	ni.intercepting = true
	blacklist := ni.blacklist
	whitelist := ni.whitelist
	ni.mu.Unlock()

	// Handle blocking via Route
	page.Route("**/*", func(route playwright.Route) {
		req := route.Request()
		url := req.URL()
		
		// Whitelist check (if not empty, MUST match)
		if len(whitelist) > 0 {
			allowed := false
			for _, term := range whitelist {
				if strings.Contains(url, term) {
					allowed = true
					break
				}
			}
			if !allowed {
				route.Abort("blockedbyclient")
				return
			}
		}

		// Blacklist check
		for _, term := range blacklist {
			if strings.Contains(url, term) {
				route.Abort("blockedbyclient")
				return
			}
		}

		route.Continue()
	})

	page.OnResponse(func(res playwright.Response) {
		ni.mu.Lock()
		if !ni.intercepting {
			ni.mu.Unlock()
			return
		}
		ni.mu.Unlock()

		req := res.Request()
		if req.ResourceType() != "image" {
			return
		}

		if !res.Ok() {
			return
		}

		body, err := res.Body()
		if err != nil || len(body) == 0 {
			return
		}

		ni.Put(res.URL(), body, res.Headers()["content-type"])
	})
}

func (ni *NetworkInterceptor) Stop() {
	ni.mu.Lock()
	ni.intercepting = false
	ni.mu.Unlock()
}

func (ni *NetworkInterceptor) Put(url string, data []byte, contentType string) {
	ni.mu.Lock()
	defer ni.mu.Unlock()

	size := int64(len(data))
	
	// Check if already in cache
	if ent, ok := ni.cache[url]; ok {
		ni.lru.MoveToFront(ent)
		img := ent.Value.(*CachedImage)
		img.LastAccess = time.Now()
		return
	}

	// Offload to temp file if too large
	var tempPath string
	if size > ni.largeThres {
		f, err := os.CreateTemp("", "scraper-cache-*.img")
		if err == nil {
			f.Write(data)
			f.Close()
			tempPath = f.Name()
			ni.tempFiles = append(ni.tempFiles, tempPath)
			data = nil // Free memory
			log.Printf("Offloaded large image to temp file: %s", tempPath)
		}
	}

	// Evict LRU if needed
	for ni.currentSize+size > ni.maxSize && ni.lru.Len() > 0 {
		ni.evict()
	}

	img := &CachedImage{
		URL:         url,
		Data:        data,
		TempPath:    tempPath,
		ContentType: contentType,
		LastAccess:  time.Now(),
	}

	ent := ni.lru.PushFront(img)
	ni.cache[url] = ent
	ni.currentSize += size
}

func (ni *NetworkInterceptor) Get(url string) ([]byte, bool) {
	ni.mu.Lock()
	defer ni.mu.Unlock()

	// Try exact match
	if ent, ok := ni.cache[url]; ok {
		log.Printf("Cache hit (exact): %s", url)
		return ni.extractData(ent)
	}

	// Try match without query params
	urlWithoutQuery := strings.Split(url, "?")[0]
	for cUrl, ent := range ni.cache {
		if strings.Split(cUrl, "?")[0] == urlWithoutQuery {
			log.Printf("Cache hit (no-query): %s", urlWithoutQuery)
			return ni.extractData(ent)
		}
	}

	log.Printf("Cache miss: %s", url)
	return nil, false
}

func (ni *NetworkInterceptor) extractData(ent *list.Element) ([]byte, bool) {
	ni.lru.MoveToFront(ent)
	img := ent.Value.(*CachedImage)
	img.LastAccess = time.Now()

	if img.Data != nil {
		return img.Data, true
	}

	if img.TempPath != "" {
		data, err := os.ReadFile(img.TempPath)
		if err == nil {
			return data, true
		}
	}

	return nil, false
}

func (ni *NetworkInterceptor) evict() {
	ent := ni.lru.Back()
	if ent == nil {
		return
	}

	img := ent.Value.(*CachedImage)
	size := int64(len(img.Data))
	if img.TempPath != "" {
		os.Remove(img.TempPath)
		// We should also remove it from ni.tempFiles but that's slow to find.
		// We'll clean all up in Clear().
	}

	ni.lru.Remove(ent)
	delete(ni.cache, img.URL)
	ni.currentSize -= size
}

func (ni *NetworkInterceptor) Clear() {
	ni.mu.Lock()
	defer ni.mu.Unlock()

	for _, f := range ni.tempFiles {
		os.Remove(f)
	}
	ni.tempFiles = nil
	ni.cache = make(map[string]*list.Element)
	ni.lru.Init()
	ni.currentSize = 0
}
