package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gatuno/scraper/internal/models"
	"github.com/gatuno/scraper/internal/ratelimit"
)

func TestScraper_CookieInjectionAdvanced(t *testing.T) {
	// 1. Setup Mock Server to check advanced cookie attributes
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("advanced")
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// In a real browser-to-server request, we only see Name/Value. 
		// But if it reached here, the browser accepted it.
		if cookie.Value == "attributes" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "<html><body><h1>Cookie Accepted</h1></body></html>")
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer ts.Close()

	pool, _ := NewBrowserPool("", 1)
	if pool == nil {
		t.Skip("Browser pool not available")
		return
	}
	defer pool.Close()
	
	limiter := ratelimit.NewMemoryRateLimiter()
	semaphore := ratelimit.NewMemorySemaphore(2)
	engine := NewScraper(pool, 1024*1024, limiter, semaphore)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := models.ScrapingNewBookRequest{
		TargetURL: ts.URL,
		NewBookExtractScript: "(() => ({ title: document.body.innerText, chapters: [] }))()",
	}
	
	wc := models.WebsiteConfig{
		Cookies: []models.Cookie{
			{
				Name:     "advanced",
				Value:    "attributes",
				Domain:   "127.0.0.1",
				Path:     "/",
				HttpOnly: true,
				Secure:   false,
				SameSite: "Lax",
			},
		},
	}

	res, err := engine.ScrapeNewBook(ctx, req, wc)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if res.Title != "Cookie Accepted" {
		t.Errorf("Expected 'Cookie Accepted', got %v", res.Title)
	}
}

func TestScraper_NetworkInterception(t *testing.T) {
	// 1. Setup Mock Server with an image
	imageContent := []byte("fake-image-data")
	ts := httptest.NewServer(nil)
	ts.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/image.jpg" {
			w.Header().Set("Content-Type", "image/jpeg")
			w.Write(imageContent)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, "<html><body><img src='%s/image.jpg' id='test-img'></body></html>", ts.URL)
	})
	defer ts.Close()

	pool, _ := NewBrowserPool("", 1)
	if pool == nil {
		t.Skip("Browser pool not available")
		return
	}
	defer pool.Close()
	
	limiter := ratelimit.NewMemoryRateLimiter()
	semaphore := ratelimit.NewMemorySemaphore(2)
	engine := NewScraper(pool, 1024*1024, limiter, semaphore)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := models.ScrapingChapterRequest{
		TargetURL: ts.URL,
	}
	
	wc := models.WebsiteConfig{
		UseNetworkInterception: true,
		Selector: "#test-img",
	}

	title, urls, results, cleanup, err := engine.ScrapeChapter(ctx, req, wc)
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}
	defer cleanup()

	if len(urls) != 1 {
		t.Errorf("Expected 1 image URL, got %d", len(urls))
	}

	// Drain results
	count := 0
	for res := range results {
		if res.Error != nil {
			t.Errorf("Image result error: %v", res.Error)
		}
		if string(res.Data) != string(imageContent) {
			t.Errorf("Image data mismatch. Expected %s, got %s", string(imageContent), string(res.Data))
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 image result, got %d", count)
	}
	
	_ = title
}
