package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gatuno/scraper/internal/models"
)

func TestScraper_ExecuteTestScript(t *testing.T) {
	// 1. Setup Mock Server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><head><title>Test Page</title></head><body><h1>Hello</h1></body></html>")
	}))
	defer ts.Close()

	// 2. Setup Scraper
	pool, err := NewBrowserPool("", 1)
	if err != nil {
		t.Skip("Skipping Scraper test: could not init pool")
		return
	}
	defer pool.Close()
	
	engine := NewScraper(pool, 1024*1024)

	// 3. Run Test
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := engine.ExecuteTestScript(ctx, models.ScrapingTestRequest{
		TargetURL: ts.URL,
		Script:    "document.title",
	})

	if err != nil {
		t.Fatalf("Script execution failed: %v", err)
	}

	if res != "Test Page" {
		t.Errorf("Expected 'Test Page', got %v", res)
	}
}

func TestScraper_ScrapeBook(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body>
			<div id="title">My Awesome Book</div>
			<div class="chapter">Chapter 1</div>
			<div class="chapter">Chapter 2</div>
		</body></html>`)
	}))
	defer ts.Close()

	pool, _ := NewBrowserPool("", 1)
	if pool == nil {
		t.Skip()
		return
	}
	defer pool.Close()
	
	engine := NewScraper(pool, 1024*1024)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req := models.ScrapingUpdateBookRequest{
		JobID:   "job-1",
		BookID:  "book-1",
		TargetURL: ts.URL,
		BookInfoExtractScript: `(() => {
			const title = document.getElementById('title').innerText;
			const chapters = Array.from(document.querySelectorAll('.chapter')).map(el => el.innerText);
			return { title: title, chapters: chapters.map((c, i) => ({ title: c, url: 'http://fake.com', index: i })) };
		})()`,
	}

	result, err := engine.ScrapeUpdateBook(ctx, req)
	if err != nil {
		t.Fatalf("Book scrape failed: %v", err)
	}

	if result.Title != "My Awesome Book" {
		t.Errorf("Expected title 'My Awesome Book', got %s", result.Title)
	}

	if len(result.Chapters) != 2 {
		t.Errorf("Expected 2 chapters, got %d", len(result.Chapters))
	}
}

func TestScraper_ScrapeUpdateBook_WithComplexConfig(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Cookie
		cookie, err := r.Cookie("session")
		if err != nil || cookie.Value != "secret" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body>
			<div id="status">locked</div>
			<div id="title">Secret Book</div>
			<div class="chapter">Chapter 1</div>
		</body></html>`)
	}))
	defer ts.Close()

	pool, _ := NewBrowserPool("", 1)
	if pool == nil {
		t.Skip()
		return
	}
	defer pool.Close()
	
	engine := NewScraper(pool, 1024*1024)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := models.ScrapingUpdateBookRequest{
		JobID:   "job-secret",
		BookID:  "book-secret",
		TargetURL: ts.URL,
		WebsiteConfig: models.WebsiteConfig{
			Cookies: []models.Cookie{
				{Name: "session", Value: "secret", Domain: "127.0.0.1"},
			},
			PreScript: `document.getElementById('status').innerText = 'unlocked';`,
		},
		// Added a trailing comment to test robustness of wrapping
		BookInfoExtractScript: `(() => {
			const title = document.getElementById('title').innerText;
			const status = document.getElementById('status').innerText;
			const chapters = Array.from(document.querySelectorAll('.chapter')).map(el => el.innerText);
			return { 
				title: title + " (" + status + ")", 
				chapters: chapters.map((c, i) => ({ title: c, url: 'http://fake.com', index: i + 0.5 })) 
			};
		})() // trailing comment`,
	}

	result, err := engine.ScrapeUpdateBook(ctx, req)
	if err != nil {
		t.Fatalf("Book scrape failed: %v", err)
	}

	expectedTitle := "Secret Book (unlocked)"
	if result.Title != expectedTitle {
		t.Errorf("Expected title '%s', got '%s'", expectedTitle, result.Title)
	}

	if len(result.Chapters) != 1 {
		t.Errorf("Expected 1 chapter, got %d", len(result.Chapters))
	}

	// Verify float index
	if result.Chapters[0].Index != 0.5 {
		t.Errorf("Expected index 0.5, got %f", result.Chapters[0].Index)
	}
}
