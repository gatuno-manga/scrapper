package scraper

import (
	"bytes"
	"os"
	"testing"
)

func TestNetworkInterceptor_PutGet(t *testing.T) {
	ni := NewNetworkInterceptor(1024 * 1024)
	url := "https://example.com/image.jpg"
	data := []byte("fake-image-data")
	contentType := "image/jpeg"

	ni.Put(url, data, contentType)
	
	cached, ok := ni.Get(url)
	if !ok {
		t.Fatal("Expected image to be in cache")
	}
	
	if !bytes.Equal(cached, data) {
		t.Errorf("Cached data mismatch. Expected %s, got %s", data, cached)
	}
}

func TestNetworkInterceptor_Eviction(t *testing.T) {
	// Small cache for 2 small images
	ni := NewNetworkInterceptor(20)
	
	ni.Put("url1", []byte("1234567890"), "image/jpeg") // 10 bytes
	ni.Put("url2", []byte("1234567890"), "image/jpeg") // 10 bytes
	
	// Cache should be full now
	ni.Put("url3", []byte("1234567890"), "image/jpeg") // Should evict url1
	
	if _, ok := ni.Get("url1"); ok {
		t.Error("url1 should have been evicted")
	}
	
	if _, ok := ni.Get("url2"); !ok {
		t.Error("url2 should still be in cache")
	}
	
	if _, ok := ni.Get("url3"); !ok {
		t.Error("url3 should be in cache")
	}
}

func TestNetworkInterceptor_LargeThres(t *testing.T) {
	ni := NewNetworkInterceptor(1024 * 1024)
	ni.largeThres = 10 // Very small threshold for testing
	
	data := []byte("this is a large image data") // > 10 bytes
	url := "large-url"
	
	ni.Put(url, data, "image/jpeg")
	
	// Find the cached image to verify it's offloaded
	ni.mu.Lock()
	ent := ni.cache[url]
	img := ent.Value.(*CachedImage)
	ni.mu.Unlock()
	
	if img.Data != nil {
		t.Error("Large image data should have been cleared from memory")
	}
	
	if img.TempPath == "" {
		t.Error("Large image should have a temporary path")
	}
	
	// Verify we can still get the data
	cached, ok := ni.Get(url)
	if !ok || !bytes.Equal(cached, data) {
		t.Error("Failed to retrieve offloaded data")
	}
	
	// Clean up
	ni.Clear()
	if _, err := os.Stat(img.TempPath); !os.IsNotExist(err) {
		t.Error("Temporary file was not deleted after Clear()")
	}
}

func TestNetworkInterceptor_GetWithoutQuery(t *testing.T) {
	ni := NewNetworkInterceptor(1024 * 1024)
	ni.Put("https://site.com/img.jpg?token=abc", []byte("data"), "image/jpeg")
	
	if _, ok := ni.Get("https://site.com/img.jpg?token=xyz"); !ok {
		t.Error("Should find image even with different query params")
	}
}
