package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gatuno/scraper/internal/models"
	"github.com/gatuno/scraper/internal/ratelimit"
	"github.com/playwright-community/playwright-go"
)

type Scraper struct {
	pool         *BrowserPool
	cacheMaxSize int64
	limiter      ratelimit.RateLimiter
	semaphore    ratelimit.Semaphore
}

func NewScraper(pool *BrowserPool, cacheMaxSize int64, limiter ratelimit.RateLimiter, semaphore ratelimit.Semaphore) *Scraper {
	return &Scraper{
		pool:         pool,
		cacheMaxSize: cacheMaxSize,
		limiter:      limiter,
		semaphore:    semaphore,
	}
}

func extractDomain(targetUrl string) string {
	u, err := url.Parse(targetUrl)
	if err == nil && u.Host != "" {
		return u.Host
	}
	return "global"
}

type ImageResult struct {
	Index int
	Data  []byte
	Error error
}

const aggressiveScrollJS = `
async (options) => {
	const { scrollPauseMs, stabilityChecks, imageSelector } = options;
	
	let lastCount = 0;
	let sameCount = 0;
	let maxAttempts = 30; 
	
	const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

	for(let i=0; i < maxAttempts; i++) {
		window.scrollTo(0, document.body.scrollHeight);
		await sleep(scrollPauseMs);
		
		let currentCount = document.querySelectorAll(imageSelector).length;
		if (currentCount > 0 && currentCount === lastCount) {
			sameCount++;
		} else if (currentCount > lastCount) {
			sameCount = 0;
			lastCount = currentCount;
		}
		
		// If stabilized for the requested amount of checks, stop
		if (currentCount > 0 && sameCount >= stabilityChecks) break;
	}
}
`

func (s *Scraper) ScrapeChapter(ctx context.Context, req models.ScrapingChapterRequest, config models.WebsiteConfig) (string, []string, <-chan ImageResult, func(), error) {
	domain := extractDomain(req.TargetURL)
	releaseSem, err := s.semaphore.Acquire(ctx, domain)
	if err != nil {
		return "", nil, nil, func() {}, err
	}

	var bCtx playwright.BrowserContext
	isCustomContext := false

	customUA := config.Headers["User-Agent"]
	if config.ProxyURL != "" || customUA != "" {
		isCustomContext = true
		opts := playwright.BrowserNewContextOptions{}
		if config.ProxyURL != "" {
			opts.Proxy = &playwright.Proxy{
				Server: config.ProxyURL,
			}
		}
		if customUA != "" {
			opts.UserAgent = playwright.String(customUA)
		}
		bCtx, err = s.pool.NewContextWithOpts(opts)
	} else {
		bCtx, err = s.pool.Acquire(ctx)
	}

	if err != nil {
		releaseSem()
		return "", nil, nil, func() {}, err
	}

	cleanup := func() {
		if !isCustomContext {
			s.pool.Release(bCtx)
		} else {
			bCtx.Close()
		}
		releaseSem()
	}

	page, err := bCtx.NewPage()
	if err != nil {
		cleanup()
		return "", nil, nil, func() {}, err
	}

	// Wrap cleanup to close page as well
	origCleanup := cleanup
	cleanup = func() {
		page.Close()
		origCleanup()
	}

	interceptedImages := &sync.Map{}
	if err := s.preparePage(page, bCtx, config, req.TargetURL, interceptedImages); err != nil {
		cleanup()
		return "", nil, nil, func() {}, err
	}

	titleSelector := config.ChapterTitleSelector
	var scrapedTitle string
	if titleSelector != "" {
		titleElement := page.Locator(titleSelector).First()
		scrapedTitle, _ = titleElement.TextContent()
	}
	if scrapedTitle == "" {
		scrapedTitle, _ = page.Title()
	}

	log.Printf("Starting count-based scroll for lazy-loaded images...")
	// Aggressive Scroll (Count-based)
	imagesSelector := config.Selector
	if req.ChapterSpecificSelector != "" {
		imagesSelector = req.ChapterSpecificSelector
	}

	if imagesSelector == "" {
		cleanup()
		return "", nil, nil, func() {}, fmt.Errorf("images selector is empty (not found in website config or chapter request)")
	}

	_, err = page.Evaluate(aggressiveScrollJS, map[string]interface{}{
		"scrollPauseMs":   1500,
		"stabilityChecks": 3,
		"imageSelector":   imagesSelector,
	})
	if err != nil {
		log.Printf("Scroll failed: %v", err)
	}

	// Execute PosScript
	if config.PosScript != "" {
		page.Evaluate(config.PosScript)
	}

	imageLocators := page.Locator(imagesSelector)
	count, err := imageLocators.Count()
	if err != nil {
		cleanup()
		return "", nil, nil, func() {}, err
	}
	log.Printf("Found %d images via selectors", count)

	imageUrls := make([]string, 0, count)
	for i := 0; i < count; i++ {
		img := imageLocators.Nth(i)
		srcI, _ := img.Evaluate("node => node.currentSrc || node.src", nil)
		src, _ := srcI.(string)
		if src == "" {
			src, _ = img.GetAttribute("data-src")
		}
		if src != "" {
			imageUrls = append(imageUrls, src)
		}
	}

	results := s.ScrapeBatchImages(ctx, page, config, imageUrls, interceptedImages)
	return scrapedTitle, imageUrls, results, cleanup, nil
}

func (s *Scraper) ScrapeBatchImages(ctx context.Context, page playwright.Page, config models.WebsiteConfig, imageUrls []string, interceptedImages *sync.Map) <-chan ImageResult {
	results := make(chan ImageResult, len(imageUrls))

	go func() {
		defer close(results)

		numWorkers := 5
		if len(imageUrls) < numWorkers {
			numWorkers = len(imageUrls)
		}
		if numWorkers <= 0 {
			return
		}

		type job struct {
			index int
			url   string
		}
		jobs := make(chan job, len(imageUrls))
		for i, url := range imageUrls {
			jobs <- job{index: i, url: url}
		}
		close(jobs)

		var wg sync.WaitGroup
		client := &http.Client{Timeout: 30 * time.Second}

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					// 1. Check Network Interception Map
					if interceptedImages != nil {
						if data, ok := interceptedImages.Load(j.url); ok {
							results <- ImageResult{Index: j.index, Data: data.([]byte)}
							continue
						}
					}

					// 2. Rate Limit per domain & Jitter
					u, _ := url.Parse(j.url)
					domain := "global"
					if u != nil && u.Host != "" {
						domain = u.Host
					}
					
					if err := s.limiter.Wait(ctx, domain); err != nil {
						results <- ImageResult{Index: j.index, Error: err}
						continue
					}
					
					jitter := time.Duration(200+rand.Intn(500)) * time.Millisecond
					time.Sleep(jitter)

					// 3. Use browser's APIRequest if page is provided (Bypasses CORS but keeps TLS/Cookies)
					if page != nil {
						// Merge headers
						headers := make(map[string]string)
						for k, v := range config.Headers {
							headers[k] = v
						}

						// Crucial for bypassing hotlink protections
						if _, ok := headers["Referer"]; !ok {
							headers["Referer"] = page.URL()
						}

						resp, err := page.Context().Request().Get(j.url, playwright.APIRequestContextGetOptions{
							Headers: headers,
						})

						if err == nil {
							data, err := resp.Body()
							if err == nil && resp.Status() == 200 {
								results <- ImageResult{Index: j.index, Data: data}
								continue // Success via browser APIRequest
							}
							log.Printf("Browser APIRequest status %d for %s (Error: %v)", resp.Status(), j.url, err)
						} else {
							log.Printf("Browser APIRequest failed for %s: %v. Falling back to HTTP client.", j.url, err)
						}
					}

					// 4. Fallback to direct download if browser APIRequest not available or failed
					var data []byte
					var dlErr error
					var success bool
					maxRetries := 3

					for attempt := 0; attempt < maxRetries; attempt++ {
						hReq, errReq := http.NewRequestWithContext(ctx, "GET", j.url, nil)
						if errReq != nil {
							dlErr = errReq
							break
						}

						// Mimic browser headers more closely
						for k, v := range config.Headers {
							hReq.Header.Set(k, v)
						}
						if hReq.Header.Get("User-Agent") == "" {
							hReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
						}
						if hReq.Header.Get("Accept") == "" {
							hReq.Header.Set("Accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
						}
						if hReq.Header.Get("Referer") == "" && page != nil {
							hReq.Header.Set("Referer", page.URL())
						}
						hReq.Header.Set("Sec-Fetch-Dest", "image")
						hReq.Header.Set("Sec-Fetch-Mode", "no-cors")
						hReq.Header.Set("Sec-Fetch-Site", "same-origin")

						// Sync cookies from browser context to http client
						if page != nil {
							cookies, _ := page.Context().Cookies()
							for _, c := range cookies {
								hReq.AddCookie(&http.Cookie{
									Name:  c.Name,
									Value: c.Value,
								})
							}
						}

						resp, errResp := client.Do(hReq)
						if errResp != nil {
							dlErr = errResp
							// Wait before retry (Backoff)
							time.Sleep(time.Duration(attempt+1) * time.Second)
							continue
						}

						if resp.StatusCode == http.StatusOK {
							data, dlErr = io.ReadAll(resp.Body)
							resp.Body.Close()
							if dlErr == nil {
								success = true
								break // Exit retry loop
							}
						} else if resp.StatusCode == 403 || resp.StatusCode == 429 {
							// Rate limit or forbidden, wait longer
							resp.Body.Close()
							dlErr = fmt.Errorf("bad status: %d", resp.StatusCode)
							time.Sleep(time.Duration((attempt+1)*2) * time.Second)
							continue
						} else {
							resp.Body.Close()
							dlErr = fmt.Errorf("bad status: %d", resp.StatusCode)
							break // Other errors, don't retry
						}
					}

					if success {
						results <- ImageResult{Index: j.index, Data: data}
					} else {
						results <- ImageResult{Index: j.index, Error: dlErr}
					}
				}
			}()
		}

		wg.Wait()
	}()

	return results
}

// bookScrapeInput bundles the parameters required by scrapeBookPage.
type bookScrapeInput struct {
	JobID         string
	BookID        string
	TargetURL     string
	WebsiteConfig models.WebsiteConfig
	Script        string
}

// scrapeBookPage is the shared implementation for ScrapeUpdateBook and ScrapeNewBook.
// It acquires a browser context, navigates to the page, evaluates the extraction
// script and returns a parsed ScrapingBookCompleted result.
func (s *Scraper) scrapeBookPage(ctx context.Context, in bookScrapeInput) (models.ScrapingBookCompleted, error) {
	domain := extractDomain(in.TargetURL)
	releaseSem, err := s.semaphore.Acquire(ctx, domain)
	if err != nil {
		return models.ScrapingBookCompleted{}, err
	}
	defer releaseSem()

	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		return models.ScrapingBookCompleted{}, err
	}
	defer s.pool.Release(bCtx)

	page, err := bCtx.NewPage()
	if err != nil {
		return models.ScrapingBookCompleted{}, err
	}
	defer page.Close()

	if err := s.preparePage(page, bCtx, in.WebsiteConfig, in.TargetURL, nil); err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	// Execute PosScript and allow DOM to settle
	if in.WebsiteConfig.PosScript != "" {
		page.Evaluate(in.WebsiteConfig.PosScript)
		time.Sleep(2 * time.Second)
	}

	// Final wait before extraction
	time.Sleep(2 * time.Second)

	if in.Script == "" {
		return models.ScrapingBookCompleted{}, fmt.Errorf("extract script is empty")
	}

	res, err := page.Evaluate(in.Script)
	if err != nil {
		log.Printf("ERROR: Script evaluation failed for %s: %v", in.TargetURL, err)
		return models.ScrapingBookCompleted{}, err
	}

	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("ERROR: Failed to marshal script result for %s: %v", in.TargetURL, err)
		return models.ScrapingBookCompleted{}, err
	}

	var result models.ScrapingBookCompleted
	if err := json.Unmarshal(data, &result); err != nil {
		log.Printf("ERROR: Failed to unmarshal script result for %s: %v. Raw: %s", in.TargetURL, err, string(data))
		return models.ScrapingBookCompleted{}, err
	}

	// Fallback for Title
	if result.Title == "" {
		result.Title, _ = page.Title()
	}

	if len(result.Chapters) == 0 {
		log.Printf("WARNING: No chapters extracted for %s. Page Title: %s", in.TargetURL, result.Title)
		bodySnippet, _ := page.Evaluate(`document.body.innerText.substring(0, 500)`)
		log.Printf("Page Body Snippet: %v", bodySnippet)
	}

	result.JobID = in.JobID
	result.BookID = in.BookID
	result.TargetURL = in.TargetURL

	return result, nil
}

func (s *Scraper) ScrapeUpdateBook(ctx context.Context, req models.ScrapingUpdateBookRequest, config models.WebsiteConfig) (models.ScrapingBookCompleted, error) {
	script := req.BookInfoExtractScript
	if script == "" {
		script = req.Script
	}
	if script == "" {
		script = config.BookInfoExtractScript
	}

	result, err := s.scrapeBookPage(ctx, bookScrapeInput{
		JobID:         req.JobID,
		BookID:        req.BookID,
		TargetURL:     req.TargetURL,
		WebsiteConfig: config,
		Script:        script,
	})
	if err != nil {
		return result, err
	}
	log.Printf("Book info extracted: %s (Chapters: %d, Covers: %d)", result.Title, len(result.Chapters), len(result.Covers))
	return result, nil
}

func (s *Scraper) ScrapeNewBook(ctx context.Context, req models.ScrapingNewBookRequest, config models.WebsiteConfig) (models.ScrapingBookCompleted, error) {
	script := req.NewBookExtractScript
	if script == "" {
		script = req.Script
	}
	if script == "" {
		script = config.NewBookExtractScript
	}

	result, err := s.scrapeBookPage(ctx, bookScrapeInput{
		JobID:         req.JobID,
		TargetURL:     req.TargetURL,
		WebsiteConfig: config,
		Script:        script,
	})
	if err != nil {
		return result, err
	}
	log.Printf("New book info extracted: %s (Chapters: %d, Covers: %d)", result.Title, len(result.Chapters), len(result.Covers))
	return result, nil
}

func (s *Scraper) ScrapeCovers(ctx context.Context, req models.ScrapingCoversRequest, config models.WebsiteConfig) (<-chan ImageResult, func()) {
	domain := extractDomain(req.TargetURL)
	releaseSem, err := s.semaphore.Acquire(ctx, domain)
	if err != nil {
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	page, err := bCtx.NewPage()
	if err != nil {
		s.pool.Release(bCtx)
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	interceptedImages := &sync.Map{}
	if err := s.preparePage(page, bCtx, config, req.TargetURL, interceptedImages); err != nil {
		page.Close()
		s.pool.Release(bCtx)
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	var imageUrls []string
	for _, c := range req.Covers {
		imageUrls = append(imageUrls, c.URL)
	}

	return s.ScrapeBatchImages(ctx, page, config, imageUrls, interceptedImages), func() {
		page.Close()
		s.pool.Release(bCtx)
		releaseSem()
	}
}

func (s *Scraper) ScrapeImages(ctx context.Context, req models.ScrapingImagesRequest, config models.WebsiteConfig) (<-chan ImageResult, func()) {
	targetURL := ""
	if len(req.ImageURLs) > 0 {
		targetURL = req.ImageURLs[0]
	}
	domain := extractDomain(targetURL)
	releaseSem, err := s.semaphore.Acquire(ctx, domain)
	if err != nil {
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	page, err := bCtx.NewPage()
	if err != nil {
		s.pool.Release(bCtx)
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	interceptedImages := &sync.Map{}
	if err := s.preparePage(page, bCtx, config, targetURL, interceptedImages); err != nil {
		page.Close()
		s.pool.Release(bCtx)
		releaseSem()
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	return s.ScrapeBatchImages(ctx, page, config, req.ImageURLs, interceptedImages), func() {
		page.Close()
		s.pool.Release(bCtx)
		releaseSem()
	}
}

func (s *Scraper) ExecuteTestScript(ctx context.Context, req models.ScrapingTestRequest) (interface{}, error) {
	domain := extractDomain(req.TargetURL)
	releaseSem, err := s.semaphore.Acquire(ctx, domain)
	if err != nil {
		return nil, err
	}
	defer releaseSem()

	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Release(bCtx)

	page, err := bCtx.NewPage()
	if err != nil {
		return nil, err
	}
	defer page.Close()

	if err := s.preparePage(page, bCtx, models.WebsiteConfig{}, req.TargetURL, nil); err != nil {
		return nil, err
	}

	return page.Evaluate(req.Script)
}

func (s *Scraper) preparePage(page playwright.Page, bCtx playwright.BrowserContext, config models.WebsiteConfig, targetURL string, interceptedImages *sync.Map) error {
	// Add console listener with noise filtering
	page.On("console", func(msg playwright.ConsoleMessage) {
		text := msg.Text()
		// Filter noisy/irrelevant logs
		if strings.Contains(text, "Third-party cookie") ||
			strings.Contains(text, "Failed to load resource") ||
			strings.Contains(text, "net::ERR_CONNECTION_RESET") ||
			strings.Contains(text, "status of 404") {
			return
		}
		log.Printf("Browser console (%s): %s", targetURL, text)
	})

	// Network Interception
	if config.UseNetworkInterception && interceptedImages != nil {
		page.On("response", func(resp playwright.Response) {
			go func(r playwright.Response) {
				if r.Request().ResourceType() == "image" && r.Status() == 200 {
					body, err := r.Body()
					if err == nil {
						interceptedImages.Store(r.URL(), body)
					}
				}
			}(resp)
		})
	}

	// Advanced Stealth Injection
	page.AddInitScript(playwright.Script{
		Content: playwright.String(`
			// 1. Hide Webdriver
			Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

			// 2. Mock Chrome runtime
			window.chrome = {
				runtime: {},
				loadTimes: function() {},
				csi: function() {},
				app: {}
			};

			// 3. Mock Plugins
			Object.defineProperty(navigator, 'plugins', {
				get: () => [
					{ description: 'Portable Document Format', filename: 'internal-pdf-viewer', name: 'Chrome PDF Viewer' },
					{ description: 'Google Cloud Print', filename: 'internal-cloud-print-viewer', name: 'Google Cloud Print' }
				]
			});

			// 4. Mock Languages
			Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'pt-BR', 'pt'] });

			// 5. WebGL Evasion (Masking SwiftShader)
			const getParameter = WebGLRenderingContext.prototype.getParameter;
			WebGLRenderingContext.prototype.getParameter = function(parameter) {
				// UNMASKED_VENDOR_WEBGL
				if (parameter === 37445) return 'Intel Inc.';
				// UNMASKED_RENDERER_WEBGL
				if (parameter === 37446) return 'Intel(R) Iris(R) Xe Graphics';
				return getParameter.apply(this, arguments);
			};
		`),
	})

	if len(config.Headers) > 0 {
		page.SetExtraHTTPHeaders(config.Headers)
	}

	// Inject Cookies with enhanced attributes
	if len(config.Cookies) > 0 {
		var playwrightCookies []playwright.OptionalCookie
		for _, c := range config.Cookies {
			path := c.Path
			if path == "" {
				path = "/"
			}
			cookie := playwright.OptionalCookie{
				Name:   c.Name,
				Value:  c.Value,
				Domain: playwright.String(c.Domain),
				Path:   playwright.String(path),
			}
			if c.HttpOnly {
				cookie.HttpOnly = playwright.Bool(true)
			}
			if c.Secure {
				cookie.Secure = playwright.Bool(true)
			}
			if c.SameSite != "" {
				ss := playwright.SameSiteAttribute(c.SameSite)
				cookie.SameSite = &ss
			}
			playwrightCookies = append(playwrightCookies, cookie)
		}
		bCtx.AddCookies(playwrightCookies)
	}
	timeout := 60000.0
	if config.EnableAdaptiveTimeouts && config.TimeoutMultipliers.Medium > 0 {
		timeout *= config.TimeoutMultipliers.Medium
	}

	log.Printf("Navigating to: %s", targetURL)
	_, err := page.Goto(targetURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(timeout),
	})
	if err != nil {
		log.Printf("Navigation failed for %s: %v", targetURL, err)
		return err
	}
	log.Printf("Navigation completed for %s", targetURL)

	// Injection of LocalStorage/SessionStorage
	if len(config.LocalStorage) > 0 || len(config.SessionStorage) > 0 {
		log.Printf("Injecting storage for %s", targetURL)
		storageScript := `(data) => {
			for (const [k, v] of Object.entries(data.local || {})) {
				const val = typeof v === 'string' ? v : JSON.stringify(v);
				localStorage.setItem(k, val);
			}
			for (const [k, v] of Object.entries(data.session || {})) {
				const val = typeof v === 'string' ? v : JSON.stringify(v);
				sessionStorage.setItem(k, val);
			}
		}`
		page.Evaluate(storageScript, map[string]interface{}{
			"local":   config.LocalStorage,
			"session": config.SessionStorage,
		})
		if config.ReloadAfterStorageInjection {
			log.Printf("Reloading page after storage injection for %s", targetURL)
			page.Reload()
		}
	}

	if config.CloudflareBypass {
		log.Printf("Cloudflare bypass enabled, waiting 5s for %s...", targetURL)
		time.Sleep(5 * time.Second)
	}

	// Handle age confirmation popup for SPAs if script is provided
	if config.PreScript != "" {
		log.Printf("Executing PreScript for %s...", targetURL)
		if _, err := page.Evaluate(config.PreScript); err != nil {
			log.Printf("WARNING: PreScript execution failed for %s: %v", targetURL, err)
		}
		// Wait for DOM to react (critical for SPAs)
		time.Sleep(5 * time.Second)
	} else {
		// Default wait for SPAs
		log.Printf("Waiting 3s for SPA/JS initialization for %s...", targetURL)
		time.Sleep(3 * time.Second)
	}

	// Basic scroll to trigger lazy-loading
	log.Printf("Performing initial triggers/scrolls for %s", targetURL)
	if _, err := page.Evaluate(`window.scrollTo(0, document.body.scrollHeight/2)`); err != nil {
		log.Printf("WARNING: Scroll (half) failed: %v", err)
	}
	time.Sleep(1 * time.Second)
	if _, err := page.Evaluate(`window.scrollTo(0, document.body.scrollHeight)`); err != nil {
		log.Printf("WARNING: Scroll (full) failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	return nil
}
