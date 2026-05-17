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
	"sync"
	"time"

	"github.com/gatuno/scraper/internal/models"
	"github.com/playwright-community/playwright-go"
	"golang.org/x/time/rate"
)

type Scraper struct {
	pool         *BrowserPool
	cacheMaxSize int64
	limiters     sync.Map // string (domain) -> *rate.Limiter
}

func NewScraper(pool *BrowserPool, cacheMaxSize int64) *Scraper {
	return &Scraper{
		pool:         pool,
		cacheMaxSize: cacheMaxSize,
	}
}

func (s *Scraper) getLimiter(targetUrl string) *rate.Limiter {
	u, err := url.Parse(targetUrl)
	domain := "global"
	if err == nil && u.Host != "" {
		domain = u.Host
	}

	if l, ok := s.limiters.Load(domain); ok {
		return l.(*rate.Limiter)
	}

	// Default: 3 requests per second, burst of 5
	l := rate.NewLimiter(rate.Every(333*time.Millisecond), 5)
	actual, _ := s.limiters.LoadOrStore(domain, l)
	return actual.(*rate.Limiter)
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

func (s *Scraper) ScrapeChapter(ctx context.Context, req models.ScrapingChapterRequest) (string, []string, <-chan ImageResult, func(), error) {
	var bCtx playwright.BrowserContext
	var err error
	isCustomContext := false

	customUA := req.WebsiteConfig.Headers["User-Agent"]
	if req.WebsiteConfig.ProxyURL != "" || customUA != "" {
		isCustomContext = true
		opts := playwright.BrowserNewContextOptions{}
		if req.WebsiteConfig.ProxyURL != "" {
			opts.Proxy = &playwright.Proxy{
				Server: req.WebsiteConfig.ProxyURL,
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
		return "", nil, nil, func() {}, err
	}

	cleanup := func() {
		if !isCustomContext {
			s.pool.Release(bCtx)
		} else {
			bCtx.Close()
		}
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

	if err := s.preparePage(page, bCtx, req.WebsiteConfig, req.TargetURL); err != nil {
		cleanup()
		return "", nil, nil, func() {}, err
	}

	titleElement := page.Locator(req.WebsiteConfig.Selectors.ChapterTitle).First()
	scrapedTitle, _ := titleElement.TextContent()
	if scrapedTitle == "" {
		scrapedTitle, _ = page.Title()
	}

	log.Printf("Starting count-based scroll for lazy-loaded images...")
	// Aggressive Scroll (Count-based)
	_, err = page.Evaluate(aggressiveScrollJS, map[string]interface{}{
		"scrollPauseMs":   1500,
		"stabilityChecks": 3,
		"imageSelector":   req.WebsiteConfig.Selectors.ChapterImages,
	})
	if err != nil {
		log.Printf("Scroll failed: %v", err)
	}

	// Execute PosScript
	if req.WebsiteConfig.PosScript != "" {
		page.Evaluate(req.WebsiteConfig.PosScript)
	}

	imageLocators := page.Locator(req.WebsiteConfig.Selectors.ChapterImages)
	count, err := imageLocators.Count()
	if err != nil {
		cleanup()
		return "", nil, nil, func() {}, err
	}
	log.Printf("Found %d images via selectors", count)

	imageUrls := make([]string, 0, count)
	for i := 0; i < count; i++ {
		img := imageLocators.Nth(i)
		src, _ := img.GetAttribute("src")
		if src == "" {
			src, _ = img.GetAttribute("data-src")
		}
		if src != "" {
			imageUrls = append(imageUrls, src)
		}
	}

	results := s.ScrapeBatchImages(ctx, page, req.WebsiteConfig, imageUrls)
	return scrapedTitle, imageUrls, results, cleanup, nil
}

func (s *Scraper) ScrapeBatchImages(ctx context.Context, page playwright.Page, config models.WebsiteConfig, imageUrls []string) <-chan ImageResult {
	results := make(chan ImageResult, len(imageUrls))
	
	go func() {
		defer close(results)
		if page != nil {
			// We only close the page if we are in ScrapeChapter context. 
			// In other contexts, the caller might manage the page.
			// However, for simplicity and safety in the new methods, 
			// we'll handle lifecycle in the higher-level ScrapeX methods.
		}

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
		limiter := s.getLimiter("global") // Default domain for batch
		client := &http.Client{Timeout: 30 * time.Second}

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					// 2. Rate Limit & Jitter
					limiter.Wait(ctx)
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

					// Fallback to direct download if browser APIRequest not available or failed
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
						
						for k, v := range config.Headers {
							hReq.Header.Set(k, v)
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

func (s *Scraper) ScrapeUpdateBook(ctx context.Context, req models.ScrapingUpdateBookRequest) (models.ScrapingBookCompleted, error) {
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

	if err := s.preparePage(page, bCtx, req.WebsiteConfig, req.TargetURL); err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	// Execute PosScript
	if req.WebsiteConfig.PosScript != "" {
		page.Evaluate(req.WebsiteConfig.PosScript)
		time.Sleep(2 * time.Second)
	}

	// Final wait before extraction
	time.Sleep(2 * time.Second)

	extractScript := req.BookInfoExtractScript
	if extractScript == "" {
		extractScript = req.Script
	}
	if extractScript == "" {
		extractScript = req.WebsiteConfig.Selectors.BookInfoExtractScript
	}

	if extractScript == "" {
		return models.ScrapingBookCompleted{}, fmt.Errorf("BookInfoExtractScript is empty")
	}

	// We use page.Evaluate directly on the script. Playwright handles both sync and async return values.
	res, err := page.Evaluate(extractScript)
	if err != nil {
		log.Printf("ERROR: Script evaluation failed for %s: %v", req.TargetURL, err)
		return models.ScrapingBookCompleted{}, err
	}

	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("ERROR: Failed to marshal script result for %s: %v", req.TargetURL, err)
		return models.ScrapingBookCompleted{}, err
	}

	var result models.ScrapingBookCompleted
	if err := json.Unmarshal(data, &result); err != nil {
		log.Printf("ERROR: Failed to unmarshal script result for %s: %v. Raw data: %s", req.TargetURL, err, string(data))
		return models.ScrapingBookCompleted{}, err
	}

	// Fallback for Title
	if result.Title == "" {
		result.Title, _ = page.Title()
	}

	if len(result.Chapters) == 0 {
		log.Printf("WARNING: No chapters extracted for %s. Page Title: %s", req.TargetURL, result.Title)
		// Log a bit of the body to debug
		bodySnippet, _ := page.Evaluate(`document.body.innerText.substring(0, 500)`)
		log.Printf("Page Body Snippet: %v", bodySnippet)
	}

	log.Printf("Book info extracted: %s (Chapters: %d, Covers: %d)", result.Title, len(result.Chapters), len(result.Covers))

	result.JobID = req.JobID
	result.BookID = req.BookID
	result.TargetURL = req.TargetURL

	return result, nil
}

func (s *Scraper) ScrapeNewBook(ctx context.Context, req models.ScrapingNewBookRequest) (models.ScrapingBookCompleted, error) {
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

	if err := s.preparePage(page, bCtx, req.WebsiteConfig, req.TargetURL); err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	// Execute PosScript
	if req.WebsiteConfig.PosScript != "" {
		page.Evaluate(req.WebsiteConfig.PosScript)
		time.Sleep(2 * time.Second)
	}

	// Final wait before extraction
	time.Sleep(2 * time.Second)

	extractScript := req.NewBookExtractScript
	if extractScript == "" {
		extractScript = req.Script
	}
	if extractScript == "" {
		extractScript = req.WebsiteConfig.Selectors.NewBookExtractScript
	}

	if extractScript == "" {
		return models.ScrapingBookCompleted{}, fmt.Errorf("NewBookExtractScript is empty")
	}

	// We use page.Evaluate directly on the script. Playwright handles both sync and async return values.
	res, err := page.Evaluate(extractScript)
	if err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	data, err := json.Marshal(res)
	if err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	var result models.ScrapingBookCompleted
	if err := json.Unmarshal(data, &result); err != nil {
		return models.ScrapingBookCompleted{}, err
	}

	// Fallback for Title
	if result.Title == "" {
		result.Title, _ = page.Title()
	}

	if len(result.Chapters) == 0 {
		log.Printf("WARNING: No chapters extracted for %s. Page Title: %s", req.TargetURL, result.Title)
		// Log a bit of the body to debug
		bodySnippet, _ := page.Evaluate(`document.body.innerText.substring(0, 500)`)
		log.Printf("Page Body Snippet: %v", bodySnippet)
	}

	log.Printf("New book info extracted: %s (Chapters: %d, Covers: %d)", result.Title, len(result.Chapters), len(result.Covers))

	result.JobID = req.JobID
	result.TargetURL = req.TargetURL

	return result, nil
}

func (s *Scraper) ScrapeCovers(ctx context.Context, req models.ScrapingCoversRequest) (<-chan ImageResult, func()) {
	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	page, err := bCtx.NewPage()
	if err != nil {
		s.pool.Release(bCtx)
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	if err := s.preparePage(page, bCtx, req.WebsiteConfig, req.TargetURL); err != nil {
		page.Close()
		s.pool.Release(bCtx)
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	var imageUrls []string
	for _, c := range req.Covers {
		imageUrls = append(imageUrls, c.URL)
	}

	return s.ScrapeBatchImages(ctx, page, req.WebsiteConfig, imageUrls), func() {
		page.Close()
		s.pool.Release(bCtx)
	}
}

func (s *Scraper) ScrapeImages(ctx context.Context, req models.ScrapingImagesRequest) (<-chan ImageResult, func()) {
	bCtx, err := s.pool.Acquire(ctx)
	if err != nil {
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	page, err := bCtx.NewPage()
	if err != nil {
		s.pool.Release(bCtx)
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	// We might not have a TargetURL for generic images, but if we do, it helps with headers/cookies
	targetURL := ""
	if len(req.ImageURLs) > 0 {
		targetURL = req.ImageURLs[0]
	}

	if err := s.preparePage(page, bCtx, req.WebsiteConfig, targetURL); err != nil {
		page.Close()
		s.pool.Release(bCtx)
		res := make(chan ImageResult, 1)
		res <- ImageResult{Error: err}
		close(res)
		return res, func() {}
	}

	return s.ScrapeBatchImages(ctx, page, req.WebsiteConfig, req.ImageURLs), func() {
		page.Close()
		s.pool.Release(bCtx)
	}
}

func (s *Scraper) ExecuteTestScript(ctx context.Context, req models.ScrapingTestRequest) (interface{}, error) {
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

	// Advanced Stealth Injection
	page.AddInitScript(playwright.Script{
		Content: playwright.String(`
			Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
			window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {}, app: {} };
			Object.defineProperty(navigator, 'plugins', { get: () => [{ description: 'Portable Document Format', filename: 'internal-pdf-viewer', name: 'Chrome PDF Viewer' }] });
			Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
			const getParameter = WebGLRenderingContext.prototype.getParameter;
			WebGLRenderingContext.prototype.getParameter = function(parameter) {
				if (parameter === 37445) return 'Intel Inc.';
				if (parameter === 37446) return 'Intel(R) Iris(R) Xe Graphics';
				return getParameter.apply(this, arguments);
			};
		`),
	})

	_, err = page.Goto(req.TargetURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateLoad,
	})
	if err != nil {
		return nil, err
	}

	return page.Evaluate(req.Script)
}

func (s *Scraper) preparePage(page playwright.Page, bCtx playwright.BrowserContext, config models.WebsiteConfig, targetURL string) error {
	// Add console listener
	page.On("console", func(msg playwright.ConsoleMessage) {
		log.Printf("Browser console (%s): %s", targetURL, msg.Text())
	})

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

	// Inject Cookies
	if len(config.Cookies) > 0 {
		var playwrightCookies []playwright.OptionalCookie
		for _, c := range config.Cookies {
			playwrightCookies = append(playwrightCookies, playwright.OptionalCookie{
				Name:   c.Name,
				Value:  c.Value,
				Domain: playwright.String(c.Domain),
				Path:   playwright.String("/"),
			})
		}
		bCtx.AddCookies(playwrightCookies)
	}

	timeout := 60000.0
	if config.EnableAdaptiveTimeouts && config.TimeoutMultipliers.Medium > 0 {
		timeout *= config.TimeoutMultipliers.Medium
	}

	log.Printf("Navigating to: %s", targetURL)
	_, err := page.Goto(targetURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateLoad,
		Timeout:   playwright.Float(timeout),
	})
	if err != nil {
		return err
	}

	// Injection of LocalStorage/SessionStorage
	if len(config.LocalStorage) > 0 || len(config.SessionStorage) > 0 {
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
			page.Reload()
		}
	}

	if config.CloudflareBypass {
		log.Printf("Cloudflare bypass enabled, waiting 5s...")
		time.Sleep(5 * time.Second)
	}

	// Handle age confirmation popup for SPAs if script is provided
	if config.PreScript != "" {
		log.Printf("Executing PreScript...")
		if _, err := page.Evaluate(config.PreScript); err != nil {
			log.Printf("WARNING: PreScript execution failed: %v", err)
		}
		// Wait for DOM to react (critical for SPAs)
		time.Sleep(5 * time.Second)
	} else {
		// Default wait for SPAs
		time.Sleep(3 * time.Second)
	}

	// Basic scroll to trigger lazy-loading
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
