package scraper

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/playwright-community/playwright-go"
)

type BrowserPool struct {
	pw         *playwright.Playwright
	browser    playwright.Browser
	browserURL string
	
	mu  sync.Mutex
	sem chan struct{}
}

func NewBrowserPool(browserURL string, poolSize int) (*BrowserPool, error) {
	pw, err := playwright.Run()
	if err != nil {
		return nil, fmt.Errorf("could not start playwright: %v", err)
	}

	pool := &BrowserPool{
		pw:         pw,
		browserURL: browserURL,
		sem:        make(chan struct{}, poolSize),
	}

	if err := pool.ensureBrowser(); err != nil {
		pw.Stop()
		return nil, err
	}

	return pool, nil
}

func (p *BrowserPool) ensureBrowser() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.browser != nil && p.browser.IsConnected() {
		return nil
	}

	// Close old browser if it exists but is disconnected
	if p.browser != nil {
		_ = p.browser.Close()
	}

	var err error
	if p.browserURL != "" {
		// Append stealth and security bypass flags to Browserless URL
		finalURL := p.browserURL
		connector := "?"
		if strings.Contains(finalURL, "?") {
			connector = "&"
		}
		
		flags := []string{
			"--disable-web-security",
			"--disable-features=IsolateOrigins,site-per-process",
			"--disable-blink-features=AutomationControlled",
		}
		
		for _, flag := range flags {
			finalURL += fmt.Sprintf("%s%s=true", connector, flag)
			connector = "&"
		}

		p.browser, err = p.pw.Chromium.ConnectOverCDP(finalURL)
		if err != nil {
			return fmt.Errorf("could not connect to browserless: %v", err)
		}
	} else {
		err = playwright.Install()
		if err != nil {
			return fmt.Errorf("could not install playwright: %v", err)
		}
		p.browser, err = p.pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
			Args: []string{
				"--disable-blink-features=AutomationControlled",
				"--no-sandbox",
				"--disable-setuid-sandbox",
				"--disable-web-security",
				"--disable-features=IsolateOrigins,site-per-process",
			},
			Headless: playwright.Bool(true),
		})
		if err != nil {
			return fmt.Errorf("could not launch browser: %v", err)
		}
	}

	log.Printf("Browser connection established (URL: %s)", p.browserURL)
	return nil
}

func (p *BrowserPool) Acquire(ctx context.Context) (playwright.BrowserContext, error) {
	select {
	case p.sem <- struct{}{}:
		if err := p.ensureBrowser(); err != nil {
			<-p.sem
			return nil, err
		}

		bCtx, err := p.browser.NewContext(playwright.BrowserNewContextOptions{
			UserAgent: playwright.String("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
			Viewport: &playwright.Size{
				Width:  1920,
				Height: 1080,
			},
			BypassCSP:         playwright.Bool(true),
			IgnoreHttpsErrors: playwright.Bool(true),
		})
		if err != nil {
			<-p.sem
			return nil, err
		}
		return bCtx, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *BrowserPool) NewContextWithOpts(opts playwright.BrowserNewContextOptions) (playwright.BrowserContext, error) {
	// Inject required stealth/bypass options if not provided
	if opts.BypassCSP == nil {
		opts.BypassCSP = playwright.Bool(true)
	}
	if opts.IgnoreHttpsErrors == nil {
		opts.IgnoreHttpsErrors = playwright.Bool(true)
	}

	select {
	case p.sem <- struct{}{}:
		if err := p.ensureBrowser(); err != nil {
			<-p.sem
			return nil, err
		}

		bCtx, err := p.browser.NewContext(opts)
		if err != nil {
			<-p.sem
			return nil, err
		}
		return bCtx, nil
	default:
		return nil, fmt.Errorf("browser pool at maximum capacity")
	}
}

func (p *BrowserPool) Release(bCtx playwright.BrowserContext) {
	if bCtx != nil {
		_ = bCtx.Close()
	}
	// Drain one slot from semaphore
	select {
	case <-p.sem:
	default:
		// Should not happen if Acquire/Release are balanced
	}
}

func (p *BrowserPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	
	if p.browser != nil {
		if err := p.browser.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.pw != nil {
		if err := p.pw.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool: %v", errs)
	}
	return nil
}
