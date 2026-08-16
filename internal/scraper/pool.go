package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gatuno/scraper/internal/metrics"
	"github.com/gatuno/scraper/internal/obs"
	"github.com/mxschmitt/playwright-go"
)

type BrowserPool struct {
	pw          *playwright.Playwright
	browser     playwright.Browser
	browserURL  string
	connectedAt time.Time

	mu  sync.Mutex
	sem chan struct{}
}

func playwrightRunOptions() *playwright.RunOptions {
	opts := &playwright.RunOptions{
		SkipInstallBrowsers: true,
	}
	// Se PLAYWRIGHT_DRIVER_PATH estiver definido (ex.: imagem Docker com driver pré-baixado),
	// usa o driver local — sem nenhum download da CDN.
	if driverPath := os.Getenv("PLAYWRIGHT_DRIVER_PATH"); driverPath != "" {
		opts.DriverDirectory = driverPath
	}
	return opts
}

func NewBrowserPool(browserURL string, poolSize int) (*BrowserPool, error) {
	err := playwright.Install(playwrightRunOptions())
	if err != nil {
		return nil, fmt.Errorf("could not install playwright driver: %v", err)
	}

	pw, err := playwright.Run()
	if err != nil {
		return nil, fmt.Errorf("could not start playwright: %v", err)
	}

	pool := &BrowserPool{
		pw:         pw,
		browserURL: browserURL,
		sem:        make(chan struct{}, poolSize),
	}

	if err := pool.ensureBrowser(context.Background()); err != nil {
		pw.Stop()
		return nil, err
	}

	return pool, nil
}

func (p *BrowserPool) ensureBrowser(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.browser != nil && p.browser.IsConnected() {
		return nil
	}

	// A prior browser existing here means we lost the connection and are
	// reconnecting, as opposed to connecting for the very first time.
	isReconnect := p.browser != nil
	previousAge := time.Since(p.connectedAt)

	// Close old browser if it exists but is disconnected
	if p.browser != nil {
		_ = p.browser.Close()
	}

	var err error
	if p.browserURL != "" {
		// Browserless v2 launches the browser per-connection using a single
		// "launch" query param (JSON-encoded), replacing the v1 scheme of one
		// query param per Chrome flag.
		var launchArgs []byte
		launchArgs, err = json.Marshal(map[string][]string{
			"args": {
				"--disable-web-security",
				"--disable-features=IsolateOrigins,site-per-process",
				"--disable-blink-features=AutomationControlled",
			},
		})
		if err != nil {
			return fmt.Errorf("could not encode browserless launch args: %v", err)
		}

		finalURL := p.browserURL
		connector := "?"
		if strings.Contains(finalURL, "?") {
			connector = "&"
		}
		finalURL += fmt.Sprintf("%slaunch=%s", connector, url.QueryEscape(string(launchArgs)))

		p.browser, err = p.pw.Chromium.ConnectOverCDP(finalURL)
		if err != nil {
			return fmt.Errorf("could not connect to browserless: %v", err)
		}
	} else {
		err = playwright.Install(playwrightRunOptions())
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

	p.connectedAt = time.Now()

	if isReconnect {
		metrics.BrowserReconnectsTotal.Inc()
		obs.From(ctx).Warn("browser reconnected", "browser_url", p.browserURL, "previous_connection_age", previousAge.String())
	} else {
		obs.From(ctx).Info("browser connection established", "browser_url", p.browserURL)
	}
	return nil
}

// IsConnected reports whether the pool currently holds a live browser
// connection. Used by the /readyz endpoint (OBS-06) to detect a scraper
// that has lost its browser without a metrics scrape or manual check.
func (p *BrowserPool) IsConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.browser != nil && p.browser.IsConnected()
}

func (p *BrowserPool) Acquire(ctx context.Context) (playwright.BrowserContext, error) {
	select {
	case p.sem <- struct{}{}:
		metrics.BrowserPoolInUse.Inc()
		if err := p.ensureBrowser(ctx); err != nil {
			<-p.sem
			metrics.BrowserPoolInUse.Dec()
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
			metrics.BrowserPoolInUse.Dec()
			return nil, err
		}
		return bCtx, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *BrowserPool) NewContextWithOpts(ctx context.Context, opts playwright.BrowserNewContextOptions) (playwright.BrowserContext, error) {
	// Inject required stealth/bypass options if not provided
	if opts.BypassCSP == nil {
		opts.BypassCSP = playwright.Bool(true)
	}
	if opts.IgnoreHttpsErrors == nil {
		opts.IgnoreHttpsErrors = playwright.Bool(true)
	}

	select {
	case p.sem <- struct{}{}:
		metrics.BrowserPoolInUse.Inc()
		if err := p.ensureBrowser(ctx); err != nil {
			<-p.sem
			metrics.BrowserPoolInUse.Dec()
			return nil, err
		}

		bCtx, err := p.browser.NewContext(opts)
		if err != nil {
			<-p.sem
			metrics.BrowserPoolInUse.Dec()
			return nil, err
		}
		return bCtx, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *BrowserPool) Release(bCtx playwright.BrowserContext) {
	if bCtx != nil {
		_ = bCtx.Close()
	}
	// Drain one slot from semaphore
	select {
	case <-p.sem:
		metrics.BrowserPoolInUse.Dec()
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
