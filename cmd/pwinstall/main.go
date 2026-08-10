// pwinstall pre-downloads the Playwright driver (no browsers) into
// PLAYWRIGHT_DRIVER_PATH, matching the RunOptions used by the scraper's
// browser pool at runtime (internal/scraper/pool.go). It exists so the
// production Docker image can warm the driver at build time without
// pulling browser binaries, since browserless is used via CDP instead.
package main

import (
	"log"

	"github.com/mxschmitt/playwright-go"
)

func main() {
	if err := playwright.Install(&playwright.RunOptions{SkipInstallBrowsers: true}); err != nil {
		log.Fatalf("could not install playwright driver: %v", err)
	}
}
