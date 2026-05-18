package main
import (
	"fmt"
	"github.com/playwright-community/playwright-go"
)
func main() {
	err := playwright.Install(&playwright.RunOptions{
		SkipInstallBrowsers: true,
	})
	if err != nil {
		fmt.Printf("Install error: %v\n", err)
		return
	}
	pw, err := playwright.Run()
	if err != nil {
		fmt.Printf("Run error: %v\n", err)
		return
	}
	fmt.Println("Success")
	pw.Stop()
}
