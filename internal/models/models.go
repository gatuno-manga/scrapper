package models

type WebsiteConfig struct {
	ID                           string                 `json:"id"`
	URL                          string                 `json:"url"`
	IsActive                     bool                   `json:"isActive"`
	Name                         string                 `json:"name"`
	CloudflareBypass             bool                   `json:"cloudflareBypass"`
	PreScript                    string                 `json:"preScript"`
	PosScript                    string                 `json:"posScript"`
	Selector                     string                 `json:"selector"` // Corresponds to chapter images
	ChapterListSelector          string                 `json:"chapterListSelector"`
	ChapterTitleSelector         string                 `json:"chapterTitleSelector"`
	BookInfoExtractScript        string                 `json:"bookInfoExtractScript"`
	NewBookExtractScript         string                 `json:"newBookExtractScript"`
	ConcurrencyLimit             int                    `json:"concurrencyLimit"`
	UseNetworkInterception       bool                   `json:"useNetworkInterception"`
	UseScreenshotMode            bool                   `json:"useScreenshotMode"`
	Cookies                      []Cookie               `json:"cookies"`
	LocalStorage                 map[string]interface{} `json:"localStorage"`
	SessionStorage               map[string]interface{} `json:"sessionStorage"`
	ReloadAfterStorageInjection  bool                   `json:"reloadAfterStorageInjection"`
	EnableAdaptiveTimeouts       bool                   `json:"enableAdaptiveTimeouts"`
	TimeoutMultipliers           TimeoutMultipliers     `json:"timeoutMultipliers"`
	UseFlareSolverr              bool                   `json:"useFlareSolverr"`
	ProxyURL                     string                 `json:"proxyUrl"`
	BlacklistTerms               []string               `json:"blacklistTerms"`
	WhitelistTerms               []string               `json:"whitelistTerms"`
	Headers                      map[string]string      `json:"headers"`
}

type Cookie struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Domain   string `json:"domain"`
	Path     string `json:"path"`
	HttpOnly bool   `json:"httpOnly"`
	Secure   bool   `json:"secure"`
	SameSite string `json:"sameSite"`
}

type TimeoutMultipliers struct {
	Small  float64 `json:"small"`
	Medium float64 `json:"medium"`
	Large  float64 `json:"large"`
	Huge   float64 `json:"huge"`
}



type UploadTarget struct {
	Bucket     string `json:"bucket"`
	PathPrefix string `json:"pathPrefix"`
}

type ScrapingChapterRequest struct {
	JobID                   string       `json:"jobId"`
	ChapterID               string       `json:"chapterId"`
	BookID                  string       `json:"bookId"`
	TargetURL               string       `json:"targetUrl"`
	WebsiteID               string         `json:"websiteId"`
	WebsiteConfig           *WebsiteConfig `json:"websiteConfig,omitempty"`
	ChapterSpecificSelector string         `json:"chapterSpecificSelector,omitempty"`
	UploadTarget            UploadTarget   `json:"uploadTarget"`
}

type ScrapedImage struct {
	OriginalURL string `json:"originalUrl"`
	Path        string `json:"path"`
}

type ScrapingChapterCompleted struct {
	JobID        string         `json:"jobId"`
	ChapterID    string         `json:"chapterId"`
	ScrapedTitle string         `json:"scrapedTitle"`
	TotalImages  int            `json:"totalImages"`
	Images       []ScrapedImage `json:"images"`
}

type ScrapingChapterPagesExtracted struct {
	JobID        string         `json:"jobId"`
	ChapterID    string         `json:"chapterId"`
	ScrapedTitle string         `json:"scrapedTitle"`
	TotalImages  int            `json:"totalImages"`
	Images       []ScrapedImage `json:"images"`
}

type ScrapingChapterFailed struct {
	JobID     string `json:"jobId"`
	ChapterID string `json:"chapterId"`
	Error     string `json:"error"`
	Message   string `json:"message"`
}

type ImageProcessingRequested struct {
	RawPath      string `json:"rawPath"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
	IsBackfill   bool   `json:"isBackfill"`
}

type ScrapingTestRequest struct {
	TargetURL       string `json:"targetUrl"`
	Script          string `json:"script"`
	UseFlareSolverr bool   `json:"useFlareSolverr"`
}

type ScrapingUpdateBookRequest struct {
	JobID                 string `json:"jobId"`
	BookID                string `json:"bookId"`
	TargetURL             string `json:"targetUrl"`
	WebsiteID             string         `json:"websiteId"`
	WebsiteConfig         *WebsiteConfig `json:"websiteConfig,omitempty"`
	BookInfoExtractScript string         `json:"bookInfoExtractScript"`
	Script                string         `json:"script,omitempty"` // Fallback for some producers
}

type ScrapingNewBookRequest struct {
	JobID                string `json:"jobId"`
	TargetURL            string `json:"targetUrl"`
	WebsiteID            string         `json:"websiteId"`
	WebsiteConfig        *WebsiteConfig `json:"websiteConfig,omitempty"`
	NewBookExtractScript string         `json:"newBookExtractScript"`
	Script               string         `json:"script,omitempty"` // Fallback for some producers
}

type ChapterInfo struct {
	Title string  `json:"title"`
	URL   string  `json:"url"`
	Index float64 `json:"index"`
}

type CoverInfo struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

type ScrapingBookCompleted struct {
	JobID       string        `json:"jobId"`
	BookID      string        `json:"bookId,omitempty"`
	TargetURL   string        `json:"targetUrl"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	Authors     []string      `json:"authors"`
	Tags        []string      `json:"tags"`
	Chapters    []ChapterInfo `json:"chapters"`
	Covers      []CoverInfo   `json:"covers"`
}

type ScrapingCoversRequest struct {
	JobID        string      `json:"jobId"`
	BookID       string      `json:"bookId"`
	TargetURL    string      `json:"urlOrigin"` // Mapped from urlOrigin
	WebsiteID    string         `json:"websiteId"`
	WebsiteConfig *WebsiteConfig `json:"websiteConfig,omitempty"`
	UploadTarget UploadTarget   `json:"uploadTarget"`
	Covers       []CoverInfo    `json:"images"` // Mapped from images
}

type ScrapingCoversCompleted struct {
	JobID   string   `json:"jobId"`
	BookID  string   `json:"bookId"`
	Results []string `json:"results"` // S3 Paths (raw)
}

type ScrapingImagesRequest struct {
	JobID        string       `json:"jobId"`
	EntityID     string       `json:"entityId"`
	WebsiteID    string         `json:"websiteId"`
	WebsiteConfig *WebsiteConfig `json:"websiteConfig,omitempty"`
	UploadTarget UploadTarget   `json:"uploadTarget"`
	ImageURLs    []string       `json:"imageUrls"`
}

type ScrapingImagesCompleted struct {
	JobID    string            `json:"jobId"`
	EntityID string            `json:"entityId"`
	Source   string            `json:"source"`
	Format   string            `json:"format"`
	URLMap   map[string]string `json:"urlMap"`
}

// DeadLetterMessage is published to the DLQ topic when a message cannot be
// deserialized or routed. It preserves the original payload for debugging.
type DeadLetterMessage struct {
	OriginalTopic string `json:"originalTopic"`
	Payload       string `json:"payload"`
	Error         string `json:"error"`
}

// ScrapingBookFailed is published when a new-book or update-book scrape job fails,
// allowing upstream services to react (e.g. retry or mark the job as failed).
type ScrapingBookFailed struct {
	JobID   string `json:"jobId"`
	BookID  string `json:"bookId,omitempty"`
	Error   string `json:"error"`
	Message string `json:"message"`
}
