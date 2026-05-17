package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/gatuno/scraper/internal/config"
	"github.com/gatuno/scraper/internal/kafka"
)

func main() {
	cfg := config.LoadConfig()
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaWriteTimeout, cfg.KafkaRequiredAcks)
	defer producer.Close()

	// Raw JSON from user
	rawJSON := `{
		"jobId": "019e2d35-377b-762a-90a1-6d82d6e2d63a",
		"bookId": "acd78f9e-ce8a-4107-98fe-d2228c518cf8",
		"targetUrl": "https://hentaistube.com/hentai-manga/deluxe-mc-gakuen",
		"websiteConfig": {
			"name": "hentaistube.com",
			"cloudflareBypass": false,
			"preScript": "setTimeout(()=>{let e=document.querySelector(\"#btnConfirmarIdade\");e?.click()},3e3);",
			"posScript": null,
			"useNetworkInterception": true,
			"useScreenshotMode": false,
			"cookies": null,
			"localStorage": null,
			"sessionStorage": null,
			"reloadAfterStorageInjection": false,
			"enableAdaptiveTimeouts": true,
			"timeoutMultipliers": null,
			"proxyUrl": null,
			"blacklistTerms": ["logo","icon","avatar","base64","placeholder","assets","sprite",".gif","aviso-mangas"],
			"whitelistTerms": null,
			"selectors": {
				"chapterListSelector": null,
				"bookInfoExtractScript": "(async()=>{let t={covers:[],chapters:[]},e=document.querySelector(\"#capaAnime img\");if(e){let r=e.getAttribute(\"src\")||e.getAttribute(\"data-src\");r&&t.covers.push({url:r.startsWith(\"http\")?r:new URL(r,window.location.href).href,title:\"Capa\"})}let l=document.querySelectorAll(\".backgroundpost .thumbnail\");return l.forEach((e,r)=>{let l=e.querySelector(\"a\"),i=e.querySelector(\"p\"),a=l?l.innerText.trim():null,n=l?l.getAttribute(\"href\")||l.href:null,c=i?i.innerText.trim().match(/(\\d+(?:\\.\\d+)?)/):null,h=c?parseFloat(c[0]):r+1;n&&(obj={url:n.startsWith(\"http\")?n:new URL(n,window.location.href).href,index:h,isFinal:!1},a&&(obj.title=a),t.chapters.push(obj))}),console.log(t),t})();"
			},
			"headers": {
				"Referer": "hentaistube.com"
			}
		},
		"uploadTarget": {
			"bucket": "processing",
			"pathPrefix": "f8/acd78f9e-ce8a-4107-98fe-d2228c518cf8"
		}
	}`

	var message map[string]interface{}
	if err := json.Unmarshal([]byte(rawJSON), &message); err != nil {
		log.Fatalf("Failed to unmarshal raw JSON: %v", err)
	}

	topic := cfg.TopicUpdateBookRequested
	if len(os.Args) > 1 {
		topic = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("Publishing message to topic: %s", topic)
	if err := producer.Publish(ctx, topic, message); err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	log.Println("Message published successfully!")
}
