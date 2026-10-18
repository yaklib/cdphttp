// Sets a cookie through the browser, then reads it back.
package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"codeberg.org/yaklib/cdphttp"
)

func main() {
	c, err := cdphttp.NewClient(context.Background(), cdphttp.WithEndpoint("http://localhost:9229"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.CloseIdleConnections() // closes the tabs it opened

	get := func(url string) {
		resp, err := c.Get(url)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("GET %s -> %s\n%s\n", url, resp.Status, b)
	}

	get("https://httpbin.org/cookies/set?cdphttp=xxx")
	get("https://httpbin.org/cookies")
}
