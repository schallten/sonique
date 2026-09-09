package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/maxence-charriere/go-app/v11/pkg/app"

	"sonique-better-frontend/ui"
)

func main() {
	app.Route("/", func() app.Composer { return &ui.App{} })
	app.Route("/client", func() app.Composer { return &ui.Client{} })
	app.Route("/dashboard", func() app.Composer { return &ui.Dashboard{} })

	app.RunWhenOnBrowser()

	backend, _ := url.Parse("http://127.0.0.1:8000")
	proxy := httputil.NewSingleHostReverseProxy(backend)

	mux := http.NewServeMux()

	mux.Handle("/match", proxy)
	mux.Handle("/load", proxy)
	mux.Handle("/dashboard", proxy)

	mux.Handle("/web/styles.css", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css")
		http.ServeFile(w, r, "web/styles.css")
	}))
	mux.Handle("/web/", http.FileServer(http.Dir(".")))

	mux.Handle("/", &app.Handler{
		Name:        "Sonique",
		ShortName:   "Sonique",
		Title:       "Sonique - Your Tune",
		Description: "Music recognition app",
		Styles:      []string{"/web/styles.css"},
	})

	log.Println("Server running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
