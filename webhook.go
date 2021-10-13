package main

import (
	"fmt"
	"log"
	"io/ioutil"
	"net/http"
	"github.com/google/go-github/github"
)

func handleWebhook(w http.ResponseWriter, r *http.Request) {
        payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("error reading request body: err=%s\n", err)
		return
	}
	defer r.Body.Close()

	event, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		log.Printf("could not parse webhook: err=%s\n", err)
		return
	}

	switch e := event.(type) {
	case *github.RepositoryEvent:
		if e.Action != nil && *e.Action == "created" {
			fmt.Printf("New repository has been created %s\n",
				 *e.Repo.FullName)

		} else if e.Action != nil && *e.Action == "deleted" {
			fmt.Printf("The repository has been deleted %s\n",
			        *e.Repo.FullName)
	        } else if e.Action != nil && *e.Action == "edited" && (e.Repo.Topics[0] == "golang-learning-test" || e.Repo.Topics[1] == "golang-learning-test")  {
			  fmt.Printf("The topic has been created %s\n",
		                e.Repo.Topics[0])
		}

	default:
		log.Printf("unknown event type %s\n", github.WebHookType(r))
		return
	}
}

func main() {
    log.Println("server started")
	http.HandleFunc("/webhook", handleWebhook)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
