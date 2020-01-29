package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello kali. How are you?\n")
	})

	http.ListenAndServe(":8070", nil)

}
