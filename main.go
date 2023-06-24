package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

type BulkDocuments struct {
	Index   string
	Records []map[string]string
}

func bulkEmailZincPost(amountEmails int, bulk BulkDocuments, wg *sync.WaitGroup, waitChan *chan int) {
	defer func() {
		<-*waitChan
		wg.Done()
	}()
	jsonStr, errJson := json.Marshal(bulk)
	if errJson != nil {
		fmt.Println("WASM :: ERROR: ", errJson)
	}
	bodyStr := string(jsonStr)
	emailReqBody := []byte(bodyStr)
	zincReq, _ := http.NewRequest(http.MethodPost, "http://localhost:4080/api/_bulkv2", bytes.NewBuffer(emailReqBody))
	zincReq.Close = true
	zincReq.Header.Add("Content-Type", "application/json")
	zincReq.SetBasicAuth("admin", "Complexpass#123")

	client := &http.Client{}
	resp, err := client.Do(zincReq)
	if err != nil {
		fmt.Println("WASM :: ERROR: ", err)
	}
	defer resp.Body.Close()
	b, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		fmt.Println(err)
	}
	fmt.Println("WASM :: ", amountEmails, "- RESPONSE: ", string(b))

}

func getEmailMap(fileContent string) map[string]string {
	possibleKeys := map[string]bool{
		"Body":                      true,
		"Content-Transfer-Encoding": true,
		"Content-Type":              true,
		"Date":                      true,
		"From":                      true,
		"Message-ID":                true,
		"Mime-Version":              true,
		"Subject":                   true,
		"To":                        true,
		"X-FileName":                true,
		"X-Folder":                  true,
		"X-From":                    true,
		"X-Origin":                  true,
		"X-To":                      true,
		"X-bcc":                     true,
		"X-cc":                      true,
		"Bcc":                       true,
		"Cc":                        true,
	}
	dataMap := make(map[string]string)
	var lastKey string
	lines := strings.Split(fileContent, "\n")
	for indexLines, line := range lines {
		line := strings.TrimSpace(line)
		if len(line) != 0 {
			colonIndex := strings.Index(line, ":")
			if colonIndex != -1 {
				key := strings.TrimSpace(line[:colonIndex])
				if possibleKeys[key] {
					value := strings.TrimSpace(line[colonIndex+1:])
					dataMap[key] = value
					lastKey = key
				}
			} else {
				dataMap[lastKey] += line
			}
		} else {
			key := "Body"
			value := strings.TrimSpace(strings.Join(lines[indexLines:], "\n"))
			dataMap[key] = value
			break
		}
	}
	return dataMap
}

func processFile(file []byte) {
	bufTgz := bytes.NewBuffer(file)
	gzipReader, _ := gzip.NewReader(bufTgz)
	defer gzipReader.Close()
	tr := tar.NewReader(gzipReader)
	var wg sync.WaitGroup
	var bulkBody BulkDocuments
	bulkBody.Index = "email"
	filesCounter := 0
	i := 0
	const AMOUNT_EMAILS_BULK = 10000
	const MAX_GO_ROUTINES = 3
	waitChan := make(chan int, MAX_GO_ROUTINES)
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("WASM :: ERROR READING FILE: ", err)
		}
		bufStr := new(strings.Builder)
		io.Copy(bufStr, tr)
		fileContent := bufStr.String()

		if i == AMOUNT_EMAILS_BULK {
			wg.Add(1)
			waitChan <- 1
			fmt.Println("WASM :: ", filesCounter, "- REQUESTED")
			go bulkEmailZincPost(filesCounter, bulkBody, &wg, &waitChan)
			bulkBody.Records = []map[string]string{}
			i = 0
		}

		if strings.HasPrefix(fileContent, "Message-ID:") {
			emailMap := getEmailMap(fileContent)
			bulkBody.Records = append(bulkBody.Records, emailMap)
			i++
			filesCounter++
		}
	}
	if i < AMOUNT_EMAILS_BULK {
		wg.Add(1)
		waitChan <- 1
		fmt.Println("WASM :: ", filesCounter, "- REQUESTED")
		go bulkEmailZincPost(filesCounter, bulkBody, &wg, &waitChan)
	}
	wg.Wait()
}

func main() {
	data, err := os.ReadFile("/home/gabodotcodes/test-go/enron_mail_20110402.tgz")
	if err != nil {
		fmt.Println("WASM :: ERROR: ", err)
	}
	processFile(data)
	fmt.Println("UPLOAD FINISH")
}