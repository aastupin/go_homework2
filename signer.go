package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

var chanMd5Quota chan struct{}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	chanIn := make(chan interface{}, 1)
	for _, inJob := range jobs {
		wg.Add(1)
		chanOut := make(chan interface{}, 1)
		go startWorker(inJob, chanIn, chanOut, wg)
		chanIn = chanOut
		runtime.Gosched()
	}
	wg.Wait()
	return
}

func startWorker(j job, chIn, chOut chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(chOut)
	j(chIn, chOut)

}
func main() {
	inputData := []int{0, 1}
	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
	}
	start := time.Now()
	ExecutePipeline(hashSignJobs...)
	end := time.Since(start)
	fmt.Println("Time:", end)
	return
}

func getCrc32InParallel(value string) chan string {
	result := make(chan string)
	go func(out chan<- string) {
		crc32value := DataSignerCrc32(value)
		out <- crc32value
	}(result)
	return result
}

func SingleHash(in, out chan interface{}) {
	chanMD5Queue := make(chan struct{}, 1)
	wg := &sync.WaitGroup{}
	for val := range in {
		wg.Add(1)
		go func(outCh chan interface{}, value int, chanMD5Queue chan struct{}, wg *sync.WaitGroup) {
			defer wg.Done()
			fmt.Println("start SingleHash with value ", value)
			chanMD5Queue <- struct{}{}
			md5 := DataSignerMd5(strconv.Itoa(value))
			<-chanMD5Queue
			res1Ch := getCrc32InParallel(strconv.Itoa(value))
			res2Ch := getCrc32InParallel(md5)
			outValue := <-res1Ch + "~" + <-res2Ch
			outCh <- outValue
		}(out, val.(int), chanMD5Queue, wg)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for val := range in {
		wg.Add(1)
		go func(outCh chan interface{}, value string, wg *sync.WaitGroup) {
			defer wg.Done()
			fmt.Println("start MultiHash with value ", value)
			var result string
			var resChannels [6]chan string
			var resHashes [6]string

			for i := 0; i < 6; i++ {
				resChannels[i] = getCrc32InParallel(strconv.Itoa(i) + value)
			}
			for i := 0; i < 6; i++ {
				resHashes[i] = <-resChannels[i]
			}
			for i := 0; i < 6; i++ {
				result += resHashes[i]
			}
			outCh <- result
		}(out, val.(string), wg)
	}
	wg.Wait()
}
func CombineResults(in, out chan interface{}) {
	var data []string
	var result string
	fmt.Println("start CombineResults ")
	for val := range in {
		data = append(data, val.(string))
	}
	sort.Strings(data)

	for _, slValue := range data {
		if result == "" {
			result = slValue
		} else {
			result = result + "_" + slValue
		}
	}
	fmt.Println(result)
	out <- result
}
