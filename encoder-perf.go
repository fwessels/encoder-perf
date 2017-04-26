package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	blake2b "github.com/minio/blake2b-simd"
)

var dataShards = flag.Int("data", 4, "Number of shards to split the data into, must be below 257.")
var parShards = flag.Int("par", 2, "Number of parity shards")
var outDir = flag.String("out", "", "Alternative output directory")
var workers = flag.Int("w", 1, "Number of workers to run in parallel.")
var runs = flag.Int("r", 1000, "Total number of runs.")
var nodisk = flag.Bool("nodisk", false, "Disable writes to disk.")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

func Reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func main() {
	// Parse command line parameters.
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	if *dataShards > 257 {
		fmt.Fprintf(os.Stderr, "Error: Too many data shards\n")
		os.Exit(1)
	}
	fname := args[0]

	fmt.Println("Number of worker routines: ", *workers)

	filesPerRout := *runs / *workers

	start := time.Now()

	var wg sync.WaitGroup

	for g := 0; g < *workers; g++ {

		wg.Add(1)

		go func(goroutine int) {
			defer wg.Done()

			for f := 0; f < filesPerRout; f++ {
				erasureCodeFile /*FullParallel*/ (fname, fmt.Sprintf("output-%d-%d", goroutine, f))
			}
		}(g)
	}

	wg.Wait()

	totalObjs := (*workers) * filesPerRout
	fmt.Println("Total objects:", totalObjs)
	elapsed := time.Since(start)
	fmt.Println("Elapsed time :", elapsed)
	seconds := float64(elapsed) / float64(time.Second)
	fmt.Printf("Speed        : %4.0f objs/sec\n", float64(totalObjs)/seconds)
}

func getHash(data []byte) string {

	h := blake2b.New512()
	h.Write(data[:])
	return fmt.Sprintf("%x", h.Sum(nil))
}

const json1 = `{"version":"1.0.0","format":"xl","stat":{"size":525968,"modTime":"2017-04-25T01:09:39.173066169Z"},"erasure":{"algorithm":"klauspost/reedsolomon/vandermonde","data":18,"parity":6,"blockSize":10485760,"index":9,"distribution":[7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,1,2,3,4,5,6],"checksum":[{"name":"part.1","algorithm":"blake2b","hash":"`
const json2 = `"}]},"minio":{"release":"DEVELOPMENT.GOGET"},"meta":{"md5Sum":"956ac5e7286265b5da68ff33c05f6b35"},"parts":[{"number":1,"name":"part.1","etag":"","size":525968}]}`

func erasureCodeFile(inputfile, outputfile string) {

	// Create encoding matrix.
	enc, err := reedsolomon.New(*dataShards, *parShards)
	checkErr(err)

	b, err := ioutil.ReadFile(inputfile)
	checkErr(err)

	// Split the file into equally sized shards.
	shards, err := enc.Split(b)
	checkErr(err)

	// Encode parity
	err = enc.Encode(shards)
	checkErr(err)

	// Write out the resulting files.
	dir, _ /*file*/ := filepath.Split(outputfile)
	if *outDir != "" {
		dir = *outDir
	}

	tstr := Reverse(strings.ToLower(fmt.Sprintf("%X", time.Now().UnixNano())))
	dirfmt := "/mnt/%s/disk%d/" + tstr[:2] + "/" + tstr[2:]
	disk := []string{"sde1", "sdf1", "sdg1", "sdh1", "sdi1", "sdj1", "sdk1", "sdl1"}

	for i, shard := range shards {
		//outfn := fmt.Sprintf("%s.%d", file, i)

		dir = fmt.Sprintf(dirfmt, disk[(i)%len(disk)], i+1)
		if !*nodisk {
			os.MkdirAll(dir, os.ModePerm)
		}

		hash := getHash(shard)

		if !*nodisk {
			err = ioutil.WriteFile(filepath.Join(dir, "part.1" /*outfn*/), shard, os.ModePerm)
			checkErr(err)
		}

		if !*nodisk {
			err = ioutil.WriteFile(filepath.Join(dir, "xl.json"), []byte(json1+hash+json2), os.ModePerm)
			checkErr(err)
		}
	}

	/*      for i, shard := range shards {
			outfn := fmt.Sprintf("%s.%d", file, i)

			err = ioutil.WriteFile(filepath.Join(dir, outfn), shard, os.ModePerm)
			checkErr(err)
		}
	*/
}

func erasureCodeFileFullParallel(inputfile, outputfile string) {

	// Create encoding matrix.
	enc, err := reedsolomon.New(*dataShards, *parShards)
	checkErr(err)

	b, err := ioutil.ReadFile(inputfile)
	checkErr(err)

	// Split the file into equally sized shards.
	shards, err := enc.Split(b)
	checkErr(err)

	// Encode parity
	err = enc.Encode(shards)
	checkErr(err)

	// Write out the resulting files.
	dir, file := filepath.Split(outputfile)
	if *outDir != "" {
		dir = *outDir
	}

	var wg sync.WaitGroup

	for i, shard := range shards {
		wg.Add(1)

		go func(i int, shard []byte) {
			defer wg.Done()

			outfn := fmt.Sprintf("%s.%d", file, i)

			err = ioutil.WriteFile(filepath.Join(dir, outfn), shard, os.ModePerm)
			checkErr(err)
		}(i, shard)
	}

	wg.Wait()
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}
