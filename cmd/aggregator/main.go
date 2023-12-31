package main

import (
	"cloud-logging-agg/model"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
)

func main() {
	csvFilePath := os.Getenv("CSV_FILE_PATH")
	f, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)

	if _, err = csvReader.Read(); err != nil {
		panic(err)
	}

	allRows := model.Rows{}
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		timestamp, err := time.Parse(time.RFC3339Nano, rec[20])
		if err != nil {
			panic(err)
		}

		reqLatency, err := durationString(rec[0]).ToDuration()
		if err != nil {
			panic(err)
		}

		allRows = append(allRows, model.Row{
			InsertID:          rec[8],
			ReceivedTimestamp: timestamp,
			RequestLatency:    reqLatency,
		})
	}

	color.Green("All Rows Loaded! rows count: %d", len(allRows))

	uniqRows := allRows.UniqByInsertID()

	color.Green("Uniq Rows Loaded! rows count: %d", len(uniqRows))

	rampUpMin, err := strconv.Atoi(os.Getenv("RAMPUP_MIN"))
	if err != nil {
		panic(err)
	}

	rampDownMin, err := strconv.Atoi(os.Getenv("RAMPDOWN_MIN"))
	if err != nil {
		panic(err)
	}

	targetRows := uniqRows.FilterRampingRows(rampUpMin, rampDownMin)

	color.Green("Target Rows Loaded! rows count: %d", len(targetRows))

	avgReqLatencyMs := targetRows.AvgReqLatencyMs()

	color.Green("Avg Req Latency: %dms", avgReqLatencyMs)

	color.Green("99th Percentile Req Latency: %dms", targetRows.PercentileNReqLatency(99).Milliseconds())
	color.Green("90th Percentile Req Latency: %dms", targetRows.PercentileNReqLatency(90).Milliseconds())
	color.Green("50th Percentile Req Latency: %dms", targetRows.PercentileNReqLatency(50).Milliseconds())
}

// durationString の例: 0.054366s
type durationString string

func (d durationString) ToDuration() (time.Duration, error) {
	floatPart, err := strconv.ParseFloat(
		strings.TrimSuffix(string(d), "s"),
		64,
	)
	if err != nil {
		return 0, err
	}

	return time.Duration(floatPart * float64(time.Second)), nil
}
