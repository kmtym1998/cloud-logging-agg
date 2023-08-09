package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/fatih/color"
)

type Row struct {
	InsertID          string
	ReceivedTimestamp time.Time
	RequestLatency    time.Duration
}

type Rows []Row

func main() {
	csvFilePath := os.Getenv("CSV_FILE_PATH")
	f, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}

	csvReader := csv.NewReader(f)

	if _, err = csvReader.Read(); err != nil {
		panic(err)
	}

	allRows := Rows{}
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

		allRows = append(allRows, Row{
			InsertID:          rec[8],
			ReceivedTimestamp: timestamp,
			RequestLatency:    reqLatency,
		})
	}

	color.Green("All Rows Loaded! rows count: %d", len(allRows))

	uniqRows := allRows.UniqByInsertID()

	color.Green("Uniq Rows Loaded! rows count: %d", len(uniqRows))

	avgReqLatencyMs := uniqRows.AvgReqLatencyMs()

	color.Green("Avg Req Latency: %d ms", avgReqLatencyMs)
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

// InsertID で重複を削除した Rows を返す
// なんかもうちょいいい書き方ありそう
func (r Rows) UniqByInsertID() Rows {
	insertIDs := lo.Map(r, func(row Row, _ int) string {
		return row.InsertID
	})

	uniqInsertIDs := lo.Uniq(insertIDs)

	return lo.Map(uniqInsertIDs, func(insertID string, _ int) Row {
		row, found := lo.Find(r, func(row Row) bool {
			return row.InsertID == insertID
		})
		if !found {
			panic("not found")
		}

		return row
	})
}

func (r Rows) AvgReqLatencyMs() int {
	sum := 0
	for _, row := range r {
		sum += int(row.RequestLatency.Milliseconds())
	}

	if len(r) > 0 {
		return sum / len(r)
	}

	return 0
}

func (r Rows) Percentile50ReqLatencyMs() int {
	if len(r) == 0 {
		return 0
	}

	if len(r)%2 == 0 {
		return int(r[len(r)/2].RequestLatency.Milliseconds())
	} else {
		return int(r[len(r)/2+1].RequestLatency.Milliseconds())
	}
}
