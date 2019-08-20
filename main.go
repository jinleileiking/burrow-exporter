package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/parnurzeal/gorequest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type BurrowRet struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Status  struct {
		Cluster        string        `json:"cluster"`
		Group          string        `json:"group"`
		Status         string        `json:"status"`
		Complete       int           `json:"complete"`
		Partitions     []interface{} `json:"partitions"`
		PartitionCount int           `json:"partition_count"`
		Maxlag         struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Owner     string `json:"owner"`
			Status    string `json:"status"`
			Start     struct {
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
			} `json:"start"`
			End struct {
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
			} `json:"end"`
			CurrentLag int `json:"current_lag"`
			Complete   int `json:"complete"`
		} `json:"maxlag"`
		Totallag int `json:"totallag"`
	} `json:"status"`
	Request struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	} `json:"request"`
}

var intval = flag.Int("intval", 10, "curl burrow intval s")
var burrowAddr = flag.String("s", "127.0.0.1:8888", "burrow server addr")
var topic = flag.String("topic", "topic", "topic")
var consumerGroup = flag.String("group", "consumergroup", "consumer group")
var promPort = flag.Int("port", 8811, "prometheus export port")

var Lag int

func main() {
	flag.Parse()

	col := burrowCollect{
		Lag: prometheus.NewDesc(
			fmt.Sprintf("lag_kafka_%s_%s", *topic, *consumerGroup),
			"the lag of consumer group",
			nil,
			nil,
		),
	}
	prometheus.MustRegister(&col)

	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *promPort), server)

	for {
		resp, body, errs := gorequest.New().Get(fmt.Sprintf("http://%s/v3/kafka/%s/consumer/%s/status", *burrowAddr, *topic, *consumerGroup)).End()

		if resp.StatusCode != 200 {
			panic("not 200")
		}
		if errs != nil {
			panic(errs)
		}

		ret := BurrowRet{}

		if err := json.Unmarshal([]byte(body), &ret); err != nil {
			panic(err)
		}

		Lag = ret.Status.Totallag
		time.Sleep(time.Duration(*intval) * time.Second)
	}
}

type burrowCollect struct {
	Lag *prometheus.Desc
}

func (c *burrowCollect) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.Lag
}

func (c *burrowCollect) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.Lag,
		prometheus.GaugeValue,
		float64(Lag),
	)
}
