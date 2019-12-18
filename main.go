package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	// "github.com/davecgh/go-spew/spew"
	"github.com/parnurzeal/gorequest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

type BurrowLagRet struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Request struct {
		Host string `json:"host"`
		URL  string `json:"url"`
	} `json:"request"`
	Status struct {
		Cluster  string `json:"cluster"`
		Complete int64  `json:"complete"`
		Group    string `json:"group"`
		Maxlag   struct {
			Complete   int64 `json:"complete"`
			CurrentLag int64 `json:"current_lag"`
			End        struct {
				Lag       int64 `json:"lag"`
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
			} `json:"end"`
			Owner     string `json:"owner"`
			Partition int64  `json:"partition"`
			Start     struct {
				Lag       int64 `json:"lag"`
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
			} `json:"start"`
			Status string `json:"status"`
			Topic  string `json:"topic"`
		} `json:"maxlag"`
		PartitionCount int64 `json:"partition_count"`
		Partitions     []struct {
			Complete   int64 `json:"complete"`
			CurrentLag int64 `json:"current_lag"`
			End        struct {
				Lag       int64 `json:"lag"`
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
			} `json:"end"`
			Owner     string `json:"owner"`
			Partition int64  `json:"partition"`
			Start     struct {
				Lag       int64 `json:"lag"`
				Offset    int64 `json:"offset"`
				Timestamp int64 `json:"timestamp"`
			} `json:"start"`
			Status string `json:"status"`
			Topic  string `json:"topic"`
		} `json:"partitions"`
		Status   string `json:"status"`
		Totallag int64  `json:"totallag"`
	} `json:"status"`
}

var intval = flag.Int("intval", 10, "curl burrow intval s")
var burrowAddr = flag.String("s", "127.0.0.1:8888", "burrow server addr")
var cluster = flag.String("cluster", "cluster", "cluster")
var consumerGroup = flag.String("group", "consumergroup", "consumer group")
var promPort = flag.Int("port", 8811, "prometheus export port")

var lag int

var lagDetail sync.Map

func getAllConsumers() []string {
	type consumerRet struct {
		Consumers []string `json:"consumers"`
		Error     bool     `json:"error"`
		Message   string   `json:"message"`
		Request   struct {
			Host string `json:"host"`
			URL  string `json:"url"`
		} `json:"request"`
	}
	resp, body, errs := gorequest.New().Get(fmt.Sprintf("http://%s/v3/kafka/%s/consumer", *burrowAddr, *cluster)).End()

	if resp.StatusCode != 200 {
		log.Debug().Msgf("get consumer not 200, code: %d", resp.StatusCode)
		return nil
	}
	if errs != nil {
		log.Debug().Msgf("errs: %#v", errs)
		return nil
	}

	ret := consumerRet{}

	if err := json.Unmarshal([]byte(body), &ret); err != nil {
		log.Debug().Msgf("please check input, response is: %s, err is %#v", body, err)
		return nil
	}

	return ret.Consumers
}

func main() {

	log.Logger = log.With().Caller().Logger()

	flag.Parse()

	col := burrowCollect{
		LagDetail: prometheus.NewDesc(
			"lag_details",
			"lag details",
			[]string{"consumer_group", "topic", "partition"},
			nil,
		),
		Lag: prometheus.NewDesc(
			fmt.Sprintf("lag_kafka_%s_%s", *cluster, *consumerGroup),
			"the lag of consumer group",
			nil,
			nil,
		),
	}
	prometheus.MustRegister(&col)

	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *promPort), server); err != nil {
			log.Debug().Msgf("listen promPort error", err.Error())
			os.Exit(-1)
		}
	}()

	log.Debug().Msg("burrow-exporter started")
	for {
		// consumers := getAllConsumers()

		consumers := []string{*consumerGroup}

		for _, cg := range consumers {
			processConsumerGroup(cg)
		}
		time.Sleep(time.Duration(*intval) * time.Second)
	}
}

func processConsumerGroup(cg string) {
	url := fmt.Sprintf("http://%s/v3/kafka/%s/consumer/%s/lag", *burrowAddr, *cluster, cg)
	resp, body, errs := gorequest.New().Get(url).End()
	if errs != nil {
		log.Debug().Msgf("errs is:%#v, url:%s ", errs, url)
		return
	}

	if resp.StatusCode != 200 {
		log.Debug().Msgf("not 200, statuscode is:%d", resp.StatusCode)
		return
	}

	ret := BurrowLagRet{}

	if err := json.Unmarshal([]byte(body), &ret); err != nil {
		log.Debug().Msgf("please check input, response is: %s, err: %#v", body, err)
		return
	}

	lag = int(ret.Status.Totallag)

	for _, p := range ret.Status.Partitions {
		k := fmt.Sprintf("%s::%s::%d", cg, p.Topic, p.Partition)
		lag, _ := lagDetail.Load(k)
		if lag == nil {
			lag = 0
		}
		lagDetail.Store(k, p.CurrentLag)
	}
}

type burrowCollect struct {
	LagDetail *prometheus.Desc
	Lag       *prometheus.Desc
}

func (c *burrowCollect) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.Lag
}

func (c *burrowCollect) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.Lag,
		prometheus.GaugeValue,
		float64(lag),
	)

	f := func(k, v interface{}) bool {
		strs := strings.Split(k.(string), "::")

		ch <- prometheus.MustNewConstMetric(
			c.LagDetail,
			prometheus.GaugeValue,
			float64(v.(int64)),
			strs...,
		)
		return true
	}
	lagDetail.Range(f)

}

type BurrowStatusRet struct {
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
