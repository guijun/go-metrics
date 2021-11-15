package metrics
//TODO 参照 协议 https://github.com/cyberdelia/go-metrics-graphite/blob/39f87cc3b432bbb898d7c643c0e93cac2bc865ad/graphite.go
import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	// UDPstatsdMaxLen is the maximum size of a packet
	// to send to statsd
	UDPstatsdMaxLen = 1400
)

// GraphitedSink provides a MetricSink that can be used
// with a graphite  metrics server. It uses
// only UDP packets
type GraphitedSink struct {
	addr        string
	metricQueue chan string
}

// NewStatsdSinkFromURL creates an GraphitedSink from a URL. It is used
// (and tested) from NewMetricSinkFromURL.
func NewGraphiteSinkFromURL(u *url.URL) (MetricSink, error) {
	return NewGraphiteSink(u.Host)
}

// NewStatsdSink is used to create a new GraphitedSink
func NewGraphiteSink(addr string) (*GraphitedSink, error) {
	s := &GraphitedSink{
		addr:        addr,
		metricQueue: make(chan string, 4096),
	}
	go s.flushMetrics()
	return s, nil
}

// Close is used to stop flushing to statsd
func (s *GraphitedSink) Shutdown() {
	close(s.metricQueue)
}

func (s *GraphitedSink) SetGauge(key []string, val float32) {
	now := time.Now().Unix()
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s.value %f %d\n", flatKey, val, now))
}

func (s *GraphitedSink) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	now := time.Now().Unix()
	flatKey := s.flattenKeyLabels(key, labels)
	//	s.pushMetric(fmt.Sprintf("%s:%f|g\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.value %f %d\n", flatKey, val, now))
}

func (s *GraphitedSink) EmitKey(key []string, val float32) {
	now := time.Now().Unix()
	flatKey := s.flattenKey(key)
	//s.pushMetric(fmt.Sprintf("%s:%f|kv\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.value %f %d\n", flatKey, val, now))
}

func (s *GraphitedSink) IncrCounter(key []string, val float32) {
	now := time.Now().Unix()
	flatKey := s.flattenKey(key)
	//s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.count %d %d\n", flatKey, int(val), now))
}

func (s *GraphitedSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	now := time.Now().Unix()
	flatKey := s.flattenKeyLabels(key, labels)
	//s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.count %d %d\n", flatKey, int(val), now))
}

func (s *GraphitedSink) AddSample(key []string, val float32) {
	now := time.Now().Unix()
	flatKey := s.flattenKey(key)
//	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.sample %d %d\n", flatKey, int(val), now))
}

func (s *GraphitedSink) AddSampleWithLabels(key []string, val float32, labels []Label) {
	now := time.Now().Unix()
	flatKey := s.flattenKeyLabels(key, labels)
//	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
	s.pushMetric(fmt.Sprintf("%s.sample %d %d\n", flatKey, int(val), now))
}

// Flattens the key for formatting, removes spaces
func (s *GraphitedSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, ".")
	return strings.Map(func(r rune) rune {
		switch r {
		case ':':
			fallthrough
		case ' ':
			return '_'
		default:
			return r
		}
	}, joined)
}

// Flattens the key along with labels for formatting, removes spaces
func (s *GraphitedSink) flattenKeyLabels(parts []string, labels []Label) string {
	for _, label := range labels {
		parts = append(parts, label.Value)
	}
	return s.flattenKey(parts)
}

// Does a non-blocking push to the metrics queue
func (s *GraphitedSink) pushMetric(m string) {
	select {
	case s.metricQueue <- m:
	default:
	}
}

// Flushes metrics
func (s *GraphitedSink) flushMetrics() {
	var sock net.Conn
	var err error
	var wait <-chan time.Time
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

CONNECT:
	// Create a buffer
	buf := bytes.NewBuffer(nil)

	// Attempt to connect
	sock, err = net.Dial("udp", s.addr)
	if err != nil {
		log.Printf("[ERR] Error connecting to Graphlite! Err: %s", err)
		goto WAIT
	}

	for {
		select {
		case metric, ok := <-s.metricQueue:
			// Get a metric from the queue
			if !ok {
				goto QUIT
			}

			// Check if this would overflow the packet size
			if len(metric)+buf.Len() > UDPstatsdMaxLen {
				_, err := sock.Write(buf.Bytes())
				buf.Reset()
				if err != nil {
					log.Printf("[ERR] Error writing to Graphlite! Err: %s", err)
					goto WAIT
				}
			}

			// Append to the buffer
			buf.WriteString(metric)

		case <-ticker.C:
			if buf.Len() == 0 {
				continue
			}

			_, err := sock.Write(buf.Bytes())
			buf.Reset()
			if err != nil {
				log.Printf("[ERR] Error flushing to Graphlite! Err: %s", err)
				goto WAIT
			}
		}
	}

WAIT:
	// Wait for a while
	wait = time.After(time.Duration(5) * time.Second)
	for {
		select {
		// Dequeue the messages to avoid backlog
		case _, ok := <-s.metricQueue:
			if !ok {
				goto QUIT
			}
		case <-wait:
			goto CONNECT
		}
	}
QUIT:
	s.metricQueue = nil
}
