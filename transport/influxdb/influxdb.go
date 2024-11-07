package influxdb

import (
	"context"
	"encoding/json"
	"flag"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/netsampler/goflow2/v2/transport"
	"log"
	"net/url"
	"os"
	"time"
)

type InfluxDBDriver struct {
	url    string
	token  string
	org    string
	bucket string
	client influxdb2.Client

	errors chan error
}

func (d *InfluxDBDriver) Prepare() error {
	flag.StringVar(&d.url, "transport.influxdb.url", "http://localhost:8086", "InfluxDB URL")
	log.Print(d.url)
	flag.StringVar(&d.token, "transport.influxdb.token", os.Getenv("INFLUXDB_TOKEN"), "InfluxDB token")
	log.Print(d.token)
	flag.StringVar(&d.org, "transport.influxdb.org", "hi", "InfluxDB organization")
	log.Print(d.org)
	flag.StringVar(&d.bucket, "transport.influxdb.bucket", "hi", "InfluxDB bucket")
	log.Print(d.bucket)
	return nil
}

func (d *InfluxDBDriver) Init() error {
	_, err := url.Parse(d.url)
	if err != nil {
		log.Printf("Invalid InfluxDB URL: %v", err)
		return err
	}
	d.client = influxdb2.NewClient(d.url, d.token)
	log.Print(d.client)
	log.Print(d.org)
	log.Print(d.bucket)
	log.Print(d.token)
	log.Print(d.url)

	return nil
}

func (d *InfluxDBDriver) Errors() <-chan error {
	return d.errors
}

func (d *InfluxDBDriver) Send(key, data []byte) error {
	var influxData map[string]interface{}
	if err := json.Unmarshal(data, &influxData); err != nil {
		log.Fatal(err)
	}
	writeAPI := d.client.WriteAPIBlocking(d.org, d.bucket)
	point := write.NewPoint("netflow", nil, influxData, time.Now())
	if err := writeAPI.WritePoint(context.Background(), point); err != nil {
		log.Fatal(err)
	}
	return nil
}
func (d *InfluxDBDriver) Close() error {
	if d.client != nil {
		d.client.Close()
	}
	return nil
}

func init() {
	d := &InfluxDBDriver{
		errors: make(chan error),
	}
	transport.RegisterTransportDriver("influxdb", d)
}
