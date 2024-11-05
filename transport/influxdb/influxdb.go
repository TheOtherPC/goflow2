package influxdb

import (
	"context"
	"encoding/json"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/netsampler/goflow2/v2/transport"
	"log"
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
	// ... (code for setting up URL, token, org, and bucket remains the same)
	return nil
}

func (d *InfluxDBDriver) Init() error {
	// ... (code for initializing the InfluxDB client remains the same)
	return nil
}

func (d *InfluxDBDriver) Errors() <-chan error {
	return d.errors
}

func (d *InfluxDBDriver) Send(key, data []byte) error {
	var influxData map[string]interface{}
	err := json.Unmarshal(data, &influxData)
	if err != nil {
		return err
	}

	writeAPI := d.client.WriteAPIBlocking(d.org, d.bucket)
	point := write.NewPoint("netflow_v5", nil, influxData, time.Now())
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
