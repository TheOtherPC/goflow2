package timescaledb

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/netsampler/goflow2/v2/transport"
	"log"
	"time"
)

type TimeScaleDBDriver struct {
	username string
	password string
	url      string
	port     string
	dbname   string

	conn *pgx.Conn

	errors chan error
}

func (d *TimeScaleDBDriver) Prepare() error {
	flag.StringVar(&d.username, "transport.timescaledb.username", "", "TimeScaleDB Username")
	flag.StringVar(&d.password, "transport.timescaledb.password", "", "TimeScaleDB Password")
	flag.StringVar(&d.url, "transport.timescaledb.url", "", "TimeScaleDB Host URL")
	flag.StringVar(&d.port, "transport.timescaledb.port", "", "TimeScaleDB Port")
	flag.StringVar(&d.dbname, "transport.timescaledb.dbname", "", "TimeScaleDB DB Name")
	return nil
}

func (d *TimeScaleDBDriver) Init() error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", d.username, d.password, d.url, d.port, d.dbname)
	ctx := context.Background()
	var err error
	d.conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("Unable to connect to TimeScaleDB: %v", err)
		return err
	}
	return nil
}

func (d *TimeScaleDBDriver) Send(key, data []byte) error {
	queryInsertMetaData := `INSERT INTO flow_raw (time_received, sequence_num, sampler_address, time_flow_start, 
												  	time_flow_end, bytes, packets, src_addr, dst_addr, src_net, etype, 
												  	proto, src_port, dst_port, in_if, out_if, ip_tos, forwarding_status, 
													tcp_flags, dst_net) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
													$11, $12, $13, $14, $15, $16, $17, $18, $19, $20);`
	log.Print(data)
	var timescaleData map[string]interface{}
	if err := json.Unmarshal(data, &timescaleData); err != nil {
		log.Fatal(err)
	}
	log.Print(timescaleData)
	time.Unix(0, int64(timescaleData["time_received"].(float64)))
	ctx := context.Background()
	_, err := d.conn.Exec(ctx, queryInsertMetaData, time.Unix(0, int64(timescaleData["time_received_ns"].(float64))), timescaleData["sequence_num"],
		timescaleData["sampler_address"], time.Unix(0, int64(timescaleData["time_flow_start_ns"].(float64))),
		time.Unix(0, int64(timescaleData["time_flow_start_ns"].(float64))),
		timescaleData["bytes"], timescaleData["packets"], timescaleData["src_addr"], timescaleData["dst_addr"],
		timescaleData["src_net"], timescaleData["etype"], timescaleData["proto"], timescaleData["src_port"],
		timescaleData["dst_port"], timescaleData["in_if"], timescaleData["out_if"], timescaleData["ip_tos"],
		timescaleData["forwarding_status"], timescaleData["tcp_flags"], timescaleData["dst_net"])
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
func (d *TimeScaleDBDriver) Errors() <-chan error { return d.errors }

func (d *TimeScaleDBDriver) Close() error {
	ctx := context.Background()
	defer func(conn *pgx.Conn, ctx context.Context) {
		err := conn.Close(ctx)
		if err != nil {
			log.Fatalf("Unable to close connection: %v", err)
		}
	}(d.conn, ctx)
	return nil
}

func init() {
	d := &TimeScaleDBDriver{
		errors: make(chan error),
	}
	transport.RegisterTransportDriver("timescaledb", d)
}
