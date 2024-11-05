package timescaledb

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/netsampler/goflow2/v2/transport"
	"log"
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

func (d *TimeScaleDBDriver) Send(key, data []byte) error { return nil }
func (d *TimeScaleDBDriver) Errors() <-chan error        { return d.errors }

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
