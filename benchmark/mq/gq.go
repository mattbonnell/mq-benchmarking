package mq

import (
	"context"
	"database/sql"
	"log"

	"github.com/mattbonnell/gq"
	"github.com/mattbonnell/mq-benchmarking/benchmark"

	_ "github.com/go-sql-driver/mysql"
)

const (
	databaseDriver = "mysql"
	databaseDSN    = "root:password@tcp(localhost:3306)/gq"
)

type Gq struct {
	handler benchmark.MessageHandler
	c       *gq.Client
	p       *gq.Producer
	cancel  context.CancelFunc
	ctx     context.Context
}

func NewGq(numberOfMessages int, testLatency bool) *Gq {
	db, err := sql.Open(databaseDriver, databaseDSN)
	if err != nil {
		log.Fatalf("gq: failed to open DB: %s", err)
	}

	c, err := gq.NewClient(db, databaseDriver)
	if err != nil {
		log.Fatalf("gq: failed to create client: %s", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Gq{
		handler: handler,
		c:       c,
		cancel:  cancel,
		ctx:     ctx,
	}
}

func (g *Gq) Setup() {
	_, err := g.c.NewConsumer(g.ctx, func(message []byte) error {
		g.handler.ReceiveMessage(message)
		return nil
	}, nil)
	if err != nil {
		log.Fatalf("gq: failed to start consumer: %s", err)
	}
	p, err := g.c.NewProducer(g.ctx, nil)
	if err != nil {
		log.Fatalf("gq: failed to start producer: %s", err)
	}
	g.p = p
}

func (g *Gq) Teardown() {
	g.cancel()
}

func (g *Gq) Send(message []byte) {
	g.p.Push(message)
}

func (g *Gq) MessageHandler() *benchmark.MessageHandler {
	return &g.handler
}
