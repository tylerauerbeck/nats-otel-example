package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.infratographer.com/x/events"
	"go.infratographer.com/x/gidx"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

var (
	errTimeout     = errors.New("timeout waiting for event")
	consumerTracer = otel.Tracer("consume")
	kickflipTracer = otel.Tracer("kickflip")
)

func init() {
	initTracer()
}

func main() {

	ctx := context.Background()

	publish(ctx)

	fmt.Println("Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	fmt.Println("Done")

	consume()

}

func publish(ctx context.Context) {

	pc := events.PublisherConfig{
		URL:        "nats://127.0.0.1:4222",
		Timeout:    0,
		Prefix:     "com.infratographer",
		NATSConfig: events.NATSConfig{},
	}

	msg := events.ChangeMessage{
		SubjectID:            "load-balancer",
		EventType:            "create",
		AdditionalSubjectIDs: []gidx.PrefixedID{},
		ActorID:              "",
		Source:               "api",
		Timestamp:            time.Time{},
		// TraceID:              span.SpanContext().TraceID().String(),
		// SpanID:               span.SpanContext().SpanID().String(),
		SubjectFields:  map[string]string{},
		FieldChanges:   []events.FieldChange{},
		AdditionalData: map[string]interface{}{},
	}

	p, err := events.NewPublisher(pc)
	if err != nil {
		log.Fatal(err)
	}
	err = p.PublishChange(ctx, "load-balancer", msg)
	if err != nil {
		log.Fatal(err)
	}

}

func consume() {
	ctx := context.Background()

	sc := events.SubscriberConfig{
		URL:        "nats://127.0.0.1:4222",
		Timeout:    0,
		Prefix:     "com.infratographer",
		NATSConfig: events.NATSConfig{},
	}

	s, err := events.NewSubscriber(sc)
	if err != nil {
		log.Fatal(err)
	}

	c, err := s.SubscribeChanges(context.TODO(), "*.load-balancer")
	if err != nil {
		log.Fatal(err)
	}

	receivedMsg, err := getSingleMessage(c, time.Minute*2)
	if err != nil {
		log.Fatal(err)
	}

	msg, err := events.UnmarshalChangeMessage(receivedMsg.Payload)
	if err != nil {
		log.Fatal(err)
	}

	ctx = events.TraceContextFromChangeMessage(ctx, msg)

	fmt.Println(ctx)

	_, span := consumerTracer.Start(ctx, "consume")
	// _, span := tracer.Start(context.Background(), "consume")
	defer span.End()

	fmt.Println(string(receivedMsg.Payload))

	doAKickflip(ctx)
}

func initTracer() {

	ctx := context.Background()

	client := otlptracegrpc.NewClient()

	otlpTraceExporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	batchSpanProcessor := trace.NewBatchSpanProcessor(otlpTraceExporter)

	tracerProvider := trace.NewTracerProvider(
		trace.WithSpanProcessor(batchSpanProcessor),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
}

func getSingleMessage(messages <-chan *message.Message, timeout time.Duration) (*message.Message, error) {
	select {
	case message := <-messages:
		return message, nil
	case <-time.After(timeout):
		return nil, errTimeout
	}
}

func doAKickflip(ctx context.Context) {
	_, span := kickflipTracer.Start(context.Background(), "kickflip")
	defer span.End()

	fmt.Println("Kickflipped!")

	time.Sleep(5 * time.Second)
}
