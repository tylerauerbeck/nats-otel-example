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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	errTimeout     = errors.New("timeout waiting for event")
	consumerTracer = otel.Tracer("consume")
	kickflipTracer = otel.Tracer("kickflip")
)

const serviceName = "nats-example"

func main() {
	ctx := context.Background()
	{
		tp, err := initTracer(ctx, serviceName)
		if err != nil {
			panic(err)
		}
		defer tp.Shutdown(ctx)
	}

	publish(ctx)

	consume(ctx)
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

func consume(ctx context.Context) {
	// ctx := context.Background()
	// initTracer()

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

	// ctx, span := otel.GetTracerProvider().Tracer("consume").Start(ctx, "consume")
	// ctx, span := consumerTracer.Start(context.Background(), "consume")
	ctx, span := consumerTracer.Start(ctx, "consume")

	defer span.End()

	fmt.Println(string(receivedMsg.Payload))

	doAKickflip(ctx)
}

func initTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint("147.75.55.123:4317"))
	if err != nil {
		return nil, err
	}

	// labels/tags/resources that are common to all traces.
	resource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		attribute.String("some-attribute", "some-value"),
	)

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
		// set the sampling rate based on the parent span to 60%
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.6))),
	)

	otel.SetTracerProvider(provider)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, // W3C Trace Context format; https://www.w3.org/TR/trace-context/
		),
	)

	return provider, nil
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
	_, span := kickflipTracer.Start(ctx, "kickflip")
	defer span.End()

	fmt.Println("Kickflipped!")

	time.Sleep(5 * time.Second)
}
