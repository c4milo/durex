package durex_test

import (
	"context"
	"fmt"
	"log"

	"github.com/c4milo/durex"
)

// Domain types — plain Go structs, no durex dependency.

type Address struct {
	Street string `json:"street"`
	City   string `json:"city"`
	State  string `json:"state"`
	Zip    string `json:"zip"`
}

type PaymentInfo struct {
	CardToken string `json:"card_token"`
	Currency  string `json:"currency"`
}

type Order struct {
	ID       string      `json:"id"`
	Items    []string    `json:"items"`
	Amount   int         `json:"amount"` // cents
	Payment  PaymentInfo `json:"payment"`
	Shipping Address     `json:"shipping"`
	Weight   float64     `json:"weight"` // kg
}

type ChargeResult struct {
	ChargeID string `json:"charge_id"`
	Status   string `json:"status"`
}

type ShippingLabel struct {
	TrackingNumber string `json:"tracking_number"`
	Carrier        string `json:"carrier"`
}

type OrderResult struct {
	ChargeID       string `json:"charge_id"`
	TrackingNumber string `json:"tracking_number"`
}

// Business logic — plain Go functions with real parameters, completely
// decoupled from durex. They know nothing about durable execution.
//
// The ctx each function receives carries an OpenTelemetry span from durex.
// Any outgoing call (HTTP, gRPC, database) that uses this ctx will appear
// as a child span of the durex step in your trace backend (Jaeger, Tempo, etc.).
// No manual trace propagation needed — just pass ctx through.

func validateOrder(_ context.Context, order Order) (bool, error) {
	return len(order.Items) > 0 && order.Amount > 0, nil
}

func chargeCard(ctx context.Context, cardToken string, amount int) (ChargeResult, error) {
	// In production: stripe.Charge(ctx, ...) — the ctx carries the durex step
	// span, so the Stripe HTTP call appears as a child span automatically.
	_ = ctx
	return ChargeResult{
		ChargeID: "ch_" + cardToken[:3],
		Status:   fmt.Sprintf("charged_%d", amount),
	}, nil
}

func createShippingLabel(ctx context.Context, address Address, weight float64) (ShippingLabel, error) {
	// In production: shippingAPI.CreateLabel(ctx, ...) — same trace propagation.
	_ = ctx
	return ShippingLabel{
		TrackingNumber: "1Z_" + address.Zip,
		Carrier:        fmt.Sprintf("carrier_%.1fkg", weight),
	}, nil
}

// Workflow — thin orchestration layer that wires parameterised business logic
// into durable steps using closures. The closures capture the parameters from
// the enclosing scope so that each step function satisfies
// func(context.Context) (T, error).

func processOrder(wf *durex.Workflow, order Order) (OrderResult, error) {
	// Step 1: validate the order (struct parameter).
	valid, err := durex.Step(wf, "validate", func(ctx context.Context) (bool, error) {
		return validateOrder(ctx, order)
	})
	if err != nil {
		return OrderResult{}, err
	}
	if !valid {
		return OrderResult{}, fmt.Errorf("order validation failed")
	}

	// Step 2: charge the card (primitive parameters: string, int).
	charge, err := durex.Step(wf, "charge", func(ctx context.Context) (ChargeResult, error) {
		return chargeCard(ctx, order.Payment.CardToken, order.Amount)
	})
	if err != nil {
		return OrderResult{}, err
	}

	// Step 3: create a shipping label (struct + float64 parameters).
	label, err := durex.Step(wf, "ship", func(ctx context.Context) (ShippingLabel, error) {
		return createShippingLabel(ctx, order.Shipping, order.Weight)
	})
	if err != nil {
		return OrderResult{}, err
	}

	return OrderResult{
		ChargeID:       charge.ChargeID,
		TrackingNumber: label.TrackingNumber,
	}, nil
}

func Example() {
	storage := durex.NewMemoryStorage()
	executor := durex.New(storage, durex.Options{})
	ctx := context.Background()

	order := Order{
		ID:    "order-abc",
		Items: []string{"widget", "gadget"},
		Amount: 4999,
		Payment: PaymentInfo{
			CardToken: "tok_visa",
			Currency:  "usd",
		},
		Shipping: Address{
			Street: "123 Main St",
			City:   "Springfield",
			State:  "IL",
			Zip:    "62704",
		},
		Weight: 2.5,
	}

	result, err := durex.RunWorkflow(ctx, executor, "order-abc", func(wf *durex.Workflow) (OrderResult, error) {
		return processOrder(wf, order)
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("charge=%s tracking=%s\n", result.ChargeID, result.TrackingNumber)
	// Output: charge=ch_tok tracking=1Z_62704
}
