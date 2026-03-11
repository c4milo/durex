package durex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// WorkflowClaim represents a distributed lock on a workflow execution.
// Claims use two layers of protection following Kleppmann's fencing model:
//
//  1. The claim object itself prevents concurrent execution (lease).
//  2. The fence token is recorded in each step result, allowing the storage
//     layer to reject writes from stale holders via write-once semantics.
type WorkflowClaim struct {
	Owner         string        `json:"owner"`
	FenceToken    int64         `json:"fence_token"`
	ClaimedAt     time.Time     `json:"claimed_at"`
	LeaseDuration time.Duration `json:"lease_duration"`
}

func (c *WorkflowClaim) isExpired() bool {
	return time.Now().UTC().After(c.ClaimedAt.Add(c.LeaseDuration))
}

func (e *Executor) claimKey(workflowID string) string {
	return path.Join(e.opts.Prefix, "workflows", workflowID, "claim.json")
}

// acquireClaim attempts to acquire a distributed lock for the given workflow.
// Returns the fence token on success.
//
// The algorithm:
//  1. Try atomic create via PutIfAbsent (S3 If-None-Match). If no other holder
//     exists, we win immediately.
//  2. If the claim exists, read it. If the lease is still valid, return
//     ErrWorkflowAlreadyRunning.
//  3. If the lease is expired, take over by overwriting with a bumped fence
//     token. A race is possible here (two processes detect expiry simultaneously),
//     but write-once step results (PutIfAbsent on every step) provide the
//     correctness backstop: even if two holders proceed, only one can persist
//     each step result.
func (e *Executor) acquireClaim(ctx context.Context, workflowID string) (int64, error) {
	ctx, span := tracer.Start(ctx, "durex.workflow.acquire_claim",
		trace.WithAttributes(
			attribute.String("durex.workflow.id", workflowID),
		),
	)
	defer span.End()

	claim := &WorkflowClaim{
		Owner:         e.ownerID,
		FenceToken:    1,
		ClaimedAt:     time.Now().UTC(),
		LeaseDuration: e.opts.LeaseDuration,
	}

	data, err := json.Marshal(claim)
	if err != nil {
		return 0, err
	}

	// Fast path: atomic create — only one executor can succeed.
	err = e.storage.PutIfAbsent(ctx, e.claimKey(workflowID), data)
	if err == nil {
		span.SetAttributes(
			attribute.String("durex.claim.action", "created"),
			attribute.Int64("durex.claim.fence_token", claim.FenceToken),
		)
		return claim.FenceToken, nil
	}
	if !errors.Is(err, ErrAlreadyExists) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("acquiring workflow claim: %w", err)
	}

	// Claim exists. Read it and check the lease.
	existingData, err := e.storage.Get(ctx, e.claimKey(workflowID))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("reading existing claim: %w", err)
	}

	var existing WorkflowClaim
	if err := json.Unmarshal(existingData, &existing); err != nil {
		return 0, fmt.Errorf("parsing existing claim: %w", err)
	}

	if !existing.isExpired() {
		err := fmt.Errorf("%w: held by %s since %s",
			ErrWorkflowAlreadyRunning, existing.Owner, existing.ClaimedAt.Format(time.RFC3339))
		span.SetAttributes(
			attribute.String("durex.claim.action", "rejected"),
			attribute.String("durex.claim.holder", existing.Owner),
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Lease expired. Take over with a bumped fence token.
	claim.FenceToken = existing.FenceToken + 1
	data, err = json.Marshal(claim)
	if err != nil {
		return 0, err
	}

	if err := e.storage.Put(ctx, e.claimKey(workflowID), data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("taking over expired claim: %w", err)
	}

	span.SetAttributes(
		attribute.String("durex.claim.action", "takeover"),
		attribute.Int64("durex.claim.fence_token", claim.FenceToken),
		attribute.String("durex.claim.previous_owner", existing.Owner),
	)

	return claim.FenceToken, nil
}

// releaseClaim removes the workflow claim, allowing future executions.
func (e *Executor) releaseClaim(ctx context.Context, workflowID string) {
	_ = e.storage.Delete(ctx, e.claimKey(workflowID))
}
