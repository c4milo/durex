package durex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	return e.opts.Prefix + "workflows/" + workflowID + "/claim.json"
}

// acquireClaim attempts to acquire a distributed lock for the given workflow.
// Returns the fence token on success.
//
// The algorithm:
//  1. Try atomic create via PutIfAbsent (S3 If-None-Match). If no other holder
//     exists, we win immediately.
//  2. If the claim exists, read it with its ETag. If the lease is still valid,
//     return ErrWorkflowAlreadyRunning.
//  3. If the lease is expired, take over via CAS (S3 If-Match) with a bumped
//     fence token. The conditional write ensures only one executor can succeed
//     when multiple detect the same expiry — losers get ErrPreconditionFailed
//     and retry from step 1.
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

	// Claim exists. Read it with ETag for CAS takeover.
	existingData, etag, err := e.storage.GetWithETag(ctx, e.claimKey(workflowID))
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

	// Lease expired. CAS takeover with bumped fence token.
	// If another executor took over between our read and this write,
	// PutIfMatch fails with ErrPreconditionFailed and we surface it
	// so the caller can retry.
	claim.FenceToken = existing.FenceToken + 1
	data, err = json.Marshal(claim)
	if err != nil {
		return 0, err
	}

	if err := e.storage.PutIfMatch(ctx, e.claimKey(workflowID), data, etag); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			// Another executor won the CAS race. Return a retryable error.
			err = fmt.Errorf("%w: claim was modified during takeover attempt",
				ErrWorkflowAlreadyRunning)
		}
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

// renewClaim extends the lease on an existing claim owned by this executor.
// Uses CAS (PutIfMatch) to avoid overwriting another executor's claim in case
// of a concurrent takeover between the read and write.
// Returns false if the claim no longer belongs to us or the CAS fails.
func (e *Executor) renewClaim(ctx context.Context, workflowID string) bool {
	data, etag, err := e.storage.GetWithETag(ctx, e.claimKey(workflowID))
	if err != nil {
		return false
	}

	var existing WorkflowClaim
	if err := json.Unmarshal(data, &existing); err != nil {
		return false
	}

	if existing.Owner != e.ownerID {
		return false
	}

	existing.ClaimedAt = time.Now().UTC()
	renewed, err := json.Marshal(existing)
	if err != nil {
		return false
	}

	return e.storage.PutIfMatch(ctx, e.claimKey(workflowID), renewed, etag) == nil
}

// renewLeaseLoop periodically renews the claim lease until ctx is cancelled.
// It renews at half the lease duration to ensure the claim doesn't expire
// while the workflow is still running.
func (e *Executor) renewLeaseLoop(ctx context.Context, workflowID string) {
	interval := e.opts.LeaseDuration / 2
	if interval < time.Millisecond {
		return // lease too short for renewal (e.g., tests with 1ns lease)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !e.renewClaim(ctx, workflowID) {
				return // lost ownership
			}
		}
	}
}

// releaseClaim removes the workflow claim, allowing future executions.
// It only deletes the claim if this executor still owns it, to avoid
// removing another executor's claim after a lease takeover.
func (e *Executor) releaseClaim(ctx context.Context, workflowID string) {
	data, err := e.storage.Get(ctx, e.claimKey(workflowID))
	if err != nil {
		return // claim already gone or unreadable — nothing to release
	}

	var claim WorkflowClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		return
	}

	if claim.Owner != e.ownerID {
		return // another executor took over — do not delete their claim
	}

	_ = e.storage.Delete(ctx, e.claimKey(workflowID))
}
