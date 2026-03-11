package durex

// WorkflowStatus represents the current state of a workflow execution.
type WorkflowStatus string

const (
	WorkflowRunning   WorkflowStatus = "running"
	WorkflowCompleted WorkflowStatus = "completed"
	WorkflowFailed    WorkflowStatus = "failed"
)

// StepStatus represents the current state of a step execution.
type StepStatus string

const (
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
)
