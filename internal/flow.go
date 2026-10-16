package internal

import "encoding/json"

// Flow represent one independent group of chained API calls.
// input.json is decoded as []Flow
type Flow struct {
	Name     string         `json:"name"`
	Vars     map[string]any `json:"vars"`
	Defaults Defaults       `json:"defaults"`
	Steps    []Step         `json:"steps"`
}

// Defaults are applied to every step of the flow,
// unless the step override them
type Defaults struct {
	TimeoutMs  int               `json:"timeout_ms"`
	VerifySSL  bool              `json:"verify_ssl"`
	Headers    map[string]string `json:"headers"`
	Expect     *Expect           `json:"expect"`
	Retry      *Retry            `json:"retry"`
	Pagination *Pagination       `json:"pagination"`
	Save       *bool             `json:"save"`
	Parallel   int               `json:"parallel"`
}

// Expect represent the success criteria of a call
type Expect struct {
	Status []int `json:"status"`
}

// Retry represent the retry policy when expect is not met
type Retry struct {
	Count         int      `json:"count"`
	DelayMs       int      `json:"delay_ms"`
	Backoff       string   `json:"backoff"`
	RetryOn       []int    `json:"retry_on"`
	RetryOnErrors []string `json:"retry_on_errors"`
}

// Pagination represent how to page through a list API
type Pagination struct {
	Type        string   `json:"type"`
	OffsetParam string   `json:"offset_param"`
	LimitParam  string   `json:"limit_param"`
	StopWhen    []string `json:"stop_when"`
}

// Step represent a single API call of the flow.
// nil pointer fields fall back to Flow.Defaults
type Step struct {
	ID         string               `json:"id"`
	DependsOn  []string             `json:"depends_on"`
	Foreach    string               `json:"foreach"`
	Request    Request              `json:"request"`
	Pagination *Pagination          `json:"pagination"`
	TimeoutMs  *int                 `json:"timeout_ms"`
	VerifySSL  *bool                `json:"verify_ssl"`
	Expect     *Expect              `json:"expect"`
	Retry      *Retry               `json:"retry"`
	Save       *bool                `json:"save"`
	Parallel   *int                 `json:"parallel"`
	Response   *Response            `json:"response"`
	OnStatus   map[int]StatusAction `json:"on_status"`
}

// Request represent the HTTP request of a step
type Request struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Params  map[string]any    `json:"params"`
	Data    map[string]any    `json:"data"`
}

// Response declare which values to keep from the API reply
type Response struct {
	Type   string           `json:"type"`
	From   string           `json:"from"`
	Fields map[string]Paths `json:"fields"`
}

// StatusAction represent the action on a specific status code
type StatusAction struct {
	Rerun string `json:"rerun"`
	Max   int    `json:"max"`
}

// Paths accept a single path or a list of paths,
// in case of list the first matched path is used
type Paths []string

func (p *Paths) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*p = Paths{one}
		return nil
	}
	return json.Unmarshal(b, (*[]string)(p))
}

// Record represent one item of a step response
type Record map[string]string

// Results hold the records of every executed step by step id
type Results map[string][]Record

const (
	ResponseObject = "object"
	ResponseList   = "list"

	PaginationOffset = "offset"
	PaginationNone   = "none"

	BackoffFixed       = "fixed"
	BackoffExponential = "exponential"
)
