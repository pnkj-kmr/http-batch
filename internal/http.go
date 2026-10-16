package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// maxPages protect from endless pagination
const maxPages = 10000

// StepResult represent the outcome of a single step,
// records and files are left out when the step has save false
type StepResult struct {
	ID          string   `json:"id"`
	Ok          bool     `json:"ok"`
	Skipped     bool     `json:"skipped,omitempty"`
	Calls       int      `json:"calls"`
	Err         string   `json:"error,omitempty"`
	RecordCount int      `json:"record_count"`
	Records     []Record `json:"records,omitempty"`
	Files       []string `json:"files,omitempty"`
}

// FlowResult represent the outcome of a flow
type FlowResult struct {
	Flow     string        `json:"flow"`
	Ok       bool          `json:"ok"`
	Duration time.Duration `json:"duration"`
	Steps    []StepResult  `json:"steps"`
}

// Options of a flow execution
type Options struct {
	// Timeout [secs] is used when neither the step nor the defaults has timeout_ms
	Timeout int
	// OutDir receive every response page under <flow>/<step>/,
	// unless the step has save false. Empty to write nothing
	OutDir string
	// Workers is the number of foreach items called in parallel,
	// unless the step or the defaults has parallel
	Workers int
}

// Call execute every step of the flow in order and make the remote api calls
func Call(f Flow, opt Options) (out FlowResult, err error) {
	st := time.Now()
	out.Flow = f.Name

	r, err := newRunner(f, opt)
	if err != nil {
		return out, err
	}

	done := map[string]bool{}
	for i := range f.Steps {
		s := &f.Steps[i]
		res := StepResult{ID: s.ID}
		if dep := failedDependency(s.DependsOn, done); dep != "" {
			res.Skipped = true
			res.Err = fmt.Sprintf("dependency %s not succeeded", dep)
		} else {
			r.runStep(s, &res)
		}

		done[s.ID] = res.Ok
		out.Steps = append(out.Steps, res)
		if !res.Ok && err == nil {
			err = fmt.Errorf("flow %s: step %s: %s", f.Name, s.ID, res.Err)
		}
	}

	out.Ok = err == nil
	out.Duration = time.Since(st)
	return out, err
}

type runner struct {
	flow    Flow
	timeout time.Duration
	outDir  string
	workers int
	steps   map[string]*Step

	// mu guard results, generation and clients,
	// foreach items read them in parallel
	mu         sync.RWMutex
	results    Results
	generation map[string]int
	clients    map[bool]*http.Client

	// refreshMu allow a single on_status rerun at a time
	refreshMu sync.Mutex
}

// loopItem is the current item of a foreach step
type loopItem struct {
	index  int
	source string
	value  any
}

// settings are the effective step settings after merging the defaults
type settings struct {
	timeout    time.Duration
	verifySSL  bool
	headers    map[string]string
	expect     *Expect
	retry      *Retry
	pagination *Pagination
	save       bool
	parallel   int
}

func newRunner(f Flow, opt Options) (*runner, error) {
	r := &runner{
		flow:       f,
		timeout:    time.Duration(opt.Timeout) * time.Second,
		outDir:     opt.OutDir,
		workers:    opt.Workers,
		steps:      map[string]*Step{},
		results:    Results{},
		generation: map[string]int{},
		clients:    map[bool]*http.Client{},
	}

	for i := range f.Steps {
		s := &f.Steps[i]
		if s.ID == "" {
			return nil, fmt.Errorf("flow %s: step %d has no id", f.Name, i+1)
		}
		if _, ok := r.steps[s.ID]; ok {
			return nil, fmt.Errorf("flow %s: duplicate step id %s", f.Name, s.ID)
		}
		for _, d := range s.DependsOn {
			if _, ok := r.steps[d]; !ok {
				return nil, fmt.Errorf("flow %s: step %s depends on unknown or later step %s", f.Name, s.ID, d)
			}
		}
		for code, act := range s.OnStatus {
			if act.Rerun == "" {
				continue
			}
			target, ok := r.steps[act.Rerun]
			if !ok {
				return nil, fmt.Errorf("flow %s: step %s on_status %d rerun unknown or later step %s", f.Name, s.ID, code, act.Rerun)
			}
			// a rerun step can't rerun again, it would wait on itself
			if len(target.OnStatus) > 0 {
				return nil, fmt.Errorf("flow %s: step %s on_status %d rerun step %s which has on_status itself", f.Name, s.ID, code, act.Rerun)
			}
		}
		if s.Response != nil && s.Response.Type != ResponseObject && s.Response.Type != ResponseList {
			return nil, fmt.Errorf("flow %s: step %s response type must be %q or %q", f.Name, s.ID, ResponseObject, ResponseList)
		}
		if err := r.checkForeach(s); err != nil {
			return nil, fmt.Errorf("flow %s: step %s: %w", f.Name, s.ID, err)
		}
		r.steps[s.ID] = s
	}
	return r, nil
}

func (r *runner) checkForeach(s *Step) error {
	if s.Foreach == "" {
		return nil
	}
	if name, ok := strings.CutPrefix(s.Foreach, "vars."); ok {
		if _, isList := r.flow.Vars[name].([]any); !isList {
			return fmt.Errorf("foreach %s is not a list in vars", s.Foreach)
		}
		return nil
	}
	src, ok := r.steps[s.Foreach]
	if !ok || !contains(s.DependsOn, s.Foreach) {
		return fmt.Errorf("foreach %s must be an earlier step listed in depends_on", s.Foreach)
	}
	if src.Response == nil || src.Response.Type != ResponseList {
		return fmt.Errorf("foreach %s must have response type %q", s.Foreach, ResponseList)
	}
	return nil
}

// itemResult is the outcome of one foreach item
type itemResult struct {
	recs  []Record
	files []string
	calls int
	err   error
}

// runStep execute the step once, or once per item in case of foreach.
// Items are independent, so they are called in parallel (settings.parallel)
// and merged back in the item order
func (r *runner) runStep(s *Step, res *StepResult) {
	items, err := r.loopItems(s)
	if err != nil {
		res.Err = err.Error()
		return
	}

	cfg := r.settings(s)
	out := make([]itemResult, len(items))
	sem := make(chan struct{}, cfg.parallel)
	var wg sync.WaitGroup
	for i, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, it *loopItem) {
			defer func() { wg.Done(); <-sem }()
			o := &out[i]
			o.recs, o.files, o.calls, o.err = r.execute(s, cfg, it)
		}(i, it)
	}
	wg.Wait()

	var recs []Record
	var errs []string
	for _, o := range out {
		res.Calls += o.calls
		recs = append(recs, o.recs...)
		res.Files = append(res.Files, o.files...)
		if o.err != nil {
			errs = append(errs, o.err.Error())
		}
	}

	// records are always kept for the next steps,
	// but only reported when the step is saved (eg. token is not)
	r.mu.Lock()
	r.results[s.ID] = recs
	r.generation[s.ID]++
	r.mu.Unlock()

	res.RecordCount = len(recs)
	if cfg.save {
		res.Records = recs
	}
	res.Ok = len(errs) == 0
	res.Err = strings.Join(errs, "; ")
}

// refresh rerun the step (eg. token_generate) once for all the parallel items,
// seen is the generation of the step when the failed request was built.
// If another item already refreshed it in the meantime, nothing is called
func (r *runner) refresh(id string, seen int) (calls int, err error) {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()

	if r.stepGeneration(id) != seen {
		return 0, nil
	}
	var res StepResult
	if r.runStep(r.steps[id], &res); !res.Ok {
		return res.Calls, fmt.Errorf("rerun %s: %s", id, res.Err)
	}
	return res.Calls, nil
}

func (r *runner) stepGeneration(id string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.generation[id]
}

func (r *runner) stepResults(id string) ([]Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	recs, ok := r.results[id]
	return recs, ok
}

func (r *runner) loopItems(s *Step) ([]*loopItem, error) {
	if s.Foreach == "" {
		return []*loopItem{nil}, nil
	}

	var items []*loopItem
	if name, ok := strings.CutPrefix(s.Foreach, "vars."); ok {
		for i, v := range r.flow.Vars[name].([]any) {
			items = append(items, &loopItem{i + 1, s.Foreach, toString(v)})
		}
		return items, nil
	}

	recs, _ := r.stepResults(s.Foreach)
	for i, rec := range recs {
		items = append(items, &loopItem{i + 1, s.Foreach, rec})
	}
	if len(items) == 0 {
		log.Printf("[%s] %s: foreach %s has no items, nothing to call", r.flow.Name, s.ID, s.Foreach)
	}
	return items, nil
}

// execute make the call of one item, following the pagination
func (r *runner) execute(s *Step, cfg settings, it *loopItem) (recs []Record, files []string, calls int, err error) {
	pg := cfg.pagination

	var offset, limit int
	paged := pg != nil && pg.Type == PaginationOffset
	if paged {
		offset = intValue(s.Request.Params[pg.OffsetParam])
		limit = intValue(s.Request.Params[pg.LimitParam])
		paged = limit > 0
	}

	var last [sha256.Size]byte
	for page := 0; page < maxPages; page++ {
		var extra map[string]any
		if paged {
			extra = map[string]any{pg.OffsetParam: offset, pg.LimitParam: limit}
		}

		body, n, err := r.send(s, cfg, it, extra)
		calls += n
		if err != nil {
			return recs, files, calls, err
		}

		sum := sha256.Sum256(body)
		if paged && page > 0 && stopOn(pg, "repeated_page") && sum == last {
			return recs, files, calls, nil
		}
		last = sum

		if cfg.save && r.outDir != "" {
			file, err := r.writePage(s, it, page+1, body)
			if err != nil {
				return recs, files, calls, err
			}
			files = append(files, file)
		}

		if s.Response == nil && !paged {
			return recs, files, calls, nil
		}
		data, err := decodeBody(body)
		if err != nil {
			return recs, files, calls, fmt.Errorf("invalid json response: %w", err)
		}

		if s.Response != nil {
			got, err := extract(data, s.Response)
			if err != nil {
				return recs, files, calls, err
			}
			recs = append(recs, got...)
		}

		if !paged {
			return recs, files, calls, nil
		}
		from := "$"
		if s.Response != nil {
			from = s.Response.From
		}
		items, isList := lookup(data, from).([]any)
		if stopOn(pg, "empty_or_not_list") && (!isList || len(items) == 0) {
			return recs, files, calls, nil
		}
		if stopOn(pg, "short_page") && len(items) < limit {
			return recs, files, calls, nil
		}
		offset += limit
	}
	return recs, files, calls, fmt.Errorf("pagination exceeded %d pages", maxPages)
}

// writePage write one response page to outDir/<flow>/<step>/<item>_p<page>.json
// and return the path relative to outDir
func (r *runner) writePage(s *Step, it *loopItem, page int, body []byte) (string, error) {
	name := fmt.Sprintf("%04d.json", page)
	if it != nil {
		name = fmt.Sprintf("%04d_%04d_%s.json", page, it.index, fileSafe(itemLabel(it.value)))
	}

	rel := fmt.Sprintf("%s_%s_%s", fileSafe(r.flow.Name), fileSafe(s.ID), name)
	path := filepath.Join(r.outDir, rel)

	// rel := filepath.Join(fileSafe(r.flow.Name), fileSafe(s.ID), name)
	// path := filepath.Join(r.outDir, rel)
	// if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
	// 	return "", fmt.Errorf("output dir: %w", err)
	// }

	if err := os.WriteFile(path, body, 0644); err != nil {
		return "", fmt.Errorf("output file: %w", err)
	}
	return rel, nil
}

// send make the http call with retry and on_status handling,
// the request is rebuilt on every attempt to pick up refreshed values (eg. token)
func (r *runner) send(s *Step, cfg settings, it *loopItem, extra map[string]any) (body []byte, calls int, err error) {
	reruns := map[int]int{}
	for attempt := 0; ; {
		// generation of the rerun steps the request is built with
		seen := map[string]int{}
		for _, act := range s.OnStatus {
			if act.Rerun != "" {
				seen[act.Rerun] = r.stepGeneration(act.Rerun)
			}
		}

		req, err := r.buildRequest(s, cfg, it, extra)
		if err != nil {
			return nil, calls, err
		}

		calls++
		st := time.Now()
		status, body, err := r.do(req, cfg)
		log.Printf("[%s] %s: %s %s -> %d (%s)", r.flow.Name, s.ID, req.Method, req.URL.Redacted(), status, time.Since(st).Round(time.Millisecond))

		if err == nil {
			if act, ok := s.OnStatus[status]; ok && act.Rerun != "" && reruns[status] < max(act.Max, 1) {
				reruns[status]++
				log.Printf("[%s] %s: status %d, rerun %s", r.flow.Name, s.ID, status, act.Rerun)
				n, err := r.refresh(act.Rerun, seen[act.Rerun])
				calls += n
				if err != nil {
					return nil, calls, err
				}
				continue
			}
			if expected(cfg.expect, status) {
				return body, calls, nil
			}
			err = fmt.Errorf("unexpected status %d: %s", status, snippet(body))
		}

		if cfg.retry == nil || attempt >= cfg.retry.Count || !retryable(cfg.retry, status, err) {
			return nil, calls, err
		}
		time.Sleep(retryDelay(cfg.retry, attempt))
		attempt++
	}
}

func (r *runner) buildRequest(s *Step, cfg settings, it *loopItem, extra map[string]any) (*http.Request, error) {
	rawURL, err := r.render(s.Request.URL, it)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url %q: %w", rawURL, err)
	}

	q := u.Query()
	params := map[string]any{}
	for k, v := range s.Request.Params {
		params[k] = v
	}
	for k, v := range extra {
		params[k] = v
	}
	for k, v := range params {
		rv, err := r.renderValue(v, it)
		if err != nil {
			return nil, err
		}
		// empty params are dropped, eg. optional filters
		if sv := toString(rv); sv != "" {
			q.Set(k, sv)
		}
	}
	u.RawQuery = q.Encode()

	var body io.Reader
	if s.Request.Data != nil {
		data, err := r.renderValue(s.Request.Data, it)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}

	method := strings.ToUpper(s.Request.Method)
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}

	for k, v := range cfg.headers {
		rv, err := r.render(v, it)
		if err != nil {
			return nil, err
		}
		req.Header.Set(k, rv)
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (r *runner) do(req *http.Request, cfg settings) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	resp, err := r.client(cfg.verifySSL).Do(req.WithContext(ctx))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func (r *runner) client(verifySSL bool) *http.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[verifySSL]; ok {
		return c
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: !verifySSL}
	c := &http.Client{Transport: tr}
	r.clients[verifySSL] = c
	return c
}

// settings merge the flow defaults with the step overrides,
// zero/empty step values fall back to the defaults
func (r *runner) settings(s *Step) settings {
	d := r.flow.Defaults
	cfg := settings{
		timeout:    r.timeout,
		verifySSL:  d.VerifySSL,
		headers:    map[string]string{},
		expect:     d.Expect,
		retry:      d.Retry,
		pagination: d.Pagination,
	}

	if d.TimeoutMs > 0 {
		cfg.timeout = time.Duration(d.TimeoutMs) * time.Millisecond
	}
	if s.TimeoutMs != nil && *s.TimeoutMs > 0 {
		cfg.timeout = time.Duration(*s.TimeoutMs) * time.Millisecond
	}
	if cfg.timeout <= 0 {
		cfg.timeout = 30 * time.Second
	}
	if s.VerifySSL != nil {
		cfg.verifySSL = *s.VerifySSL
	}

	for k, v := range d.Headers {
		cfg.headers[k] = v
	}
	for k, v := range s.Request.Headers {
		cfg.headers[k] = v
	}

	if s.Expect != nil && len(s.Expect.Status) > 0 {
		cfg.expect = s.Expect
	}
	cfg.retry = mergeRetry(d.Retry, s.Retry)
	cfg.pagination = mergePagination(d.Pagination, s.Pagination)

	cfg.save = true
	if d.Save != nil {
		cfg.save = *d.Save
	}
	if s.Save != nil {
		cfg.save = *s.Save
	}

	cfg.parallel = r.workers
	if d.Parallel > 0 {
		cfg.parallel = d.Parallel
	}
	if s.Parallel != nil && *s.Parallel > 0 {
		cfg.parallel = *s.Parallel
	}
	if cfg.parallel < 1 {
		cfg.parallel = 1
	}
	return cfg
}

func mergeRetry(d, s *Retry) *Retry {
	if s == nil {
		return d
	}
	if d == nil {
		return s
	}
	m := *d
	if s.Count != 0 {
		m.Count = s.Count
	}
	if s.DelayMs != 0 {
		m.DelayMs = s.DelayMs
	}
	if s.Backoff != "" {
		m.Backoff = s.Backoff
	}
	if s.RetryOn != nil {
		m.RetryOn = s.RetryOn
	}
	if s.RetryOnErrors != nil {
		m.RetryOnErrors = s.RetryOnErrors
	}
	return &m
}

func mergePagination(d, s *Pagination) *Pagination {
	if s == nil {
		return d
	}
	if s.Type == PaginationNone {
		return nil
	}
	if d == nil {
		return s
	}
	m := *d
	if s.Type != "" {
		m.Type = s.Type
	}
	if s.OffsetParam != "" {
		m.OffsetParam = s.OffsetParam
	}
	if s.LimitParam != "" {
		m.LimitParam = s.LimitParam
	}
	if s.StopWhen != nil {
		m.StopWhen = s.StopWhen
	}
	return &m
}

var placeholder = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

// render replace every {{...}} of the string
func (r *runner) render(s string, it *loopItem) (string, error) {
	var firstErr error
	out := placeholder.ReplaceAllStringFunc(s, func(m string) string {
		v, err := r.resolve(placeholder.FindStringSubmatch(m)[1], it)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return v
	})
	return out, firstErr
}

// renderValue render the strings inside maps and lists, other values kept as is
func (r *runner) renderValue(v any, it *loopItem) (any, error) {
	switch t := v.(type) {
	case string:
		return r.render(t, it)
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			rv, err := r.renderValue(val, it)
			if err != nil {
				return nil, err
			}
			m[k] = rv
		}
		return m, nil
	case []any:
		l := make([]any, len(t))
		for i, val := range t {
			rv, err := r.renderValue(val, it)
			if err != nil {
				return nil, err
			}
			l[i] = rv
		}
		return l, nil
	}
	return v, nil
}

// resolve return the value of vars.<name> or <step>.<field>,
// inside a foreach the loop source refers to the current item
func (r *runner) resolve(expr string, it *loopItem) (string, error) {
	if it != nil && it.source == expr {
		return toString(it.value), nil
	}

	name, field, _ := strings.Cut(expr, ".")
	if name == "vars" {
		v, ok := r.flow.Vars[field]
		if !ok {
			return "", fmt.Errorf("undefined var %q", expr)
		}
		if _, isList := v.([]any); isList {
			return "", fmt.Errorf("var %q is a list, use it as foreach", expr)
		}
		return toString(v), nil
	}

	if it != nil && it.source == name {
		if v, ok := it.value.(Record)[field]; ok {
			return v, nil
		}
		return "", fmt.Errorf("field %q not found in current item of %s", field, name)
	}

	recs, ok := r.stepResults(name)
	if !ok {
		return "", fmt.Errorf("%q refers to step %s which has no result (missing depends_on?)", expr, name)
	}
	if st := r.steps[name]; st.Response != nil && st.Response.Type == ResponseList {
		return "", fmt.Errorf("%q refers to list step %s, use it as foreach", expr, name)
	}
	if len(recs) == 0 {
		return "", fmt.Errorf("%q: step %s returned no data", expr, name)
	}
	v, ok := recs[0][field]
	if !ok {
		return "", fmt.Errorf("%q: field %s not found in step %s response", expr, field, name)
	}
	return v, nil
}

// extract build the records declared by the step response from the api reply
func extract(data any, resp *Response) ([]Record, error) {
	var items []any
	switch t := lookup(data, resp.From).(type) {
	case nil:
		if resp.Type == ResponseObject {
			return nil, fmt.Errorf("no data found at %q", resp.From)
		}
	case []any:
		if resp.Type == ResponseObject {
			return nil, fmt.Errorf("expected object at %q, got list", resp.From)
		}
		items = t
	default:
		items = []any{t}
	}

	recs := make([]Record, 0, len(items))
	for _, item := range items {
		rec := Record{}
		for name, paths := range resp.Fields {
			for _, p := range paths {
				if v := lookup(item, p); v != nil {
					rec[name] = toString(v)
					break
				}
			}
			if _, ok := rec[name]; !ok && resp.Type == ResponseObject {
				return nil, fmt.Errorf("field %s not found in response (paths %v)", name, paths)
			}
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// lookup walk a dotted path (eg. "$.data.items", "info.ip", "ports.0.name")
func lookup(data any, path string) any {
	path = strings.TrimPrefix(strings.TrimPrefix(path, "$"), ".")
	if path == "" {
		return data
	}

	cur := data
	for _, seg := range strings.Split(path, ".") {
		switch t := cur.(type) {
		case map[string]any:
			cur = t[seg]
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(t) {
				return nil
			}
			cur = t[i]
		default:
			return nil
		}
	}
	return cur
}

func decodeBody(b []byte) (any, error) {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// itemLabel give a short name of a loop item, eg. the ip of a node record
func itemLabel(v any) string {
	rec, ok := v.(Record)
	if !ok {
		return toString(v)
	}
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([]string, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, rec[k])
	}
	return strings.Join(vals, "_")
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// fileSafe make the name usable as a file or directory name
func fileSafe(s string) string {
	s = strings.Trim(unsafeChars.ReplaceAllString(s, "_"), "._")
	if len(s) > 60 {
		s = s[:60]
	}
	if s == "" {
		s = "item"
	}
	return s
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	case bool:
		return strconv.FormatBool(t)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func intValue(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case json.Number:
		i, _ := t.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(t)
		return i
	}
	return 0
}

// expected check the status, 2xx when no expect is given
func expected(e *Expect, status int) bool {
	if e == nil || len(e.Status) == 0 {
		return status >= 200 && status < 300
	}
	return containsInt(e.Status, status)
}

// retryable check the failure against retry_on / retry_on_errors,
// an empty list means retry on any failure of that kind
func retryable(rt *Retry, status int, err error) bool {
	if status == 0 {
		return len(rt.RetryOnErrors) == 0 || contains(rt.RetryOnErrors, errKind(err))
	}
	return len(rt.RetryOn) == 0 || containsInt(rt.RetryOn, status)
}

func errKind(err error) string {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "timeout"
	}
	return "connection_error"
}

func retryDelay(rt *Retry, attempt int) time.Duration {
	d := time.Duration(rt.DelayMs) * time.Millisecond
	if rt.Backoff == BackoffExponential {
		d <<= attempt
	}
	return d
}

// stopOn check the pagination stop rule, all rules apply when none is given
func stopOn(pg *Pagination, rule string) bool {
	return len(pg.StopWhen) == 0 || contains(pg.StopWhen, rule)
}

func failedDependency(deps []string, done map[string]bool) string {
	for _, d := range deps {
		if !done[d] {
			return d
		}
	}
	return ""
}

func snippet(b []byte) string {
	const n = 200
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

func contains(l []string, v string) bool {
	for _, s := range l {
		if s == v {
			return true
		}
	}
	return false
}

func containsInt(l []int, v int) bool {
	for _, i := range l {
		if i == v {
			return true
		}
	}
	return false
}
