# http-batch

http-batch runs batches of chained HTTP API calls described in a JSON file and saves every response to disk.

A call can use values from an earlier call's response, for example a login token or a list of node IPs. A step can loop over a list, page through large results, retry failures and refresh an expired token. Independent flows, and the items of a loop, run in parallel.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Input file](#input-file)
  - [Flow](#flow)
  - [Defaults](#defaults)
  - [Step](#step)
  - [Placeholders](#placeholders)
  - [Response](#response)
  - [Foreach](#foreach)
  - [Pagination](#pagination)
  - [Retry](#retry)
  - [On status (token refresh)](#on-status-token-refresh)
- [Output](#output)
- [Command line options](#command-line-options)
- [Notes](#notes)

## Install

_Download the relevant OS package from [here](https://github.com/pnkj-kmr/http-batch/releases)_

_OR_ build from source (Go 1.22+):

```
make b          # go build -o httpbatch main.go
```

## Quick start

_Create an **input.json** file:_

```json
[
  {
    "name": "nodes-inventory",
    "vars": {
      "base_url": "http://localhost:7070",
      "username": "YOUR_USERNAME",
      "password": "YOUR_PASSWORD"
    },
    "defaults": {
      "timeout_ms": 10000,
      "headers": { "Content-Type": "application/json" }
    },
    "steps": [
      {
        "id": "token_generate",
        "save": false,
        "request": {
          "url": "{{vars.base_url}}/openApi/token/generate",
          "method": "POST",
          "data": { "source": "{{vars.username}}", "key": "{{vars.password}}" }
        },
        "response": {
          "type": "object",
          "fields": { "token": ["token", "auth-token", "jwt"] }
        }
      },
      {
        "id": "fetch_nodes",
        "depends_on": ["token_generate"],
        "request": {
          "url": "{{vars.base_url}}/openApi/resourceInventoryManagementV2/v4/resource",
          "method": "GET",
          "headers": { "Authorization": "Bearer {{token_generate.token}}" },
          "params": { "objectType": "Node" }
        },
        "response": {
          "type": "list",
          "fields": { "ip": "ipAddress" }
        }
      },
      {
        "id": "get_resources",
        "depends_on": ["token_generate", "fetch_nodes"],
        "foreach": "fetch_nodes",
        "request": {
          "url": "{{vars.base_url}}/openApi/resourceInventoryManagementV2/v4/resource",
          "method": "GET",
          "headers": { "Authorization": "Bearer {{token_generate.token}}" },
          "params": { "objectType": "resource", "ipAddress": "{{fetch_nodes.ip}}" }
        },
        "on_status": { "401": { "rerun": "token_generate", "max": 1 } }
      }
    ]
  }
]
```

_Run the binary:_

```
./httpbatch -f input.json -o ./output
```

What happens:

1. `token_generate` logs in and keeps the token.
2. `fetch_nodes` uses the token and keeps the IP of every node.
3. `get_resources` makes one call per node, in parallel, with that node's IP.

Every response is written to `./output`, along with a `summary.json`.

More examples are in [samples/](samples/).

## Input file

The input file is a JSON **array of flows**.

### Flow

A flow is an independent group of steps, and flows run in parallel. Each flow is self-contained: it has its own `vars`, `defaults` and token step.

| Field      | Type   | Description                                                   |
| ---------- | ------ | ------------------------------------------------------------- |
| `name`     | string | Flow name; used in the logs, summary and output file names    |
| `vars`     | object | Values used in placeholders, e.g. `{{vars.base_url}}`; a value can be a string, number or list |
| `defaults` | object | Settings applied to every step (see below)                    |
| `steps`    | array  | Steps, run **in the order written**                           |

### Defaults

These settings apply to every step automatically. A step can override any of them with a field of the same name.

| Field        | Type   | Default                 | Description                                               |
| ------------ | ------ | ----------------------- | --------------------------------------------------------- |
| `timeout_ms` | int    | `-t` flag               | Timeout of one HTTP call, in milliseconds                 |
| `verify_ssl` | bool   | `false`                 | Check the server's TLS certificate; set `false` for self-signed certificates |
| `headers`    | object | -                       | Headers sent on every call                                |
| `expect`     | object | any 2xx status          | `{"status": [200, 201]}`: which statuses count as success |
| `retry`      | object | no retry                | See [Retry](#retry)                                       |
| `pagination` | object | no pagination           | See [Pagination](#pagination)                             |
| `save`       | bool   | `true`                  | Write responses to the output directory                   |
| `parallel`   | int    | `-w` flag               | How many loop items run at the same time                  |

**How a step's settings combine with the defaults:**

- `headers`, `retry` and `pagination` are merged field by field. A field the step sets wins; any field it leaves out comes from `defaults`.
- Other fields are replaced by the step's value.

### Step

| Field        | Type             | Description                                                              |
| ------------ | ---------------- | ------------------------------------------------------------------------ |
| `id`         | string           | Unique name of the step; `vars` and `item` are reserved                  |
| `depends_on` | array            | Steps that must succeed first; if one failed, this step is **skipped**   |
| `foreach`    | string \| array  | Run the step once per item; see [Foreach](#foreach)                      |
| `request`    | object           | The HTTP request (below)                                                 |
| `response`   | object           | Values to keep from the reply for later steps; see [Response](#response) |
| `on_status`  | object           | Action on a given status code; see [On status](#on-status-token-refresh) |
| `save`       | bool             | Write this step's responses to disk; `false` also leaves its records out of the summary (use it for the token step) |
| `parallel`   | int              | Override `defaults.parallel` for this step                               |
| `timeout_ms`, `verify_ssl`, `expect`, `retry`, `pagination` | | Override `defaults` for this step          |

**Request:**

| Field     | Type   | Description                                                          |
| --------- | ------ | -------------------------------------------------------------------- |
| `url`     | string | Request URL                                                          |
| `method`  | string | `GET` (default), `POST`, `PUT`, `PATCH`, `DELETE`, ...               |
| `headers` | object | Added to `defaults.headers`; the step's value wins for the same name |
| `params`  | object | Query parameters; a parameter whose value is **empty is left out**   |
| `data`    | object | JSON body                                                            |

### Placeholders

`{{...}}` can be used in `url`, `headers`, `params` and `data`.

| Placeholder           | Value                                                              |
| --------------------- | ------------------------------------------------------------------ |
| `{{vars.base_url}}`   | A value from `vars`                                                |
| `{{token_generate.token}}` | A field kept by an earlier step whose `response.type` is `object` |
| `{{fetch_nodes.ip}}`  | Inside `foreach: "fetch_nodes"`: the field of the **current** item |
| `{{vars.protocols}}`  | Inside `foreach: "vars.protocols"`: the **current** value          |
| `{{item}}` / `{{item.ip}}` | Inside a `foreach` over a fixed list: the current value / field of the current object |

If a placeholder can't be filled in (e.g. an undefined var, a missing field, or a step that isn't in `depends_on`), the call is not sent and the step fails with a clear error.

### Response

`response` tells which values to keep from the reply. You write it from the actual reply of your API.

```json
"response": {
  "type": "list",
  "from": "$",
  "fields": { "ip": "ipAddress", "name": "info.name" }
}
```

| Field    | Description                                                                     |
| -------- | ------------------------------------------------------------------------------- |
| `type`   | `object` (one item) or `list` (many items)                                      |
| `from`   | Where the data is in the reply: `$` is the whole reply (default), `$.data` is the `data` key |
| `fields` | Name to keep → path **inside each item**; a list of paths, e.g. `["token", "auth-token", "jwt"]`, uses the first one found |

- **Paths** are dotted and can use number indexes, e.g. `ipAddress`, `info.mgmt.ip` or `ports.0.name`.
- **If the reply doesn't match the declared `type`:**
  - `type: list` with an object reply: treated as a list of 1;
  - `type: list` with an empty reply: 0 items, which counts as success;
  - `type: object` with a list reply, or a missing field: an error.

### Foreach

`foreach` runs the step once per item. Items are independent, so they run in parallel (`parallel`). Results are kept in item order.

| Form                                     | Loops over                               | Current item          |
| ---------------------------------------- | ---------------------------------------- | --------------------- |
| `"foreach": "fetch_nodes"`               | The records of a `type: list` step (must be in `depends_on`) | `{{fetch_nodes.ip}}` |
| `"foreach": "vars.protocols"`            | A list in `vars`                         | `{{vars.protocols}}`  |
| `"foreach": ["GPON", "SDH", "LLDP"]`     | A fixed list of values                   | `{{item}}`            |
| `"foreach": [{"ip": "10.0.0.1", "cat": "OLT"}]` | A fixed list of objects           | `{{item.ip}}`, `{{item.cat}}` |

An empty list makes no calls and counts as success. If any item fails, the step fails, and the steps depending on it are skipped.

### Pagination

```json
"pagination": {
  "type": "offset",
  "offset_param": "offset",
  "limit_param": "limit",
  "stop_when": ["short_page", "empty_or_not_list", "repeated_page"]
}
```

The first page uses the step's `params` (e.g. `"limit": 250, "offset": 0`). After each page, `offset` is increased by `limit`. A step pages only when its `limit` param is greater than 0.

Stops when (all three apply if `stop_when` is empty):

- `short_page`: a page returns fewer items than `limit`
- `empty_or_not_list`: nothing at `from`, or the reply isn't a list
- `repeated_page`: the same page is returned twice

`"pagination": { "type": "none" }` turns pagination off for one step (e.g. the token call). There's also a hard limit of 10,000 pages per item.

### Retry

```json
"retry": {
  "count": 1,
  "delay_ms": 1000,
  "backoff": "exponential",
  "retry_on": [429, 500, 502, 503, 504],
  "retry_on_errors": ["timeout", "connection_error"]
}
```

| Field             | Description                                                     |
| ----------------- | --------------------------------------------------------------- |
| `count`           | Retries after the first attempt                                 |
| `delay_ms`        | Wait before a retry                                             |
| `backoff`         | `fixed`, or `exponential` (`delay_ms × 2^attempt`: 1s, 2s, 4s, ...) |
| `retry_on`        | Statuses to retry; empty = retry any unexpected status          |
| `retry_on_errors` | `timeout`, `connection_error`; empty = retry any network error  |

### On status (token refresh)

```json
"on_status": { "401": { "rerun": "token_generate", "max": 1 } }
```

On the given status:

1. the `rerun` step runs again (e.g. to get a new token);
2. the request is sent again with the new values.

This doesn't use up the `retry` count.

- `max` limits the reruns for each request (default 1).
- With parallel items, only **one** refresh runs at a time. Items that hit the same 401 wait and then reuse the new token.
- The `rerun` step must come earlier in the file, and it can't have an `on_status` of its own.

## Output

Everything is written to the `-o` directory.

```
<output dir>/
├── summary.json
├── nodes-inventory_fetch_nodes_0001.json
├── nodes-inventory_get_resources_0001_0001_10.0.0.1.json
├── nodes-inventory_get_resources_0001_0002_10.0.0.2.json
└── topology_get_topology_0001_0003_SDH.json
```

**Response files:** one file for each page of each call, written as soon as it arrives:

- `<flow>_<step>_<page>.json` for a step without `foreach`
- `<flow>_<step>_<page>_<item no>_<item value>.json` for a step with `foreach`

Steps with `save: false` write no files.

**summary.json:** the result of every flow and step:

```json
[
  {
    "flow": "nodes-inventory",
    "ok": true,
    "duration": 1295959000,
    "steps": [
      { "id": "token_generate", "ok": true, "calls": 1, "record_count": 1 },
      {
        "id": "fetch_nodes",
        "ok": true,
        "calls": 1,
        "record_count": 2,
        "records": [ { "ip": "10.0.0.1" }, { "ip": "10.0.0.2" } ],
        "files": [ "nodes-inventory_fetch_nodes_0001.json" ]
      },
      {
        "id": "get_resources",
        "ok": false,
        "calls": 2,
        "error": "unexpected status 500: ...",
        "record_count": 0,
        "files": [ "..." ]
      }
    ]
  }
]
```

| Field          | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| `ok`           | The step succeeded; for a `foreach`, every item succeeded                   |
| `skipped`      | Not run because a step in `depends_on` failed                               |
| `calls`        | HTTP calls made, including retries and token reruns                         |
| `error`        | Errors of the step (or of its items)                                        |
| `record_count` | Records kept by `response`; 0 when the step has no `response`               |
| `records`      | The kept records (left out when `save: false`)                              |
| `files`        | Response files written by this step                                         |
| `duration`     | Flow duration in nanoseconds                                                |

## Command line options

```
./httpbatch --help

Usage of ./httpbatch:
  -f string
        Give a input file name (default "input.json")
  -o string
        Output directory (default ".")
  -t int
        Timeout [secs] (default 30)
  -version
        Application version
  -w int
        Number of workers (default 4)
```

| Option     | Default      | Description                                                                 |
| ---------- | ------------ | --------------------------------------------------------------------------- |
| `-f`       | `input.json` | Input file                                                                  |
| `-o`       | `.`          | Output directory (created if missing)                                       |
| `-t`       | `30`         | Call timeout in seconds, used when neither the step nor `defaults` has `timeout_ms` |
| `-w`       | `4`          | Flows run in parallel; also the default `parallel` for loop items           |
| `-version` | -            | Print the version                                                           |

```
./httpbatch -f ./samples/input1.json -o ./output -w 8 -t 60
```

## Notes

- **Credentials:** `vars` holds the username and password in plain text. Keep real credentials out of git, and use `"save": false` on the token step so the token is never written to disk.
- **Server load:** up to `-w` flows × `parallel` items can call the server at the same time. Increase it step by step.
- **Retry overrides:** zero or empty values in a step's override fall back to `defaults`. For example, `"retry": { "count": 0 }` on a step doesn't turn off a default retry.
- **Error handling:** an invalid input file (unknown step in `depends_on` or `rerun`, a bad `foreach`, a wrong `response.type`) stops the flow before any call is made.

:)
