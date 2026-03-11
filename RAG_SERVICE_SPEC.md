# Specification - Local RAG Service With Incremental Ingestion and Real-Time Web UI

## 1. Purpose

Build a local RAG service running continuously on the Asus GX10 to maintain a global knowledge base from:

- technical documents
- source code and configuration
- Thunderbird / IMAP email archives

The system must:

- monitor configurable directories
- detect file additions, modifications, and deletions
- parse and clean content
- index data into a vector store and lexical search engine
- expose a real-time web interface for status, control, and troubleshooting

## 2. Goals

The system must provide:

- automated incremental indexing
- clean file-level reindexing
- Thunderbird mailbox support
- attachment text extraction for useful file types
- hybrid search: vector + BM25
- real-time monitoring and control
- resilience to crashes, reboots, and power loss

## 3. Scope

### 3.1 Supported Sources

- generic document directories
- code / script / config directories
- Thunderbird / IMAP archives in mbox format
- symbolic links pointing to such directories

### 3.2 Supported File Types

Documents:

- `txt`
- `md`
- `rst`
- `html`
- `pdf`
- `doc`, `docx`, `docm`
- `xls`, `xlsx`, `xlsm`
- `ppt`, `pptx`, `pptm`
- `csv`

Code / config:

- `py`
- `js`, `ts`
- `java`
- `c`, `cpp`, `h`
- `sh`
- `json`
- `yaml`, `yml`
- `xml`
- `ini`, `cfg`, `conf`
- `sql`

Email:

- Thunderbird mbox
- multipart MIME messages

Useful attachments:

- `pdf`
- `docx`
- `pptx`
- `txt`
- `html`
- `md`
- `csv`
- optionally `xlsx`

### 3.3 Out of Scope for Initial Version

- OCR for images
- audio / video processing
- complex multi-worker parallel execution
- chunk-level resume
- image semantic analysis

## 4. Target Architecture

```text
Sources
 ├─ documents
 ├─ code
 └─ email archives
        │
        ▼
Scanner / Planner
        │
        ├─ SQLite state DB
        ├─ task queue
        └─ ingestion worker
              │
              ├─ parsing
              ├─ cleanup
              ├─ attachment extraction
              ├─ chunking
              ├─ embeddings
              └─ indexing
                    │
                    ├─ Qdrant (vector)
                    └─ BM25 (lexical)

FastAPI Web UI
 ├─ dashboard
 ├─ live monitoring
 ├─ watched directories
 ├─ tasks
 ├─ logs
 └─ search
```

## 5. Technical Stack

Backend:

- Python
- FastAPI
- SQLAlchemy
- SQLite
- Qdrant client
- Ollama client
- custom ingestion pipeline

Services:

- Qdrant in Docker
- Ollama locally
- systemd-managed background service

UI:

- FastAPI HTML templates
- HTMX or lightweight JS
- SSE or WebSocket for live updates

## 6. Functional Requirements

### 6.1 Watched Directories Management

The web UI must allow:

- add watched directory
- edit watched directory
- delete watched directory
- enable / disable watched directory
- choose source type: `documents`, `code`, `thunderbird`, `mixed`
- choose recursive scan on/off
- display configured path and resolved path
- display last scan, last status, last error

### 6.2 Change Detection

The service must detect:

- new file
- modified file
- deleted file

Detection strategy:

- periodic scan
- comparison with persisted local state

Minimum comparison fields:

- `mtime`
- `size`

Confirmation field:

- `content_hash`

### 6.3 Task Types

The system must support:

- `scan_directory`
- `index_file`
- `reindex_file`
- `delete_file`
- `reindex_mbox`
- `full_reindex`

### 6.4 Reindex Strategy

When a file changes:

1. delete existing vectors linked to that file
2. parse file again
3. chunk content
4. embed
5. upload to Qdrant

Chunk-level resume is not required.

### 6.5 Thunderbird / IMAP Handling

The service must:

- detect mbox files, including files without extension
- exclude:
  - `.msf`
  - `.dat`
  - `.com`
  - `.sbd` directories
- extract for each email:
  - subject
  - from
  - to
  - date
  - cleaned text body
- ignore raw Base64 content
- detect useful attachments
- decode useful attachments
- extract text from those attachments
- index attachments as separate documents linked to the parent email

### 6.6 Base64 Handling

The service must:

- detect long Base64 blocks
- never index raw Base64 blobs
- replace useless blobs with logical markers
- decode useful attachments before text extraction

### 6.7 Throttle / Speed Control

The web UI must expose a speed slider from 10% to 100%.

Expected behavior:

- `100%`: continuous processing
- `50%`: pause equals previous processing time
- `25%`: pause equals triple previous processing time

Throttle formula:

```text
sleep_time = work_time * (1 / ratio - 1)
```

Throttle should be applied at least per file, ideally per indexing batch.

### 6.8 Real-Time Web Interface

The UI must show in real time:

- global service status
- configured speed
- number of tasks:
  - pending
  - running
  - failed
  - done
- current task
- current phase:
  - scan
  - parse
  - chunk
  - embedding
  - qdrant upload
- current source
- current file
- current progress:
  - absolute values
  - percentage
- live logs
- errors

### 6.9 Administration

The UI must allow:

- pause
- resume
- stop after current task
- reindex one file
- reindex one directory
- delete + reindex one file
- purge a collection if needed
- inspect indexing stats

## 7. Persistence Model

### 7.1 `watched_directories`

Minimum fields:

- `id`
- `path_configured`
- `path_resolved`
- `source_type`
- `collection_name`
- `parser_profiles`
- `enabled`
- `recursive`
- `include_patterns`
- `exclude_patterns`
- `scan_interval_seconds`
- `last_scan_at`
- `last_status`
- `last_error`

### 7.2 `files`

Minimum fields:

- `id`
- `source_path`
- `resolved_path`
- `source_type`
- `collection_name`
- `parser_profiles`
- `mtime`
- `size`
- `content_hash`
- `status`
- `last_seen_at`
- `last_indexed_at`
- `last_error`
- `file_ingest_id`
- `qdrant_points_count`

### 7.3 `tasks`

Minimum fields:

- `id`
- `task_type`
- `source_path`
- `status`
- `progress_current`
- `progress_total`
- `progress_percent`
- `message`
- `created_at`
- `started_at`
- `finished_at`
- `last_error`

### 7.4 `events`

Minimum fields:

- `id`
- `timestamp`
- `level`
- `category`
- `message`
- `task_id`

### 7.5 `settings`

Minimum fields:

- `key`
- `value`

Examples:

- `service_mode`
- `speed_ratio`
- `paused`

## 8. Indexing Strategy

### 8.1 Collections

Recommended collection layout:

- `docs_streamwide`
- `emails_thunderbird`
- `code_email_agent`

The application must still expose a unified global search experience.

### 8.2 Qdrant Metadata

Each indexed chunk must contain at minimum:

- `source_type`
- `source_path`
- `file_ingest_id`
- `collection_name`

For emails:

- `mailbox`
- `subject`
- `from`
- `date`
- `parent_email_id` if available

For email attachments:

- `attachment_filename`
- `attachment_content_type`
- `parent_subject`
- `parent_mailbox`

### 8.3 Identifiers

The system must use:

- one `file_ingest_id` per file
- ideally one logical `email_id` per email for Thunderbird

These identifiers must support:

- targeted deletion
- targeted reindexing
- traceability

## 9. Search Architecture

### 9.1 Hybrid Retrieval

Search must combine:

- vector search via Qdrant
- lexical search via BM25

### 9.2 Retrieval Flow

```text
query
 ├─ vector search
 ├─ BM25 search
 ├─ fusion
 ├─ reranking (optional)
 └─ final LLM answer
```

### 9.3 Response Requirements

The final answer should be able to provide:

- generated answer
- source references
- source type
- origin path or logical origin

## 10. Real-Time UI

### 10.1 Minimum Pages

Dashboard:

- service state
- speed
- pending / running / failed tasks
- last scan
- current file
- current phase
- current progress
- `Scan now` must trigger a real manual scan for `documents` and `code`
- `thunderbird` remains excluded from manual scan until its dedicated parser is implemented

Watched directories:

- add
- edit
- remove
- enable / disable
- edit path, source type, recursive flag, and scan interval directly from the UI

Tasks:

- queue
- running task
- percentage progress
- errors

Files:

- path
- type
- hash
- last indexed
- last status
- reindex action
- delete + reindex action

Logs:

- live stream
- optional filtering by level or category

Search:

- query input
- top-k limit
- vector search against `global_knowledge`
- direct retrieval mode
- LLM answer mode built from retrieved context
- results
- source path
- source type
- chunk index
- chunk text
- retrieval score
- generated answer with cited source blocks

SQLite explorer:

- table overview
- row counts
- column preview
- record drill-down with pagination

### 10.2 Live Updates

The UI should use:

- SSE or WebSocket

Fallback:

- light polling if needed

Live data must include:

- service status
- current task
- phase
- current file
- progress percentage
- live logs

The web UI should also expose a lightweight SQLite inspection view for operational debugging, with:

- table-level overview
- row count visibility
- record browsing
- paginated drill-down

Watched directory configuration must also allow setting a target `collection_name` so that each source can index into a chosen Qdrant collection.
Watched directory configuration must also allow selecting `parser_profiles` per source. `source_type` remains a high-level domain label, while `parser_profiles` drive the actual extraction pipeline. Typical parser profiles include:

- `generic_text`
- `code`
- `pdf`
- `office`
- `3gpp_spec`
- `thunderbird_mbox`
- `email_attachments`

Each source may enable multiple parser profiles at once, for example:

- `documents` -> `generic_text,pdf,office`
- `code` -> `generic_text,code`
- `3gpp` -> `generic_text,pdf,office,3gpp_spec`
- `thunderbird` -> `thunderbird_mbox,email_attachments`

The service should also support a dedicated `3gpp` source type with:

- default monitoring of `/data/3gpp`
- a dedicated default collection
- extraction of `spec_id`, version/release, stage and clause metadata
- clause-aware chunking so search and answers can cite 3GPP clauses more precisely

The search UI must support two query modes:

- raw retrieval (`Search`)
- retrieval + answer synthesis (`Ask LLM`)

The search UI should also support filtering by:

- `collection_name`
- `source_type`

These filters must be applied in the backend retrieval path, not only in the browser rendering layer.

The service should maintain a lightweight document memory per indexed file:

- a structured summary generated after successful indexing
- stored in SQLite on the `files` table
- reused in `Ask LLM` prompts in addition to raw retrieved chunks
- visible in the search UI so the user can inspect what the system appears to have understood from a document

The answer synthesis mode must support at least two response depth levels:

- `standard`: clear, precise, moderately detailed
- `deep`: clear, precise, more explanatory and more structured

LLM answers must prefer a structured format with:

- direct answer
- explanation
- important details or implications
- sources used

LLM answers should also include inline source citations directly in the body of the answer, for example `[1]` or `[2][3]`, so that factual claims remain traceable without forcing the user to read only the final source list.
In the web UI, these inline citations should be clickable and jump directly to the corresponding retrieved source cards shown below the answer.

## 11. Performance and Scheduling

Current implementation note:

- task execution is split across one scan worker and multiple ingestion workers
- this allows `index_file` and `delete_file` work to continue while a long `scan_directory` is still running
- a blocked file must not stall the entire ingestion queue; other ingestion workers must continue consuming ready tasks
- file stat/read/hash operations must be bounded by a timeout so a hanging filesystem access can fail the current file instead of freezing a worker forever
- file read helpers must run in isolated subprocesses that do not inherit the web server listening socket
- the `systemd` unit must restart cleanly even if a timed-out file helper remains stuck in kernel I/O; unit stop semantics must target the main service process instead of waiting indefinitely on the whole cgroup
- long directory scans should enqueue changed files incrementally during traversal instead of waiting for the full directory walk to complete
- this incremental walk behavior is a general scan-engine rule and must not be implemented as a special case for one watched directory
- task claiming in SQLite must be atomic so that multiple workers cannot start the same task concurrently
- files left in `pending` state without any active task must be detected and requeued automatically by the service
- the transition `file -> pending` and the creation of the corresponding indexing task must be committed atomically in the same SQLite transaction
- files in `error` state should be retryable explicitly in small bounded batches with a bounded number of attempts
- global search should query all configured enabled collections and merge the best results

### 11.1 Duty Cycle

The service must support a configurable `speed_ratio`.

Formula:

```text
sleep_time = work_time * (1 / ratio - 1)
```

### 11.2 Process Priority

The service should be runnable with reduced priority:

- `nice`
- optionally `ionice`

### 11.3 Background Service Mode

The service must run continuously in the background with controlled CPU pressure.

## 12. Resilience

### 12.1 Automatic Restart

The service must run under `systemd` with:

- automatic restart after crash
- automatic start at boot
- deployment managed through a project install script such as `deploy/install-systemd-service.sh`
- a user-scoped unit (`systemctl --user`) is acceptable for local workstation deployment
- a deployed service should keep its SQLite state in a dedicated runtime directory, separate from the editable source tree
- deployment tooling should handle stuck prior processes explicitly before starting the refreshed unit

Qdrant Docker must also use persistent storage and auto-restart.

### 12.2 Recovery After Reboot / Power Loss

At startup, the service must:

- move tasks with status `running` back to `pending`
- perform a consistency scan
- detect changes that occurred during downtime

### 12.3 Recovery Strategy

Chunk-level recovery is not required.

Retained strategy:

- delete vectors for the file
- reindex the full file

## 13. Logging

The system must log:

- scans started / completed
- tasks created
- tasks started / completed / failed
- parse errors
- embedding errors
- Qdrant errors
- deletions / reindex operations

## 14. Safety and Hygiene

The system must:

- never index raw Base64 blobs
- exclude non-useful binaries
- enforce file size limits
- avoid duplicate tasks
- avoid symlink loops
- validate that configured paths exist and are readable

## 15. Acceptance Criteria

The system is accepted if:

- watched directories can be added / edited / removed from the UI
- file changes are correctly detected
- Thunderbird mbox files without extension are handled
- raw Base64 is excluded
- useful attachments are extracted and indexed
- modified files are deleted then reindexed cleanly
- the UI shows real-time:
  - current task
  - current file
  - current phase
  - progress percentage
  - logs
- processing speed is adjustable
- the service resumes cleanly after reboot
- unified global search works

## 16. Recommended Delivery Roadmap

### Phase 1

- documents ingestion
- Qdrant integration
- minimal dashboard

### Phase 2

- Thunderbird mbox ingestion
- attachment extraction
- live UI
- speed control

Current implementation note:

- the service currently performs real docs/code indexing to Qdrant
- Thunderbird mailbox files are now parsed through a dedicated `reindex_mbox` pipeline
- manual scan is allowed for Thunderbird sources
- automatic Thunderbird scan remains conservative until parser tuning and attachment policy are stabilized

### Phase 3

- BM25
- hybrid search
- global search page

### Phase 4

- reranker
- performance optimization
- advanced monitoring
