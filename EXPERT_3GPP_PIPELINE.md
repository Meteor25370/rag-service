# Expert 3GPP Pipeline

## Goal

Move from a generic RAG flow to a structured 3GPP expert stack that learns and uses:

- `spec -> release -> version -> clause -> neighboring clause`
- `stage -> taxonomy -> procedures -> messages -> information elements`
- domain concepts such as `MCPTT`, `group communication`, `floor control`, `SIP`, `SDP`, `QoS`

## First Deliverables In This Repo

This repo now contains a first concrete builder:

- [`scripts/expert_3gpp_pipeline.py`](/home/egarcia/rag-service/scripts/expert_3gpp_pipeline.py)

It produces a structured corpus from the local 3GPP archive with:

- one `document` record per spec file or zip entry
- one `clause` record per extracted clause
- one `edge` record per `previous/next clause` relation

## Output Files

The default output directory is:

- `build/expert_3gpp/`

Generated files:

- `documents.jsonl`
- `clauses.jsonl`
- `edges.jsonl`
- `manifest.json`

### `documents.jsonl`

One JSON object per parsed document / zip entry.

Core fields:

- `document_id`
- `source_path`
- `zip_entry_path`
- `spec_id`
- `spec_version`
- `spec_release`
- `spec_stage`
- `version_rank`
- `is_mcptt`
- `clause_count`

### `clauses.jsonl`

One JSON object per clause.

Core fields:

- `clause_key`
- `document_id`
- `spec_id`
- `spec_version`
- `spec_release`
- `spec_stage`
- `clause_index`
- `clause_id`
- `clause_title`
- `taxonomy`
- `terms`
- `text`
- `prev_clause_key`
- `next_clause_key`

### `edges.jsonl`

Current edge types:

- `next_clause`
- `read_failure`

Next edge types to add:

- `same_clause_other_version`
- `same_spec_latest_version`
- `stage1_to_stage2`
- `stage2_to_stage3`
- `procedure_to_message`
- `message_to_ie`

## Taxonomy

The first version uses heuristic tags:

- `requirements`
- `architecture`
- `procedures`
- `messages`
- `security`
- `test`
- `general`

These come from:

- 3GPP stage
- clause title
- clause text keywords

This is only a bootstrap. The target is to replace it with supervised labels.

## Domain Terms

The first version extracts coarse terms such as:

- `mcptt`
- `group_communication`
- `floor_control`
- `sip`
- `sdp`
- `qos`
- `mcvideo`
- `mcdata`
- `security`
- `test`
- `procedure`
- `message`

This provides the seed dictionary for the future 3GPP classifier/reranker.

## Training Roadmap

### Layer 1: Structured Base

Use `documents.jsonl`, `clauses.jsonl`, and `edges.jsonl` to build:

- a clause graph
- release/version alignment
- spec-family browsing
- candidate generation by exact structure

### Layer 2: Specialized Models

Build three task-specific models:

1. `3GPP query classifier`
   - input: user question
   - outputs: `stage`, `task_type`, `spec_family`, `topic`

2. `3GPP embedder / retriever`
   - input: question or clause
   - output: dense vectors adapted to 3GPP semantics

3. `3GPP reranker`
   - input: `(question, clause)`
   - output: expert relevance score

### Layer 3: Guided Generation

The generator should answer with a forced structure:

1. direct answer
2. main clauses/specs
3. technical detail
4. stage split
5. release/version notes
6. limits/ambiguities

## Dataset Extensions To Add Next

Recommended next datasets:

- `query_labels.jsonl`
  - `question`
  - `task_type`
  - `expected_stage`
  - `expected_spec_ids`
  - `expected_clause_ids`

- `qa_gold.jsonl`
  - expert question
  - direct answer
  - supporting clauses
  - negative clauses

- `triples.jsonl`
  - `(query, positive_clause, hard_negative_clause)`
  - for embedder and reranker training

- `alignment.jsonl`
  - link same clause across versions/releases

## How To Run

```bash
cd /home/egarcia/rag-service
python3 scripts/expert_3gpp_pipeline.py --root /data/3gpp --output-dir build/expert_3gpp
```

Quick smoke test:

```bash
cd /home/egarcia/rag-service
python3 scripts/expert_3gpp_pipeline.py --root /data/3gpp --output-dir build/expert_3gpp_sample --limit-files 5
```

## What This Does Not Do Yet

Not implemented yet:

- clause alignment across versions
- message / IE extraction
- expert gold annotations
- training loop for classifier/embedder/reranker
- structured answer generator

This file and the script are the first repo foundation, not the final expert stack.
