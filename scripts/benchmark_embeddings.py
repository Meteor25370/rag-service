#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = PROJECT_ROOT / "server.py"


def load_server_module():
    spec = importlib.util.spec_from_file_location("rag_service_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load server module from {SERVER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SERVER = load_server_module()


def list_ollama_models() -> list[str]:
    try:
        payload = SERVER.http_json("GET", SERVER.OLLAMA_TAGS_URL, timeout=10)
    except Exception as exc:
        raise RuntimeError(f"failed to query Ollama tags: {exc}") from exc
    return [str(item.get("name", "")).strip() for item in payload.get("models", []) if item.get("name")]


def embed_texts_with_model(model: str, texts: list[str], embed_url: str) -> list[list[float]]:
    payload = SERVER.http_json(
        "POST",
        embed_url,
        {
            "model": model,
            "input": texts,
        },
        timeout=600,
    )
    embeddings = payload.get("embeddings") or []
    if not embeddings:
        raise RuntimeError(f"ollama returned no embeddings for model {model}")
    return embeddings


def discover_sample_files(root: Path, limit: int) -> list[Path]:
    candidates: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".zip", ".pdf", ".txt", ".doc", ".docx", ".ppt", ".pptx"}:
            continue
        candidates.append(path)
        if len(candidates) >= limit:
            break
    return candidates


def build_sample_texts(files: list[Path], max_chunks_per_file: int, max_chunk_chars: int) -> tuple[list[str], list[dict[str, Any]]]:
    texts: list[str] = []
    details: list[dict[str, Any]] = []
    parser_profiles = "archive_zip,generic_text,pdf,office,3gpp_spec"

    for path in files:
        start = time.perf_counter()
        digest, text = SERVER.read_file_payload_with_timeout(path, parser_profiles, timeout_seconds=120)
        source_meta = SERVER.parse_3gpp_filename_metadata(path, text)
        chunks = SERVER.build_chunks_for_source("3gpp", text, parser_profiles)
        selected_chunks = [entry["chunk_text"][:max_chunk_chars] for entry in chunks[:max_chunks_per_file] if entry.get("chunk_text")]
        if not selected_chunks:
            continue
        texts.extend(selected_chunks)
        details.append(
            {
                "path": str(path),
                "digest": digest,
                "spec_id": source_meta.get("spec_id", ""),
                "spec_version": source_meta.get("spec_version", ""),
                "chunks": len(selected_chunks),
                "read_seconds": round(time.perf_counter() - start, 3),
            }
        )
    return texts, details


def benchmark_model(model: str, texts: list[str], batch_size: int, parallel_requests: int, embed_url: str) -> dict[str, Any]:
    if not texts:
        raise RuntimeError("no texts to benchmark")

    batches = [texts[i : i + batch_size] for i in range(0, len(texts), batch_size)]
    latencies: list[float] = []
    total_vectors = 0
    vector_dim = 0

    start = time.perf_counter()
    for i in range(0, len(batches), parallel_requests):
        group = batches[i : i + parallel_requests]
        group_start = time.perf_counter()
        group_results = [embed_texts_with_model(model, batch, embed_url) for batch in group]
        group_elapsed = time.perf_counter() - group_start
        per_batch_latency = group_elapsed / max(1, len(group_results))
        latencies.extend([per_batch_latency] * len(group_results))
        for embeddings in group_results:
            total_vectors += len(embeddings)
            if embeddings and not vector_dim:
                vector_dim = len(embeddings[0])
    elapsed = time.perf_counter() - start
    total_chars = sum(len(text) for text in texts)
    return {
        "model": model,
        "vectors": total_vectors,
        "dimension": vector_dim,
        "elapsed_seconds": round(elapsed, 3),
        "vectors_per_second": round(total_vectors / elapsed, 2) if elapsed > 0 else 0.0,
        "chars_per_second": round(total_chars / elapsed, 2) if elapsed > 0 else 0.0,
        "batch_size": batch_size,
        "parallel_requests": parallel_requests,
        "embed_url": embed_url,
        "latency_avg_seconds": round(statistics.mean(latencies), 3) if latencies else 0.0,
        "latency_p95_seconds": round(sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], 3) if latencies else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Ollama embedding models on local 3GPP samples.")
    parser.add_argument("--root", default="/data/3gpp", help="Root directory for sample discovery")
    parser.add_argument("--files", type=int, default=3, help="Number of sample files")
    parser.add_argument("--max-chunks-per-file", type=int, default=24, help="Maximum chunks per file")
    parser.add_argument("--max-chunk-chars", type=int, default=1800, help="Maximum characters per chunk")
    parser.add_argument("--batch-size", type=int, default=32, help="Embedding batch size")
    parser.add_argument("--parallel-requests", type=int, default=2, help="Concurrent embedding requests")
    parser.add_argument("--embed-url", default=SERVER.OLLAMA_EMBED_URL, help="Ollama embed endpoint URL")
    parser.add_argument("--models", nargs="*", default=[], help="Explicit model names to benchmark")
    parser.add_argument("--list-models", action="store_true", help="List Ollama models and exit")
    args = parser.parse_args()

    if args.list_models:
        for model in list_ollama_models():
            print(model)
        return 0

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"root does not exist: {root}")

    models = [model for model in args.models if model.strip()]
    if not models:
        available = list_ollama_models()
        preferred = ["embeddinggemma:latest", "nomic-embed-text:latest", "bge-m3:latest"]
        models = [model for model in preferred if model in available]
        if not models:
            models = available[:3]
    if not models:
        raise SystemExit("no Ollama embedding models found")

    files = discover_sample_files(root, args.files)
    if not files:
        raise SystemExit(f"no sample files found under {root}")

    texts, details = build_sample_texts(files, args.max_chunks_per_file, args.max_chunk_chars)
    if not texts:
        raise SystemExit("no sample chunks produced")

    print(json.dumps({
        "sample_files": [str(path) for path in files],
        "sample_details": details,
        "chunk_count": len(texts),
        "models": models,
        "batch_size": args.batch_size,
        "parallel_requests": args.parallel_requests,
    }, indent=2))

    results = []
    for model in models:
        print(f"\n=== Benchmarking {model} ===", file=sys.stderr)
        results.append(benchmark_model(model, texts, args.batch_size, args.parallel_requests, args.embed_url))

    print(json.dumps({"results": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
