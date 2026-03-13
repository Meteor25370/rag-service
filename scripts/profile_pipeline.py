#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import time
from pathlib import Path


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile the local rag-service ingest pipeline on one file.")
    parser.add_argument("path", help="File to profile")
    parser.add_argument("--source-type", default="3gpp", help="Logical source type")
    parser.add_argument(
        "--parser-profiles",
        default="archive_zip,generic_text,pdf,office,3gpp_spec",
        help="Parser profiles",
    )
    parser.add_argument("--batch-size", type=int, default=32, help="Embed batch size")
    parser.add_argument("--parallel-requests", type=int, default=2, help="Parallel embed requests")
    parser.add_argument("--skip-summary", action="store_true", help="Skip document memory summary generation")
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"missing path: {path}")

    result: dict[str, object] = {
        "path": str(path),
        "source_type": args.source_type,
        "parser_profiles": args.parser_profiles,
        "batch_size": args.batch_size,
        "parallel_requests": args.parallel_requests,
    }

    t0 = time.perf_counter()
    digest, text = SERVER.read_file_payload_with_timeout(path, args.parser_profiles, timeout_seconds=180)
    t1 = time.perf_counter()

    if path.suffix.lower() == ".zip" and "archive_zip" in set(SERVER.normalize_parser_profiles(args.parser_profiles)):
        zip_entries = SERVER.extract_zip_entries(path, args.source_type, args.parser_profiles)
        source_metadata = SERVER.parse_3gpp_filename_metadata(path, text) if args.source_type == "3gpp" else {}
        chunk_entries = []
        for entry in zip_entries:
            entry_meta = SERVER.parse_3gpp_filename_metadata(Path(entry["inner_name"]), entry["text"]) if args.source_type == "3gpp" else {}
            for chunk_entry in SERVER.build_chunks_for_source(args.source_type, entry["text"], args.parser_profiles):
                chunk_entry.update(
                    {
                        "zip_entry_path": entry["inner_path"],
                        "zip_entry_name": entry["inner_name"],
                        "entry_spec_id": entry_meta.get("spec_id", ""),
                        "entry_spec_version": entry_meta.get("spec_version", ""),
                        "entry_spec_release": entry_meta.get("spec_release", ""),
                        "entry_spec_stage": entry_meta.get("spec_stage"),
                    }
                )
                chunk_entries.append(chunk_entry)
        summary_source_text = "\n\n".join(f"[ZIP ENTRY: {entry['inner_path']}]\n{entry['text']}" for entry in zip_entries[:20])
    else:
        source_metadata = SERVER.parse_3gpp_filename_metadata(path, text) if args.source_type == "3gpp" else {}
        chunk_entries = SERVER.build_chunks_for_source(args.source_type, text, args.parser_profiles)
        summary_source_text = text
        zip_entries = []
    t2 = time.perf_counter()

    chunk_texts = [entry["chunk_text"] for entry in chunk_entries]
    embeddings, total_batches = SERVER.embed_texts_parallel(
        chunk_texts,
        batch_size=args.batch_size,
        parallel_requests=args.parallel_requests,
    )
    t3 = time.perf_counter()

    if len(embeddings) != len(chunk_entries):
        raise RuntimeError("embedding count mismatch")

    summary_text = ""
    if not args.skip_summary:
        summary_text = SERVER.summarize_document_text(
            summary_source_text,
            source_path=str(path),
            source_type=args.source_type,
            model=SERVER.SUMMARY_MODEL,
        ).strip()
    t4 = time.perf_counter()

    result.update(
        {
            "digest": digest,
            "text_chars": len(text),
            "zip_entries": len(zip_entries),
            "chunks": len(chunk_entries),
            "embedding_vectors": len(embeddings),
            "embedding_dim": len(embeddings[0]) if embeddings else 0,
            "embed_batches": total_batches,
            "summary_chars": len(summary_text),
            "spec_id": source_metadata.get("spec_id", ""),
            "spec_version": source_metadata.get("spec_version", ""),
            "timings": {
                "read_seconds": round(t1 - t0, 3),
                "chunk_build_seconds": round(t2 - t1, 3),
                "embed_seconds": round(t3 - t2, 3),
                "summary_seconds": round(t4 - t3, 3),
                "total_seconds": round(t4 - t0, 3),
            },
        }
    )

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
