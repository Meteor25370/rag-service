#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


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

THREE_GPP_PARSER_PROFILES = "archive_zip,generic_text,pdf,office,3gpp_spec"
DEFAULT_TERMS = {
    "mcptt": ["mcptt", "mission critical push", "push-to-talk"],
    "group_communication": ["group communication", "group communications", "group call", "communications de groupe"],
    "floor_control": ["floor control", "plancher"],
    "sip": [" sip ", "session initiation protocol"],
    "sdp": [" sdp ", "session description protocol"],
    "qos": ["qos", "quality of service"],
    "mcvideo": ["mcvideo", "mission critical video"],
    "mcdata": ["mcdata", "mission critical data"],
    "security": ["security", "securite", "integrity", "ciphering", "authentication", "key management"],
    "test": ["test", "conformance", "validation", "test purpose", "ics"],
    "procedure": ["procedure", "procedures"],
    "message": ["message", "messages", "information element", "information elements"],
}


@dataclass
class CorpusStats:
    files_seen: int = 0
    files_loaded: int = 0
    documents_emitted: int = 0
    clauses_emitted: int = 0
    edges_emitted: int = 0
    failures: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "files_seen": self.files_seen,
            "files_loaded": self.files_loaded,
            "documents_emitted": self.documents_emitted,
            "clauses_emitted": self.clauses_emitted,
            "edges_emitted": self.edges_emitted,
            "failures": self.failures,
        }


def write_progress(progress_file: Path | None, payload: dict[str, Any]) -> None:
    if progress_file is None:
        return
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    progress_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_json_file(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def honor_control(progress_file: Path | None, control_file: Path | None, payload: dict[str, Any]) -> None:
    control = read_json_file(control_file)
    if not control:
        return
    if bool(control.get("stop_requested")):
        payload["status"] = "stopped"
        payload["stop_requested"] = True
        payload["pause_requested"] = False
        payload["message"] = "expert 3GPP pipeline stopped by user"
        write_progress(progress_file, payload)
        raise SystemExit(130)
    while bool(control.get("pause_requested")):
        payload["status"] = "paused"
        payload["pause_requested"] = True
        payload["stop_requested"] = False
        payload["message"] = "expert 3GPP pipeline paused"
        write_progress(progress_file, payload)
        time.sleep(0.5)
        control = read_json_file(control_file)
        if bool(control.get("stop_requested")):
            payload["status"] = "stopped"
            payload["stop_requested"] = True
            payload["pause_requested"] = False
            payload["message"] = "expert 3GPP pipeline stopped by user"
            write_progress(progress_file, payload)
            raise SystemExit(130)
    if payload.get("status") == "paused":
        payload["status"] = "running"
        payload["pause_requested"] = False
        payload["message"] = "expert 3GPP pipeline resumed"
        write_progress(progress_file, payload)


def discover_candidate_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if not SERVER.should_keep(path, "3gpp", THREE_GPP_PARSER_PROFILES):
            continue
        yield path


def text_taxonomy(stage: int | None, clause_title: str, clause_text: str) -> list[str]:
    combined = SERVER.normalize_text(f"{clause_title}\n{clause_text}").lower()
    tags: set[str] = set()
    if stage == 1:
        tags.add("requirements")
    if stage == 2:
        tags.add("architecture")
    if stage == 3:
        tags.add("procedures")
    if any(token in combined for token in ["message", "messages", "information element", "information elements"]):
        tags.add("messages")
    if any(token in combined for token in ["security", "securite", "integrity", "ciphering", "authentication", "key management"]):
        tags.add("security")
    if any(token in combined for token in ["test", "conformance", "ics", "test purpose"]):
        tags.add("test")
    if any(token in combined for token in ["procedure", "procedures"]):
        tags.add("procedures")
    if not tags:
        tags.add("general")
    return sorted(tags)


def domain_terms(clause_title: str, clause_text: str) -> list[str]:
    combined = f" {SERVER.normalize_text(clause_title)}\n{SERVER.normalize_text(clause_text)} ".lower()
    found = [term_name for term_name, patterns in DEFAULT_TERMS.items() if any(pattern in combined for pattern in patterns)]
    return sorted(found)


def build_doc_id(source_path: str, zip_entry_path: str = "") -> str:
    return f"{source_path}::{zip_entry_path}" if zip_entry_path else source_path


def emit_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")
            count += 1
    return count


def process_document(
    *,
    source_path: Path,
    doc_text: str,
    doc_meta: dict[str, Any],
    zip_entry_path: str = "",
    zip_entry_name: str = "",
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    clauses = SERVER.split_3gpp_clauses(doc_text)
    document_id = build_doc_id(str(source_path), zip_entry_path)
    document_row = {
        "document_id": document_id,
        "source_path": str(source_path),
        "zip_entry_path": zip_entry_path,
        "zip_entry_name": zip_entry_name,
        "spec_id": doc_meta.get("spec_id", ""),
        "spec_version": doc_meta.get("spec_version", ""),
        "spec_release": doc_meta.get("spec_release", ""),
        "spec_stage": doc_meta.get("spec_stage"),
        "version_rank": SERVER.threegpp_version_rank(doc_meta.get("spec_version")),
        "is_mcptt": bool(doc_meta.get("spec_id") in SERVER.MCPTT_SPEC_IDS),
        "clause_count": len(clauses),
        "text_chars": len(doc_text),
    }

    clause_rows: list[dict[str, Any]] = []
    edge_rows: list[dict[str, Any]] = []
    previous_clause_key = ""
    for idx, clause in enumerate(clauses, start=1):
        clause_id = str(clause.get("clause_id") or "").strip()
        clause_title = str(clause.get("clause_title") or "").strip()
        clause_text = SERVER.normalize_text(clause.get("text") or "").strip()
        clause_key = f"{document_id}::{clause_id or idx}"
        clause_rows.append(
            {
                "clause_key": clause_key,
                "document_id": document_id,
                "source_path": str(source_path),
                "zip_entry_path": zip_entry_path,
                "spec_id": doc_meta.get("spec_id", ""),
                "spec_version": doc_meta.get("spec_version", ""),
                "spec_release": doc_meta.get("spec_release", ""),
                "spec_stage": doc_meta.get("spec_stage"),
                "is_mcptt": bool(doc_meta.get("spec_id") in SERVER.MCPTT_SPEC_IDS),
                "clause_index": idx,
                "clause_id": clause_id,
                "clause_title": clause_title,
                "taxonomy": text_taxonomy(doc_meta.get("spec_stage"), clause_title, clause_text),
                "terms": domain_terms(clause_title, clause_text),
                "text": clause_text,
                "text_chars": len(clause_text),
                "prev_clause_key": previous_clause_key,
                "next_clause_key": "",
            }
        )
        if previous_clause_key:
            edge_rows.append(
                {
                    "edge_type": "next_clause",
                    "source_clause_key": previous_clause_key,
                    "target_clause_key": clause_key,
                    "document_id": document_id,
                }
            )
        previous_clause_key = clause_key

    for idx in range(len(clause_rows) - 1):
        clause_rows[idx]["next_clause_key"] = clause_rows[idx + 1]["clause_key"]
    return document_row, clause_rows, edge_rows


def build_structured_corpus(
    root: Path,
    output_dir: Path,
    limit: int = 0,
    progress_file: Path | None = None,
    control_file: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stats = CorpusStats()
    document_rows: list[dict[str, Any]] = []
    clause_rows: list[dict[str, Any]] = []
    edge_rows: list[dict[str, Any]] = []
    candidates = list(discover_candidate_files(root))
    if limit:
        candidates = candidates[: max(0, limit)]
    total_files = len(candidates)

    progress_payload = {
        "status": "running",
        "stage": "discover",
        "root": str(root),
        "output_dir": str(output_dir),
        "files_total": total_files,
        "files_seen": 0,
        "files_loaded": 0,
        "documents_emitted": 0,
        "clauses_emitted": 0,
        "edges_emitted": 0,
        "failures": 0,
        "current_file": "",
        "message": "discovering 3GPP files",
        "pause_requested": False,
        "stop_requested": False,
    }
    write_progress(progress_file, progress_payload)

    for index, path in enumerate(candidates, start=1):
        stats.files_seen += 1
        progress_payload.update(
            {
                "status": "running",
                "stage": "read",
                "files_seen": stats.files_seen,
                "files_loaded": stats.files_loaded,
                "documents_emitted": len(document_rows),
                "clauses_emitted": len(clause_rows),
                "edges_emitted": len(edge_rows),
                "failures": stats.failures,
                "current_file": str(path),
                "message": f"reading file {index}/{max(1, total_files)}",
                "pause_requested": False,
                "stop_requested": False,
            }
        )
        honor_control(progress_file, control_file, progress_payload)
        write_progress(progress_file, progress_payload)
        try:
            _digest, text = SERVER.read_file_payload_with_timeout(path, THREE_GPP_PARSER_PROFILES, timeout_seconds=180)
        except Exception as exc:
            stats.failures += 1
            edge_rows.append(
                {
                    "edge_type": "read_failure",
                    "source_path": str(path),
                    "error": str(exc),
                }
            )
            continue
        stats.files_loaded += 1

        if path.suffix.lower() == ".zip":
            entries = SERVER.extract_zip_entries(path, "3gpp", THREE_GPP_PARSER_PROFILES)
            for entry in entries:
                entry_text = SERVER.normalize_text(entry.get("text") or "").strip()
                if not entry_text:
                    continue
                entry_meta = SERVER.parse_3gpp_filename_metadata(Path(entry.get("inner_name") or entry.get("inner_path") or path.name), entry_text)
                document_row, local_clauses, local_edges = process_document(
                    source_path=path,
                    doc_text=entry_text,
                    doc_meta=entry_meta,
                    zip_entry_path=entry.get("inner_path") or "",
                    zip_entry_name=entry.get("inner_name") or "",
                )
                document_rows.append(document_row)
                clause_rows.extend(local_clauses)
                edge_rows.extend(local_edges)
        else:
            source_meta = SERVER.parse_3gpp_filename_metadata(path, text)
            document_row, local_clauses, local_edges = process_document(
                source_path=path,
                doc_text=text,
                doc_meta=source_meta,
            )
            document_rows.append(document_row)
            clause_rows.extend(local_clauses)
            edge_rows.extend(local_edges)

        progress_payload.update(
            {
                "status": "running",
                "stage": "build",
                "files_seen": stats.files_seen,
                "files_loaded": stats.files_loaded,
                "documents_emitted": len(document_rows),
                "clauses_emitted": len(clause_rows),
                "edges_emitted": len(edge_rows),
                "failures": stats.failures,
                "current_file": str(path),
                "message": f"processed file {index}/{max(1, total_files)}",
                "pause_requested": False,
                "stop_requested": False,
            }
        )
        honor_control(progress_file, control_file, progress_payload)
        write_progress(progress_file, progress_payload)

    progress_payload.update(
        {
            "status": "running",
            "stage": "write",
            "files_seen": stats.files_seen,
            "files_loaded": stats.files_loaded,
            "documents_emitted": len(document_rows),
            "clauses_emitted": len(clause_rows),
            "edges_emitted": len(edge_rows),
            "failures": stats.failures,
            "current_file": "",
            "message": "writing output files",
            "pause_requested": False,
            "stop_requested": False,
        }
    )
    honor_control(progress_file, control_file, progress_payload)
    write_progress(progress_file, progress_payload)
    stats.documents_emitted = emit_jsonl(output_dir / "documents.jsonl", document_rows)
    stats.clauses_emitted = emit_jsonl(output_dir / "clauses.jsonl", clause_rows)
    stats.edges_emitted = emit_jsonl(output_dir / "edges.jsonl", edge_rows)

    manifest = {
        "root": str(root),
        "output_dir": str(output_dir),
        "parser_profiles": THREE_GPP_PARSER_PROFILES,
        "stats": stats.as_dict(),
        "files": {
            "documents": "documents.jsonl",
            "clauses": "clauses.jsonl",
            "edges": "edges.jsonl",
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    progress_payload.update(
        {
            "status": "completed",
            "stage": "done",
            "files_seen": stats.files_seen,
            "files_loaded": stats.files_loaded,
            "documents_emitted": stats.documents_emitted,
            "clauses_emitted": stats.clauses_emitted,
            "edges_emitted": stats.edges_emitted,
            "failures": stats.failures,
            "current_file": "",
            "message": "expert 3GPP corpus build completed",
            "pause_requested": False,
            "stop_requested": False,
        }
    )
    write_progress(progress_file, progress_payload)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the first structured 3GPP corpus for expert models.")
    parser.add_argument("--root", default="/data/3gpp", help="3GPP archive root")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "build" / "expert_3gpp"), help="Output directory")
    parser.add_argument("--limit-files", type=int, default=0, help="Optional max number of files to process")
    parser.add_argument("--progress-file", default="", help="Optional JSON progress file")
    parser.add_argument("--control-file", default="", help="Optional JSON control file")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"missing root: {root}")

    progress_file = Path(args.progress_file) if str(args.progress_file).strip() else None
    control_file = Path(args.control_file) if str(args.control_file).strip() else None
    try:
        manifest = build_structured_corpus(
            root,
            Path(args.output_dir),
            limit=max(0, int(args.limit_files)),
            progress_file=progress_file,
            control_file=control_file,
        )
    except SystemExit as exc:
        if int(exc.code or 0) == 130:
            return 130
        raise
    except Exception as exc:
        write_progress(
            progress_file,
            {
                "status": "failed",
                "stage": "error",
                "root": str(root),
                "output_dir": str(Path(args.output_dir)),
                "message": str(exc),
                "pause_requested": False,
                "stop_requested": False,
            },
        )
        raise
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
