#!/usr/bin/env python3
import atexit
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import email
import html
import hashlib
import json
import mailbox
import multiprocessing
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from email import policy
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


HOST = "0.0.0.0"
PORT = int(os.environ.get("RAG_SERVICE_PORT", "6334"))
SERVICE_HOME = Path(os.environ.get("RAG_SERVICE_HOME", str(Path(__file__).resolve().parent)))
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = SERVICE_HOME / "rag-service.db"
UI_BUILD_ID = "2026-03-12-runtime-tree-1"
QDRANT_URL = "http://127.0.0.1:6333/collections"
OLLAMA_EMBED_URL = "http://127.0.0.1:11434/api/embed"
OLLAMA_GENERATE_URL = "http://127.0.0.1:11434/api/generate"
OLLAMA_TAGS_URL = "http://127.0.0.1:11434/api/tags"
EMBED_MODEL = "embeddinggemma:latest"
LLM_MODEL = os.environ.get("RAG_SERVICE_LLM_MODEL", "qwen2.5-coder:32b")
SUMMARY_MODEL = os.environ.get("RAG_SERVICE_SUMMARY_MODEL", LLM_MODEL)
VECTOR_COLLECTION = "global_knowledge"
THREE_GPP_COLLECTION = "mcx_3gpp"
JIRA_COLLECTION = "jira"
SCAN_INTERVAL_SECONDS = 300
MAX_LOGS = 300
INDEX_SLEEP_SECONDS = 0.25
CHUNK_SIZE = 1400
CHUNK_OVERLAP = 180
EMBED_BATCH_SIZE = int(os.environ.get("RAG_SERVICE_EMBED_BATCH_SIZE", "32"))
EMBED_PARALLEL_REQUESTS = int(os.environ.get("RAG_SERVICE_EMBED_PARALLEL_REQUESTS", "2"))
SEARCH_TOP_K = 6
INGEST_WORKER_COUNT = int(os.environ.get("RAG_SERVICE_INGEST_WORKERS", "4"))
SCAN_WORKER_COUNT = int(os.environ.get("RAG_SERVICE_SCAN_WORKERS", "2"))
FILE_IO_TIMEOUT_SECONDS = int(os.environ.get("RAG_SERVICE_FILE_IO_TIMEOUT_SECONDS", "20"))
FAILED_RETRY_BATCH_SIZE = int(os.environ.get("RAG_SERVICE_FAILED_RETRY_BATCH_SIZE", "8"))
FAILED_RETRY_MAX_ATTEMPTS = int(os.environ.get("RAG_SERVICE_FAILED_RETRY_MAX_ATTEMPTS", "3"))
SUMMARY_MAX_CHARS = int(os.environ.get("RAG_SERVICE_SUMMARY_MAX_CHARS", "12000"))
THUNDERBIRD_ATTACHMENT_MAX_BYTES = int(os.environ.get("RAG_SERVICE_THUNDERBIRD_ATTACHMENT_MAX_BYTES", str(10 * 1024 * 1024)))
ZIP_ENTRY_MAX_BYTES = int(os.environ.get("RAG_SERVICE_ZIP_ENTRY_MAX_BYTES", str(15 * 1024 * 1024)))
ZIP_MAX_ENTRIES = int(os.environ.get("RAG_SERVICE_ZIP_MAX_ENTRIES", "200"))
SCAN_YIELD_EVERY = int(os.environ.get("RAG_SERVICE_SCAN_YIELD_EVERY", "50"))
SCAN_SLEEP_SECONDS = float(os.environ.get("RAG_SERVICE_SCAN_SLEEP_SECONDS", "0.10"))
SCAN_IDLE_SLEEP_SECONDS = float(os.environ.get("RAG_SERVICE_SCAN_IDLE_SLEEP_SECONDS", "5.0"))
SCAN_MAINTENANCE_INTERVAL_SECONDS = int(os.environ.get("RAG_SERVICE_SCAN_MAINTENANCE_INTERVAL_SECONDS", "60"))
GPU_SNAPSHOT_TTL_SECONDS = float(os.environ.get("RAG_SERVICE_GPU_SNAPSHOT_TTL_SECONDS", "5.0"))
PROCESS_CPU_SAMPLES: dict[Any, tuple[int, float]] = {}
DEFAULT_EMBED_SERVERS = [{"address": "", "weight": 1} for _ in range(4)]
DEFAULT_LLM_SERVER_ADDRESS = ""
OFFICE_SUFFIXES = {
    ".doc",
    ".docx",
    ".docm",
    ".xls",
    ".xlsx",
    ".xlsm",
    ".ppt",
    ".pptx",
    ".pptm",
}
DEFAULT_WATCHED_DIRS = [
    ("/home/egarcia/qdrant/rag-docs/streamwide", "documents", VECTOR_COLLECTION, "archive_zip,generic_text,pdf,office"),
    ("/home/egarcia/qdrant/rag-docs/email-agent", "code", VECTOR_COLLECTION, "archive_zip,generic_text,code"),
    ("/home/egarcia/qdrant/rag-docs/thunderbird", "thunderbird", VECTOR_COLLECTION, "thunderbird_mbox,email_attachments"),
    ("/data/3gpp", "3gpp", THREE_GPP_COLLECTION, "archive_zip,generic_text,pdf,office,3gpp_spec"),
    ("/data/jira-archive", "jira", JIRA_COLLECTION, "jira_issue"),
]
AUTO_SCAN_SOURCE_TYPES = {"code"}
SOURCE_SCAN_INTERVALS = {
    "code": 300,
    "documents": 0,
    "thunderbird": 0,
    "3gpp": 0,
    "jira": 0,
}
ALLOWED_SUFFIXES = {
    ".txt",
    ".md",
    ".rst",
    ".py",
    ".js",
    ".ts",
    ".jsx",
    ".tsx",
    ".java",
    ".kt",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".sh",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".xml",
    ".html",
    ".htm",
    ".pdf",
    ".doc",
    ".docx",
    ".docm",
    ".csv",
    ".xls",
    ".xlsx",
    ".xlsm",
    ".ppt",
    ".pptx",
    ".pptm",
    ".log",
    ".ini",
    ".cfg",
    ".conf",
    ".sql",
    ".mbox",
    ".zip",
    "",
}
EXCLUDED_DIR_NAMES = {
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    ".cache",
    "dist",
    "build",
    ".idea",
    ".vscode",
    ".pytest_cache",
    ".mypy_cache",
    "target",
    "bin",
    "obj",
}
EXCLUDED_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".mp4",
    ".mkv",
    ".mov",
    ".avi",
    ".mp3",
    ".wav",
    ".flac",
    ".tar",
    ".gz",
    ".7z",
    ".rar",
    ".pdf.lock",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".bin",
    ".iso",
    ".class",
    ".o",
    ".a",
    ".lib",
    ".msf",
    ".dat",
    ".com",
}
LOG_PATH_MARKERS = {
    "logs",
    "log",
    "logcat",
    "traces",
    "trace",
}
LOG_NAME_MARKERS = (
    "mobile_log",
    "debug_secure",
    "pre_debug_secure",
    "logcat",
    "device_log",
    "radio_log",
    "trace_",
    "_trace",
    "audit_server_backend",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def parse_iso_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def file_ingest_id(path: str) -> str:
    return hashlib.sha1(path.encode("utf-8")).hexdigest()


def content_hash(path: Path) -> str:
    hasher = hashlib.sha1()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def short_path(path: str, max_len: int = 110) -> str:
    if len(path) <= max_len:
        return path
    return "..." + path[-(max_len - 3):]


def seconds_since_iso(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return 0.0


def format_process_state(code: str) -> str:
    labels = {
        "R": "Running",
        "S": "Sleeping",
        "D": "Blocked I/O",
        "T": "Stopped",
        "Z": "Zombie",
        "I": "Idle",
        "?": "Unknown",
    }
    label = labels.get(code or "?", "Unknown")
    return f"{code} - {label}" if code else label


def parse_proc_stat_text(text: str) -> dict[str, Any] | None:
    try:
        close_paren = text.rfind(")")
        if close_paren <= 0:
            return None
        rest = text[close_paren + 2 :].split()
        pid_start = text.find("(")
        pid = int(text[:pid_start].strip())
        return {
            "pid": pid,
            "state": rest[0],
            "ppid": int(rest[1]),
            "total_ticks": int(rest[11]) + int(rest[12]),
            "start_ticks": int(rest[19]),
            "rss_pages": int(rest[21]),
        }
    except Exception:
        return None


def read_proc_snapshot() -> dict[str, Any]:
    page_size = os.sysconf("SC_PAGE_SIZE")
    clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    uptime_seconds = float(Path("/proc/uptime").read_text(encoding="utf-8").split()[0])
    processes: dict[int, dict[str, Any]] = {}
    for stat_path in Path("/proc").glob("[0-9]*/stat"):
        try:
            parsed = parse_proc_stat_text(stat_path.read_text(encoding="utf-8"))
        except Exception:
            parsed = None
        if not parsed:
            continue
        parsed["rss_bytes"] = parsed["rss_pages"] * page_size
        processes[parsed["pid"]] = parsed
    return {
        "page_size": page_size,
        "clock_ticks": clock_ticks,
        "uptime_seconds": uptime_seconds,
        "processes": processes,
    }


def descendant_pids(root_pid: int, proc_snapshot: dict[str, Any]) -> list[int]:
    processes = proc_snapshot["processes"]
    children_by_parent: dict[int, list[int]] = {}
    for pid, item in processes.items():
        children_by_parent.setdefault(item["ppid"], []).append(pid)
    stack = list(children_by_parent.get(root_pid, []))
    descendants: list[int] = []
    seen: set[int] = set()
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        descendants.append(pid)
        stack.extend(children_by_parent.get(pid, []))
    return descendants


def _runtime_metrics_for_pids(
    pids: list[int],
    *,
    sample_key: Any,
    role: str,
    root_pid: int,
    proc_snapshot: dict[str, Any],
) -> dict[str, Any]:
    processes = proc_snapshot["processes"]
    clock_ticks = proc_snapshot["clock_ticks"]
    uptime_seconds = proc_snapshot["uptime_seconds"]
    root = processes.get(root_pid)
    if not root:
        return {
            "pid": root_pid,
            "role": role,
            "state": "?",
            "cpu_percent": 0.0,
            "cpu_now_percent": 0.0,
            "rss_mb": 0.0,
            "elapsed_seconds": 0.0,
            "alive": False,
            "proc_count": 0,
        }

    total_ticks = sum(processes[pid]["total_ticks"] for pid in pids if pid in processes)
    rss_bytes = sum(processes[pid]["rss_bytes"] for pid in pids if pid in processes)
    elapsed_seconds = max(0.001, uptime_seconds - (root["start_ticks"] / clock_ticks))
    cpu_seconds = total_ticks / clock_ticks
    now = time.monotonic()
    previous = PROCESS_CPU_SAMPLES.get(sample_key)
    instant_cpu_percent = 0.0
    if previous:
        previous_ticks, previous_ts = previous
        delta_ticks = max(0, total_ticks - previous_ticks)
        delta_seconds = max(0.001, now - previous_ts)
        instant_cpu_percent = round((delta_ticks / clock_ticks) / delta_seconds * 100, 1)
    PROCESS_CPU_SAMPLES[sample_key] = (total_ticks, now)
    return {
        "pid": root_pid,
        "role": role,
        "state": root["state"],
        "cpu_percent": round((cpu_seconds / elapsed_seconds) * 100, 1),
        "cpu_now_percent": instant_cpu_percent,
        "rss_mb": round(rss_bytes / (1024 * 1024), 1),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "alive": True,
        "proc_count": len(pids),
    }


def process_runtime_info(pid: int, role: str = "", proc_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        proc_snapshot = proc_snapshot or read_proc_snapshot()
        descendants = descendant_pids(pid, proc_snapshot)
        own_metrics = _runtime_metrics_for_pids(
            [pid],
            sample_key=f"proc:{pid}",
            role=role,
            root_pid=pid,
            proc_snapshot=proc_snapshot,
        )
        tree_metrics = _runtime_metrics_for_pids(
            [pid, *descendants],
            sample_key=f"tree:{pid}",
            role=role,
            root_pid=pid,
            proc_snapshot=proc_snapshot,
        )
        return {
            **own_metrics,
            "tree_cpu_percent": tree_metrics["cpu_percent"],
            "tree_cpu_now_percent": tree_metrics["cpu_now_percent"],
            "tree_rss_mb": tree_metrics["rss_mb"],
            "child_process_count": max(0, tree_metrics["proc_count"] - 1),
            "proc_count": tree_metrics["proc_count"],
        }
    except Exception:
        return {
            "pid": pid,
            "role": role,
            "state": "?",
            "cpu_percent": 0.0,
            "cpu_now_percent": 0.0,
            "rss_mb": 0.0,
            "tree_cpu_percent": 0.0,
            "tree_cpu_now_percent": 0.0,
            "tree_rss_mb": 0.0,
            "child_process_count": 0,
            "proc_count": 0,
            "elapsed_seconds": 0.0,
            "alive": False,
        }


def query_gpu_runtime() -> dict[str, Any]:
    def parse_nvidia_number(value: str) -> int:
        raw = str(value or "").strip()
        if not raw:
            return 0
        lowered = raw.lower()
        if lowered in {"n/a", "[n/a]", "not supported", "[not supported]"}:
            return 0
        return int(float(raw))

    result: dict[str, Any] = {
        "ok": False,
        "gpu_name": "",
        "gpu_util": 0,
        "gpu_memory_used_mb": 0,
        "gpu_memory_total_mb": 0,
        "ollama_gpu_pids": [],
        "ollama_gpu_memory_mb": 0,
        "error": "",
    }
    try:
        summary = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        first_line = next((line.strip() for line in summary.stdout.splitlines() if line.strip()), "")
        if first_line:
            parts = [part.strip() for part in first_line.split(",")]
            if len(parts) >= 4:
                result["gpu_name"] = parts[0]
                result["gpu_util"] = parse_nvidia_number(parts[1])
                result["gpu_memory_used_mb"] = parse_nvidia_number(parts[2])
                result["gpu_memory_total_mb"] = parse_nvidia_number(parts[3])
    except Exception as exc:
        result["error"] = str(exc)
        return result

    try:
        apps = subprocess.run(
            [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_gpu_memory",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        ollama_pids: list[int] = []
        ollama_mem = 0
        for line in apps.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 3:
                continue
            pid_text, process_name, mem_text = parts[:3]
            if "ollama" not in process_name.lower():
                continue
            try:
                pid = int(pid_text)
            except ValueError:
                continue
            ollama_pids.append(pid)
            try:
                ollama_mem += parse_nvidia_number(mem_text)
            except ValueError:
                pass
        result["ollama_gpu_pids"] = sorted(ollama_pids)
        result["ollama_gpu_memory_mb"] = ollama_mem
        result["ok"] = True
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def query_ollama_runtime(proc_snapshot: dict[str, Any]) -> dict[str, Any]:
    serve_pids: list[int] = []
    runner_pids: list[int] = []
    runner_threads_by_pid: dict[int, int] = {}
    try:
        ps_output = subprocess.run(
            ["ps", "-eo", "pid,nlwp,args"],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        for line in ps_output.stdout.splitlines():
            line = line.strip()
            if not line or "ollama" not in line:
                continue
            try:
                pid_text, nlwp_text, args = line.split(None, 2)
                pid = int(pid_text)
                nlwp = int(nlwp_text)
            except ValueError:
                continue
            if "ollama serve" in args:
                serve_pids.append(pid)
            elif "ollama runner" in args:
                runner_pids.append(pid)
                runner_threads_by_pid[pid] = nlwp
    except Exception:
        pass

    info = {
        "serve_pids": serve_pids,
        "runner_pids": runner_pids,
        "runner_count": len(runner_pids),
        "runner_threads": 0,
        "runner_cpu_now_percent": 0.0,
        "runner_cpu_percent": 0.0,
        "runner_memory_mb": 0.0,
    }
    if not runner_pids:
        return info

    total_threads = 0
    cpu_now = 0.0
    cpu_avg = 0.0
    memory_mb = 0.0
    for pid in runner_pids:
        metrics = process_runtime_info(pid, role="ollama-runner", proc_snapshot=proc_snapshot)
        total_threads += runner_threads_by_pid.get(pid, 0)
        cpu_now += metrics.get("tree_cpu_now_percent", 0.0)
        cpu_avg += metrics.get("tree_cpu_percent", 0.0)
        memory_mb += metrics.get("tree_rss_mb", 0.0)
    info["runner_threads"] = total_threads
    info["runner_cpu_now_percent"] = round(cpu_now, 1)
    info["runner_cpu_percent"] = round(cpu_avg, 1)
    info["runner_memory_mb"] = round(memory_mb, 1)
    return info


def normalize_parser_profiles(value: str | list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw = ",".join(str(item) for item in value)
    else:
        raw = str(value)
    return sorted({item.strip() for item in raw.split(",") if item.strip()})


def parser_profile_string(value: str | list[str] | tuple[str, ...] | set[str] | None) -> str:
    return ",".join(normalize_parser_profiles(value))


def is_probably_log_path(name: str | Path) -> bool:
    path = Path(str(name))
    parts = [part.lower() for part in path.parts]
    filename = path.name.lower()
    stem = path.stem.lower()

    if path.suffix.lower() == ".log":
        return True
    if any(part in LOG_PATH_MARKERS or part.startswith("logs_") for part in parts[:-1]):
        return True
    if any(marker in filename for marker in LOG_NAME_MARKERS):
        return True
    if stem.startswith("log_") or stem.endswith("_log"):
        return True
    return False


def inner_name_is_indexable(name: str, source_type: str, parser_profiles: str | list[str] | None = None) -> bool:
    inner_path = Path(name)
    if any(part in EXCLUDED_DIR_NAMES for part in inner_path.parts):
        return False
    if ".sbd" in inner_path.parts:
        return False
    if is_probably_log_path(inner_path):
        return False

    suffix = inner_path.suffix.lower()
    profiles = set(normalize_parser_profiles(parser_profiles))

    if suffix in EXCLUDED_SUFFIXES:
        return False
    if suffix == ".zip":
        return "archive_zip" in profiles
    if source_type == "jira" or "jira_issue" in profiles:
        return suffix in {".html", ".htm", ".txt"}
    if source_type == "thunderbird" or "thunderbird_mbox" in profiles:
        return inner_path.name not in {".DS_Store"}
    if suffix == ".pdf" and "pdf" not in profiles:
        return False
    if suffix in OFFICE_SUFFIXES and "office" not in profiles:
        return False
    if suffix in {".py", ".js", ".ts", ".jsx", ".tsx", ".java", ".kt", ".c", ".cpp", ".h", ".hpp", ".sh", ".sql"}:
        if "code" not in profiles and source_type == "code":
            return False
    return suffix in ALLOWED_SUFFIXES


def should_keep(path: Path, source_type: str, parser_profiles: str | list[str] | None = None) -> bool:
    if not path.is_file():
        return False

    if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
        return False

    if ".sbd" in path.parts:
        return False
    if is_probably_log_path(path):
        return False
    profiles = set(normalize_parser_profiles(parser_profiles))
    if source_type == "jira" or "jira_issue" in profiles:
        suffix = path.suffix.lower()
        if suffix not in {".html", ".htm", ".txt"}:
            return False
        if suffix == ".txt" and path.with_suffix(".html").exists():
            return False

    return inner_name_is_indexable(path.name, source_type, parser_profiles)


def default_parser_profiles_for_source(source_type: str) -> str:
    for _, default_source_type, _, parser_profiles in DEFAULT_WATCHED_DIRS:
        if default_source_type == source_type:
            return parser_profile_string(parser_profiles)
    return "generic_text"


def build_chunks_for_source(source_type: str, text: str, parser_profiles: str | list[str] | None = None) -> list[dict[str, str]]:
    profiles = set(normalize_parser_profiles(parser_profiles))
    if source_type == "3gpp" or "3gpp_spec" in profiles:
        chunks: list[dict[str, str]] = []
        clauses = split_3gpp_clauses(text)
        for clause in clauses:
            for piece in chunk_text(clause["text"]):
                chunks.append(
                    {
                        "chunk_text": piece,
                        "clause_id": clause.get("clause_id", ""),
                        "clause_title": clause.get("clause_title", ""),
                    }
                )
        return chunks
    if source_type == "jira" or "jira_issue" in profiles:
        return [{"chunk_text": piece, "clause_id": "", "clause_title": ""} for piece in chunk_text(text)]
    return [{"chunk_text": piece, "clause_id": "", "clause_title": ""} for piece in chunk_text(text)]


def read_text_file(path: Path, parser_profiles: str | list[str] | None = None) -> str:
    profiles = set(normalize_parser_profiles(parser_profiles))
    source_type = "documents"
    if "code" in profiles:
        source_type = "code"
    if "3gpp_spec" in profiles:
        source_type = "3gpp"
    if "jira_issue" in profiles:
        source_type = "jira"
    if "thunderbird_mbox" in profiles:
        source_type = "thunderbird"
    if path.suffix.lower() == ".zip" and "archive_zip" in profiles:
        return read_zip_file(path, source_type, parser_profiles)
    if path.suffix.lower() == ".pdf" and "pdf" in profiles:
        return read_pdf_file(path)
    if path.suffix.lower() in OFFICE_SUFFIXES and "office" in profiles:
        return read_office_file(path)
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def read_pdf_file(path: Path) -> str:
    result = subprocess.run(
        ["/usr/bin/pdftotext", "-q", str(path), "-"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        error = (result.stderr or "").strip() or f"pdftotext exited with code {result.returncode}"
        raise OSError(error)
    return result.stdout


def read_office_file(path: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="rag-office-") as tmp_dir:
        result = subprocess.run(
            [
                "/usr/bin/soffice",
                "--headless",
                "--convert-to",
                "txt:Text",
                "--outdir",
                tmp_dir,
                str(path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error = (result.stderr or result.stdout or "").strip() or f"soffice exited with code {result.returncode}"
            raise OSError(error)

        converted = list(Path(tmp_dir).glob("*.txt"))
        if not converted:
            raise OSError("no text output produced by libreoffice")
        return converted[0].read_text(encoding="utf-8", errors="ignore")


def decode_document_bytes(data: bytes, name: str, parser_profiles: str | list[str] | None = None) -> str:
    suffix = Path(name).suffix.lower()
    profiles = set(normalize_parser_profiles(parser_profiles))
    if suffix == ".pdf" and "pdf" in profiles:
        with tempfile.TemporaryDirectory(prefix="rag-zip-pdf-") as tmp_dir:
            temp_path = Path(tmp_dir) / (Path(name).name or "document.pdf")
            temp_path.write_bytes(data)
            return read_pdf_file(temp_path)
    if suffix in OFFICE_SUFFIXES and "office" in profiles:
        with tempfile.TemporaryDirectory(prefix="rag-zip-office-") as tmp_dir:
            temp_path = Path(tmp_dir) / (Path(name).name or f"document{suffix or '.bin'}")
            temp_path.write_bytes(data)
            return read_office_file(temp_path)
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="ignore")


def extract_zip_entries(path: Path, source_type: str, parser_profiles: str | list[str] | None = None) -> list[dict[str, str]]:
    profiles = set(normalize_parser_profiles(parser_profiles))
    if "archive_zip" not in profiles:
        return []

    sections: list[dict[str, str]] = []
    processed = 0
    with zipfile.ZipFile(path) as archive:
        for entry in archive.infolist():
            if processed >= ZIP_MAX_ENTRIES:
                break
            if entry.is_dir():
                continue
            if entry.file_size > ZIP_ENTRY_MAX_BYTES:
                continue
            if not inner_name_is_indexable(entry.filename, source_type, parser_profiles):
                continue
            try:
                data = archive.read(entry)
            except Exception:
                continue
            try:
                text = normalize_text(decode_document_bytes(data, entry.filename, parser_profiles))
            except Exception:
                continue
            if not text:
                continue
            sections.append(
                {
                    "inner_path": entry.filename,
                    "inner_name": Path(entry.filename).name,
                    "text": text,
                }
            )
            processed += 1
    return sections


def read_zip_file(path: Path, source_type: str, parser_profiles: str | list[str] | None = None) -> str:
    entries = extract_zip_entries(path, source_type, parser_profiles)
    return "\n\n".join(f"[ZIP ENTRY: {entry['inner_path']}]\n{entry['text']}" for entry in entries).strip()


def normalize_text(text: str) -> str:
    text = text.replace("\x00", " ")
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines).strip()


def clean_html_text(raw_html: str) -> str:
    text = re.sub(r"<[^<]+?>", " ", raw_html or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def html_to_text_blocks(raw_html: str) -> str:
    text = raw_html or ""
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|section|article|tr|li|ul|ol|h1|h2|h3|h4|h5|h6|table)>", "\n", text)
    text = re.sub(r"(?i)<(p|div|section|article|tr|li|ul|ol|h1|h2|h3|h4|h5|h6|table)[^>]*>", "\n", text)
    text = re.sub(r"<[^<]+?>", " ", text)
    text = html.unescape(text)
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return "\n".join(line.strip() for line in text.splitlines() if line.strip()).strip()


def decode_attachment_bytes(data: bytes, filename: str, content_type: str) -> str:
    if not data:
        return ""
    suffix = Path(filename or "").suffix.lower()
    if content_type == "text/html" or suffix in {".html", ".htm"}:
        return clean_html_text(data.decode("utf-8", errors="ignore"))
    if content_type.startswith("text/") or suffix in {".txt", ".md", ".rst", ".log", ".csv", ".json", ".xml", ".yaml", ".yml", ".ini", ".cfg", ".conf"}:
        return data.decode("utf-8", errors="ignore")
    with tempfile.TemporaryDirectory(prefix="rag-mail-attach-") as tmp_dir:
        temp_name = filename or f"attachment{suffix or ''}"
        temp_path = Path(tmp_dir) / temp_name
        temp_path.write_bytes(data)
        if suffix == ".pdf":
            return read_pdf_file(temp_path)
        if suffix in OFFICE_SUFFIXES:
            return read_office_file(temp_path)
    return ""


def extract_email_text(msg: email.message.EmailMessage, parser_profiles: str | list[str] | None = None) -> tuple[str, list[str]]:
    profiles = set(normalize_parser_profiles(parser_profiles))
    body_parts: list[str] = []
    attachment_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            disposition = (part.get_content_disposition() or "").lower()
            filename = part.get_filename() or ""

            if disposition == "attachment" or filename:
                if "email_attachments" not in profiles:
                    continue
                try:
                    data = part.get_payload(decode=True) or b""
                except Exception:
                    data = b""
                if len(data) > THUNDERBIRD_ATTACHMENT_MAX_BYTES:
                    attachment_parts.append(f"[ATTACHMENT: {filename or 'unnamed'} skipped: too large]")
                    continue
                extracted = decode_attachment_bytes(data, filename, content_type)
                if extracted.strip():
                    attachment_parts.append(
                        f"[ATTACHMENT: {filename or 'unnamed'} | {content_type or 'unknown'}]\n{normalize_text(extracted)}"
                    )
                else:
                    attachment_parts.append(f"[ATTACHMENT: {filename or 'unnamed'} | {content_type or 'unknown'}]")
                continue

            if content_type == "text/plain":
                try:
                    body_parts.append(str(part.get_content()))
                except Exception:
                    pass
            elif content_type == "text/html":
                try:
                    body_parts.append(clean_html_text(str(part.get_content())))
                except Exception:
                    pass
    else:
        try:
            content = str(msg.get_content())
        except Exception:
            content = ""
        if (msg.get_content_type() or "").lower() == "text/html":
            content = clean_html_text(content)
        if content:
            body_parts.append(content)

    body = normalize_text("\n\n".join(part for part in body_parts if part))
    attachments = [normalize_text(part) for part in attachment_parts if part]
    return body, attachments


def parse_mbox_messages(path: Path, parser_profiles: str | list[str] | None = None) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    try:
        mbox = mailbox.mbox(path)
    except Exception:
        return messages

    for index, raw_msg in enumerate(mbox, start=1):
        try:
            msg = email.message_from_bytes(raw_msg.as_bytes(), policy=policy.default)
        except Exception:
            continue
        subject = normalize_text(str(msg.get("subject", "")))
        sender = normalize_text(str(msg.get("from", "")))
        recipients = normalize_text(str(msg.get("to", "")))
        date = normalize_text(str(msg.get("date", "")))
        message_id = normalize_text(str(msg.get("message-id", "")))
        body, attachments = extract_email_text(msg, parser_profiles)
        text_sections = [
            f"Subject: {subject}",
            f"From: {sender}",
            f"To: {recipients}",
            f"Date: {date}",
            "",
            body,
        ]
        if attachments:
            text_sections.extend(["", "Attachments:", *attachments])
        text = normalize_text("\n".join(section for section in text_sections if section is not None))
        if not text:
            continue
        messages.append(
            {
                "message_index": index,
                "subject": subject,
                "from": sender,
                "to": recipients,
                "date": date,
                "message_id": message_id,
                "text": text,
            }
        )
    return messages


CLAUSE_HEADER_RE = re.compile(
    r"^(?P<clause_id>(?:\d+\.)*\d+)\s+(?P<title>[A-Z][^\n]{2,200})$",
    re.MULTILINE,
)
SPEC_ID_RE = re.compile(r"(?<!\d)(\d{2}\.\d{3})(?!\d)")
SPEC_FILE_RE = re.compile(r"(?<!\d)(\d{5})-([a-z])(\d{2})(?!\d)", re.IGNORECASE)
MCPTT_SPEC_IDS = {
    "22.179",
    "23.179",
    "24.379",
    "24.380",
    "24.381",
    "24.382",
    "24.383",
    "24.384",
    "26.179",
    "33.179",
}
JIRA_ISSUE_KEY_RE = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")
JIRA_FIELD_LABELS = [
    "Issue",
    "Title",
    "Type",
    "Status",
    "Resolution",
    "Priority",
    "Fix Version/s",
    "Affects Version/s",
    "Labels",
    "Area",
    "Feature",
    "Component",
    "Sprint",
    "Customer",
    "Architecture",
    "Installation Status",
    "Installation Type",
]
JIRA_SECTION_HEADERS = {
    "description",
    "comments",
    "activity",
    "attachments",
    "linked issues",
    "issue links",
    "sub-tasks",
    "details",
    "people",
    "traceability",
}
JIRA_NOISE_PREFIXES = (
    "loading",
    "skip to ",
    "dashboards",
    "projects",
    "issues",
    "boards",
    "plans",
    "create",
    "give feedback",
    "help",
    "jira software help",
    "advanced roadmaps",
    "keyboard shortcuts",
    "about jira",
    "jira credits",
    "profile",
    "accessibility",
    "atlassian marketplace",
    "my jira home",
    "dashboard",
    "issue navigator",
    "log out",
)


def detect_3gpp_stage(spec_id: str | None) -> int | None:
    if not spec_id:
        return None
    try:
        family = int(spec_id.split(".", 1)[0])
    except ValueError:
        return None
    if 21 <= family <= 22:
        return 1
    if 24 <= family <= 29:
        return 3
    if family == 23:
        return 2
    if 31 <= family <= 35:
        return 3
    return None


def parse_3gpp_filename_metadata(path: Path, text: str = "") -> dict[str, Any]:
    name = path.name
    stem = path.stem
    spec_id = None
    version = None
    release = None
    m = SPEC_ID_RE.search(name) or SPEC_ID_RE.search(stem)
    if m:
        spec_id = m.group(1)
    fm = SPEC_FILE_RE.search(name) or SPEC_FILE_RE.search(stem)
    if fm:
        raw = fm.group(1)
        spec_id = f"{raw[:2]}.{raw[2:]}"
        version = f"{fm.group(2).lower()}{fm.group(3)}"
        try:
            release = f"Rel-{10 + (ord(fm.group(2).lower()) - ord('g'))}"
        except Exception:
            release = None
    if not spec_id and text:
        tm = SPEC_ID_RE.search(text[:2000])
        if tm:
            spec_id = tm.group(1)
    stage = detect_3gpp_stage(spec_id)
    return {
        "spec_id": spec_id or "",
        "spec_version": version or "",
        "spec_release": release or "",
        "spec_stage": stage,
    }


def parse_jira_document(path: Path, raw_text: str) -> tuple[str, dict[str, Any]]:
    text = html_to_text_blocks(raw_text) if path.suffix.lower() in {".html", ".htm"} else normalize_text(raw_text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    filtered_lines: list[str] = []
    for line in lines:
        lowered = line.lower()
        if any(lowered.startswith(prefix) for prefix in JIRA_NOISE_PREFIXES):
            continue
        filtered_lines.append(line)

    fields: dict[str, str] = {}
    i = 0
    while i < len(filtered_lines):
        line = filtered_lines[i]
        matched = False
        for label in JIRA_FIELD_LABELS:
            prefix = f"{label}:"
            if line.startswith(prefix):
                value = line[len(prefix):].strip()
                if not value and i + 1 < len(filtered_lines):
                    next_line = filtered_lines[i + 1]
                    if not any(next_line.startswith(f"{other}:") for other in JIRA_FIELD_LABELS):
                        value = next_line.strip()
                        i += 1
                fields[label] = value
                matched = True
                break
        i += 1

    issue_key = fields.get("Issue", "").strip()
    if not issue_key:
        match = JIRA_ISSUE_KEY_RE.search(path.stem) or JIRA_ISSUE_KEY_RE.search(text[:500])
        issue_key = match.group(1) if match else path.stem

    raw_title = fields.get("Title", "").strip()
    if not raw_title and path.suffix.lower() in {".html", ".htm"}:
        title_match = re.search(r"(?is)<title>\s*(.*?)\s*</title>", raw_text or "")
        if title_match:
            raw_title = normalize_text(clean_html_text(title_match.group(1)))
    if raw_title.lower() in {"loading...", "loading…", "loading"}:
        for idx, line in enumerate(filtered_lines):
            if line == issue_key and idx + 1 < len(filtered_lines):
                candidate = filtered_lines[idx + 1].strip()
                if candidate and candidate.lower() not in {"edit", "add comment"}:
                    raw_title = candidate
                    break
    title = re.sub(rf"^\[{re.escape(issue_key)}\]\s*", "", raw_title).strip() if raw_title else ""

    description_lines: list[str] = []
    capture_description = False
    for line in filtered_lines:
        lowered = line.lower()
        if lowered == "description":
            capture_description = True
            continue
        if capture_description and lowered in JIRA_SECTION_HEADERS:
            break
        if capture_description:
            if not any(line.startswith(f"{label}:") for label in JIRA_FIELD_LABELS):
                description_lines.append(line)

    if not description_lines:
        description_lines = [
            line
            for line in filtered_lines[:120]
            if not any(line.startswith(f"{label}:") for label in JIRA_FIELD_LABELS)
            and not JIRA_ISSUE_KEY_RE.fullmatch(line)
            and line != title
        ]

    description = normalize_text("\n".join(description_lines))
    structured_lines = [
        f"Issue: {issue_key}",
        f"Title: {title}" if title else "",
        f"Type: {fields.get('Type', '')}" if fields.get("Type") else "",
        f"Status: {fields.get('Status', '') or fields.get('Resolution', '')}" if (fields.get("Status") or fields.get("Resolution")) else "",
        f"Priority: {fields.get('Priority', '')}" if fields.get("Priority") else "",
        f"Fix Version: {fields.get('Fix Version/s', '')}" if fields.get("Fix Version/s") else "",
        f"Affects Version: {fields.get('Affects Version/s', '')}" if fields.get("Affects Version/s") else "",
        f"Area: {fields.get('Area', '')}" if fields.get("Area") else "",
        f"Feature: {fields.get('Feature', '')}" if fields.get("Feature") else "",
        f"Component: {fields.get('Component', '')}" if fields.get("Component") else "",
        f"Labels: {fields.get('Labels', '')}" if fields.get("Labels") else "",
        f"Sprint: {fields.get('Sprint', '')}" if fields.get("Sprint") else "",
        "",
        "Description:",
        description,
    ]
    normalized_text = normalize_text("\n".join(line for line in structured_lines if line is not None))
    metadata = {
        "issue_key": issue_key,
        "issue_title": title,
        "issue_type": fields.get("Type", ""),
        "issue_status": fields.get("Status", "") or fields.get("Resolution", ""),
        "issue_priority": fields.get("Priority", ""),
        "fix_version": fields.get("Fix Version/s", ""),
        "affects_version": fields.get("Affects Version/s", ""),
        "issue_area": fields.get("Area", ""),
        "issue_feature": fields.get("Feature", ""),
        "issue_component": fields.get("Component", ""),
        "issue_labels": fields.get("Labels", ""),
        "issue_sprint": fields.get("Sprint", ""),
    }
    return normalized_text, metadata


def threegpp_version_rank(version: str | None) -> int:
    raw = str(version or "").strip().lower()
    if not raw:
        return -1
    letter = raw[0]
    digits = raw[1:]
    try:
        number = int(digits)
    except ValueError:
        number = 0
    return (ord(letter) - ord("a") + 1) * 1000 + number


def classify_3gpp_path(path: str | Path) -> dict[str, Any]:
    meta = parse_3gpp_filename_metadata(Path(path))
    spec_id = meta.get("spec_id", "") or ""
    version = meta.get("spec_version", "") or ""
    return {
        **meta,
        "version_rank": threegpp_version_rank(version),
        "is_mcptt": spec_id in MCPTT_SPEC_IDS,
        "family": "mcptt" if spec_id in MCPTT_SPEC_IDS else "other_3gpp",
    }


def task_priority_for(task_type: str, source_path: str, source_type: str) -> int:
    if task_type == "scan_directory":
        return 1000
    if task_type == "delete_file":
        return 1500
    if source_type == "3gpp":
        info = classify_3gpp_path(source_path)
        version_rank = info["version_rank"]
        if task_type == "summarize_file":
            base = 200000 if info["is_mcptt"] else 400000
        else:
            base = 100000 if info["is_mcptt"] else 300000
        return max(1, base - max(0, version_rank))
    if source_type == "code":
        return 500000
    if source_type == "jira":
        return 5000
    if source_type == "thunderbird":
        return 600000
    return 700000


def build_3gpp_plan(rows: list[dict[str, Any]]) -> dict[str, Any]:
    specs: dict[str, dict[str, Any]] = {}
    latest_by_spec: dict[str, dict[str, Any]] = {}
    summary = {
        "mcptt_fast_indexed": 0,
        "mcptt_fast_pending": 0,
        "mcptt_fast_error": 0,
        "mcptt_latest_deep_done": 0,
        "mcptt_latest_deep_pending": 0,
        "other_fast_indexed": 0,
        "other_fast_pending": 0,
        "other_fast_error": 0,
    }

    for row in rows:
        info = classify_3gpp_path(row["source_path"])
        spec_id = info["spec_id"] or "unknown"
        bucket = specs.setdefault(
            spec_id,
            {
                "spec_id": spec_id,
                "family": info["family"],
                "latest_version": "",
                "latest_path": "",
                "versions": 0,
                "fast_indexed": 0,
                "fast_pending": 0,
                "fast_error": 0,
                "deep_latest_done": False,
                "deep_latest_pending": False,
            },
        )
        bucket["versions"] += 1
        status = row.get("status", "")
        if status == "indexed":
            bucket["fast_indexed"] += 1
        elif status == "pending":
            bucket["fast_pending"] += 1
        elif status == "error":
            bucket["fast_error"] += 1

        current_latest = latest_by_spec.get(spec_id)
        if not current_latest or info["version_rank"] > current_latest["version_rank"]:
            latest_by_spec[spec_id] = {
                "version_rank": info["version_rank"],
                "path": row["source_path"],
                "version": info["spec_version"],
                "summary_text": row.get("summary_text") or "",
            }

        if info["is_mcptt"]:
            if status == "indexed":
                summary["mcptt_fast_indexed"] += 1
            elif status == "pending":
                summary["mcptt_fast_pending"] += 1
            elif status == "error":
                summary["mcptt_fast_error"] += 1
        else:
            if status == "indexed":
                summary["other_fast_indexed"] += 1
            elif status == "pending":
                summary["other_fast_pending"] += 1
            elif status == "error":
                summary["other_fast_error"] += 1

    for spec_id, latest in latest_by_spec.items():
        bucket = specs[spec_id]
        bucket["latest_version"] = latest["version"]
        bucket["latest_path"] = latest["path"]
        if bucket["family"] == "mcptt":
            done = bool(latest["summary_text"])
            bucket["deep_latest_done"] = done
            bucket["deep_latest_pending"] = not done
            if done:
                summary["mcptt_latest_deep_done"] += 1
            else:
                summary["mcptt_latest_deep_pending"] += 1

    spec_rows = sorted(
        specs.values(),
        key=lambda item: (
            0 if item["family"] == "mcptt" else 1,
            -threegpp_version_rank(item["latest_version"]),
            item["spec_id"],
        ),
    )
    return {
        "summary": summary,
        "specs": spec_rows,
        "latest_by_spec": {spec_id: value["path"] for spec_id, value in latest_by_spec.items()},
    }


def jira_project_key_for_path(path: str) -> str:
    parts = Path(path).parts
    try:
        idx = parts.index("jira-archive")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    except ValueError:
        pass
    name = Path(path).name
    if "-" in name:
        return name.split("-", 1)[0].upper()
    return "unknown"


def build_jira_plan(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "indexed": 0,
        "pending": 0,
        "error": 0,
        "projects": 0,
    }
    projects: dict[str, dict[str, Any]] = {}
    for row in rows:
        project = jira_project_key_for_path(row.get("source_path") or "")
        bucket = projects.setdefault(
            project,
            {
                "project": project,
                "indexed": 0,
                "pending": 0,
                "error": 0,
                "total": 0,
            },
        )
        status = (row.get("status") or "").strip()
        bucket["total"] += 1
        if status == "indexed":
            bucket["indexed"] += 1
            summary["indexed"] += 1
        elif status == "error":
            bucket["error"] += 1
            summary["error"] += 1
        elif status != "deleted":
            bucket["pending"] += 1
            summary["pending"] += 1
    project_rows = sorted(
        projects.values(),
        key=lambda item: (
            -(item["pending"] + item["error"]),
            -item["total"],
            item["project"],
        ),
    )
    summary["projects"] = len(project_rows)
    return {
        "summary": summary,
        "projects": project_rows,
    }


def split_3gpp_clauses(text: str) -> list[dict[str, str]]:
    matches = list(CLAUSE_HEADER_RE.finditer(text))
    if not matches:
        return [{"clause_id": "", "clause_title": "", "text": text}]
    clauses: list[dict[str, str]] = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        clause_text = text[start:end].strip()
        clauses.append(
            {
                "clause_id": match.group("clause_id").strip(),
                "clause_title": match.group("title").strip(),
                "text": clause_text,
            }
        )
    return clauses


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]
    chunks: list[str] = []
    start = 0
    step = max(1, chunk_size - overlap)
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start += step
    return chunks


def _build_file_payload(path_str: str, parser_profiles: str | list[str] | None = None) -> dict[str, Any]:
    path = Path(path_str)
    try:
        path.stat()
        return {
            "ok": True,
            "digest": content_hash(path),
            "text": normalize_text(read_text_file(path, parser_profiles)),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }


def read_file_payload_with_timeout(
    path: Path, parser_profiles: str | list[str] | None = None, timeout_seconds: int = FILE_IO_TIMEOUT_SECONDS
) -> tuple[str, str]:
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--read-file-payload",
                str(path),
                parser_profile_string(parser_profiles),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            close_fds=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"file read timeout after {timeout_seconds}s") from exc

    if result.returncode != 0 and not result.stdout:
        error = (result.stderr or "").strip() or f"file helper exited with code {result.returncode}"
        raise OSError(error)

    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        error = (result.stderr or result.stdout or "").strip() or "invalid helper output"
        raise OSError(error) from exc

    if not payload.get("ok"):
        error_type = payload.get("error_type", "OSError")
        error = payload.get("error", "unknown file read error")
        if error_type == "FileNotFoundError":
            raise FileNotFoundError(error)
        raise OSError(error)

    return str(payload["digest"]), str(payload["text"])


def http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, method=method)
    request.add_header("Content-Type", "application/json")
    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw) if raw else {}


def compact_embed_server_label(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or parsed.netloc or url
    port = parsed.port
    if not host:
        return "local"
    if port and port != 11434:
        return f"{host}:{port}"
    return host


def normalize_ollama_base_url(address: str) -> str:
    value = (address or "").strip()
    if not value:
        return "http://127.0.0.1:11434"
    if "://" not in value:
        value = f"http://{value}"
    parsed = urlparse(value)
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    if not netloc:
        return "http://127.0.0.1:11434"
    if ":" not in netloc:
        netloc = f"{netloc}:11434"
    base_path = path.rstrip("/")
    return f"{parsed.scheme or 'http'}://{netloc}{base_path}"


def generate_url_from_embed_url(embed_url: str) -> str:
    base = normalize_ollama_base_url(embed_url.replace("/api/embed", ""))
    return f"{base}/api/generate"


def tags_url_from_base_url(base_url: str) -> str:
    return f"{normalize_ollama_base_url(base_url)}/api/tags"


def ensure_qdrant_collection(collection_name: str, vector_size: int) -> None:
    try:
        existing = http_json("GET", QDRANT_URL)
        names = [item["name"] for item in existing.get("result", {}).get("collections", [])]
        if collection_name in names:
            return
    except Exception:
        pass

    http_json(
        "PUT",
        f"{QDRANT_URL}/{collection_name}",
        {
            "vectors": {
                "size": vector_size,
                "distance": "Cosine",
            }
        },
    )


def delete_qdrant_points_by_file(collection_name: str, file_id: str) -> None:
    try:
        http_json(
            "POST",
            f"{QDRANT_URL}/{collection_name}/points/delete",
            {
                "filter": {
                    "must": [
                        {
                            "key": "file_ingest_id",
                            "match": {"value": file_id},
                        }
                    ]
                }
            },
        )
    except HTTPError as exc:
        if exc.code != 404:
            raise


def embed_texts(texts: list[str]) -> tuple[list[list[float]], str]:
    embed_url = OLLAMA_EMBED_URL
    embed_label = compact_embed_server_label(embed_url)
    service_obj = globals().get("SERVICE")
    if service_obj is not None:
        try:
            embed_url = service_obj.choose_embed_url()
            embed_label = compact_embed_server_label(embed_url)
        except Exception:
            embed_url = OLLAMA_EMBED_URL
            embed_label = compact_embed_server_label(embed_url)
    response = http_json(
        "POST",
        embed_url,
        {
            "model": EMBED_MODEL,
            "input": texts,
        },
    )
    embeddings = response.get("embeddings") or []
    if not embeddings:
        raise RuntimeError("ollama returned no embeddings")
    return embeddings, embed_label


def throttle_sleep_duration(work_time: float, ratio: float) -> float:
    ratio = max(0.1, min(1.0, float(ratio)))
    if ratio >= 0.999 or work_time <= 0:
        return 0.0
    return work_time * ((1.0 / ratio) - 1.0)


def embed_texts_timed(texts: list[str]) -> tuple[list[list[float]], float, str]:
    start = time.monotonic()
    embeddings, embed_label = embed_texts(texts)
    return embeddings, time.monotonic() - start, embed_label


def embed_texts_parallel(
    texts: list[str],
    *,
    batch_size: int,
    parallel_requests: int,
    on_batch_done: Any | None = None,
) -> tuple[list[list[float]], int]:
    if not texts:
        return [], 0

    batches = [texts[start : start + batch_size] for start in range(0, len(texts), batch_size)]
    total_batches = len(batches)
    if total_batches == 1:
        embeddings, elapsed, embed_label = embed_texts_timed(batches[0])
        if on_batch_done:
            on_batch_done(1, total_batches, elapsed, embed_label)
        return embeddings, total_batches

    max_workers = max(1, min(parallel_requests, total_batches))
    ordered_results: list[list[list[float]] | None] = [None] * total_batches
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="embed-batch") as executor:
        future_to_batch: dict[Any, int] = {}
        next_batch_index = 0

        while next_batch_index < total_batches and len(future_to_batch) < max_workers:
            future = executor.submit(embed_texts_timed, batches[next_batch_index])
            future_to_batch[future] = next_batch_index
            next_batch_index += 1

        while future_to_batch:
            done, _ = wait(future_to_batch.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                batch_index = future_to_batch.pop(future)
                batch_embeddings, elapsed, embed_label = future.result()
                ordered_results[batch_index] = batch_embeddings
                completed += 1
                if on_batch_done:
                    on_batch_done(completed, total_batches, elapsed, embed_label)
                if next_batch_index < total_batches:
                    next_future = executor.submit(embed_texts_timed, batches[next_batch_index])
                    future_to_batch[next_future] = next_batch_index
                    next_batch_index += 1

    flattened: list[list[float]] = []
    for batch_embeddings in ordered_results:
        if batch_embeddings is None:
            raise RuntimeError("missing embedding batch result")
        flattened.extend(batch_embeddings)
    return flattened, total_batches


def upsert_qdrant_points(collection_name: str, points: list[dict[str, Any]]) -> None:
    http_json(
        "PUT",
        f"{QDRANT_URL}/{collection_name}/points",
        {
            "points": points,
        },
    )


def search_qdrant(collection_name: str, vector: list[float], limit: int = SEARCH_TOP_K) -> list[dict[str, Any]]:
    payload = http_json(
        "POST",
        f"{QDRANT_URL}/{collection_name}/points/search",
        {
            "vector": vector,
            "limit": limit,
            "with_payload": True,
            "with_vector": False,
        },
    )
    return payload.get("result") or []


def list_ollama_models() -> list[str]:
    tags_url = OLLAMA_TAGS_URL
    service_obj = globals().get("SERVICE")
    if service_obj is not None:
        try:
            tags_url = service_obj.configured_llm_tags_url()
        except Exception:
            tags_url = OLLAMA_TAGS_URL
    payload = http_json("GET", tags_url, timeout=20)
    models = []
    for item in payload.get("models", []) or []:
        name = item.get("name")
        if name:
            models.append(str(name))
    return models


def generate_answer(prompt: str, model: str, generate_url: str = OLLAMA_GENERATE_URL) -> str:
    response = http_json(
        "POST",
        generate_url,
        {
            "model": model,
            "prompt": prompt,
            "stream": False,
        },
        timeout=180,
    )
    text = str(response.get("response") or "").strip()
    if not text:
        raise RuntimeError("ollama returned no answer")
    return text


def summarize_document_text(
    text: str,
    *,
    source_path: str,
    source_type: str,
    model: str = SUMMARY_MODEL,
    generate_url: str = OLLAMA_GENERATE_URL,
) -> str:
    clipped = text[:SUMMARY_MAX_CHARS].strip()
    if not clipped:
        return ""
    prompt = (
        "You produce a structured document memory entry from the provided content.\n"
        "Use only the provided content.\n"
        "Be clear, precise, and useful for later retrieval and explanation.\n"
        "Do not invent missing details.\n"
        "Write in English.\n"
        "Use this exact structure:\n"
        "Overview:\n"
        "- ...\n"
        "Key points:\n"
        "- ...\n"
        "How it works / what it describes:\n"
        "- ...\n"
        "Important entities / terms:\n"
        "- ...\n"
        "Open questions or limits:\n"
        "- ...\n\n"
        f"Source path: {source_path}\n"
        f"Source type: {source_type}\n\n"
        "Content:\n"
        f"{clipped}\n\n"
        "Structured memory:"
    )
    return generate_answer(prompt, model, generate_url=generate_url)


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_db()
        self._bootstrap_defaults()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=60, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def _init_db(self) -> None:
        PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS watched_directories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path_configured TEXT NOT NULL UNIQUE,
                    path_resolved TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    collection_name TEXT NOT NULL DEFAULT 'global_knowledge',
                    parser_profiles TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    recursive INTEGER NOT NULL DEFAULT 1,
                    scan_interval_seconds INTEGER NOT NULL DEFAULT 30,
                    last_scan_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    watched_directory_id INTEGER NOT NULL,
                    source_path TEXT NOT NULL UNIQUE,
                    resolved_path TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    collection_name TEXT NOT NULL DEFAULT 'global_knowledge',
                    parser_profiles TEXT NOT NULL DEFAULT '',
                    mtime REAL,
                    size INTEGER,
                    content_hash TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_seen_at TEXT,
                    last_indexed_at TEXT,
                    last_error TEXT,
                    summary_text TEXT,
                    summary_model TEXT,
                    summary_updated_at TEXT,
                    file_ingest_id TEXT NOT NULL,
                    qdrant_points_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (watched_directory_id) REFERENCES watched_directories(id)
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    source_type TEXT,
                    task_priority INTEGER NOT NULL DEFAULT 500000,
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress_current INTEGER NOT NULL DEFAULT 0,
                    progress_total INTEGER NOT NULL DEFAULT 0,
                    progress_percent INTEGER NOT NULL DEFAULT 0,
                    message TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    category TEXT NOT NULL,
                    message TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS worker_runtime (
                    pid INTEGER PRIMARY KEY,
                    role TEXT NOT NULL,
                    task_type TEXT,
                    current_file TEXT,
                    message TEXT,
                    embed_server TEXT,
                    progress_current INTEGER NOT NULL DEFAULT 0,
                    progress_total INTEGER NOT NULL DEFAULT 0,
                    progress_percent INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_status_type_id
                ON tasks(status, task_type, id);

                CREATE INDEX IF NOT EXISTS idx_tasks_type_path_status
                ON tasks(task_type, source_path, status);

                CREATE INDEX IF NOT EXISTS idx_files_status_dir
                ON files(status, watched_directory_id);
                """
            )
            watched_cols = [row["name"] for row in conn.execute("PRAGMA table_info(watched_directories)").fetchall()]
            if "collection_name" not in watched_cols:
                conn.execute("ALTER TABLE watched_directories ADD COLUMN collection_name TEXT NOT NULL DEFAULT 'global_knowledge'")
            if "parser_profiles" not in watched_cols:
                conn.execute("ALTER TABLE watched_directories ADD COLUMN parser_profiles TEXT NOT NULL DEFAULT ''")
            file_cols = [row["name"] for row in conn.execute("PRAGMA table_info(files)").fetchall()]
            if "collection_name" not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN collection_name TEXT NOT NULL DEFAULT 'global_knowledge'")
            if "parser_profiles" not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN parser_profiles TEXT NOT NULL DEFAULT ''")
            if "summary_text" not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN summary_text TEXT")
            if "summary_model" not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN summary_model TEXT")
            if "summary_updated_at" not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN summary_updated_at TEXT")
            worker_cols = [row["name"] for row in conn.execute("PRAGMA table_info(worker_runtime)").fetchall()]
            if "embed_server" not in worker_cols:
                conn.execute("ALTER TABLE worker_runtime ADD COLUMN embed_server TEXT")
            task_cols = [row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
            if "task_priority" not in task_cols:
                conn.execute("ALTER TABLE tasks ADD COLUMN task_priority INTEGER NOT NULL DEFAULT 500000")

    def _bootstrap_defaults(self) -> None:
        now = utc_now()
        with self.connect() as conn:
            for path, source_type, collection_name, parser_profiles in DEFAULT_WATCHED_DIRS:
                resolved = str(Path(path))
                interval = SOURCE_SCAN_INTERVALS.get(source_type, SCAN_INTERVAL_SECONDS)
                conn.execute(
                    """
                    INSERT INTO watched_directories (
                        path_configured, path_resolved, source_type, collection_name, parser_profiles, enabled,
                        recursive, scan_interval_seconds, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
                    ON CONFLICT(path_configured) DO NOTHING
                    """,
                    (
                        path,
                        resolved,
                        source_type,
                        collection_name,
                        parser_profile_string(parser_profiles),
                        interval,
                        now,
                        now,
                    ),
                )
            for path, source_type, collection_name, parser_profiles in DEFAULT_WATCHED_DIRS:
                exists = conn.execute(
                    "SELECT 1 FROM watched_directories WHERE path_configured = ?",
                    (path,),
                ).fetchone()
                if not exists:
                    interval = SOURCE_SCAN_INTERVALS.get(source_type, SCAN_INTERVAL_SECONDS)
                    conn.execute(
                        """
                        INSERT INTO watched_directories (
                            path_configured, path_resolved, source_type, collection_name, parser_profiles, enabled,
                            recursive, scan_interval_seconds, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
                        """,
                        (
                            path,
                            str(Path(path)),
                            source_type,
                            collection_name,
                            parser_profile_string(parser_profiles),
                            interval,
                            now,
                            now,
                        ),
                    )
            for _, source_type, _, parser_profiles in DEFAULT_WATCHED_DIRS:
                conn.execute(
                    """
                    UPDATE watched_directories
                    SET parser_profiles = ?, updated_at = ?
                    WHERE source_type = ? AND (parser_profiles IS NULL OR TRIM(parser_profiles) = '')
                    """,
                    (parser_profile_string(parser_profiles), now, source_type),
                )
                conn.execute(
                    """
                    UPDATE files
                    SET parser_profiles = ?, updated_at = ?
                    WHERE source_type = ? AND (parser_profiles IS NULL OR TRIM(parser_profiles) = '')
                    """,
                    (parser_profile_string(parser_profiles), now, source_type),
                )
            for _, source_type, _, parser_profiles in DEFAULT_WATCHED_DIRS:
                desired = set(normalize_parser_profiles(parser_profiles))
                watched_rows = conn.execute(
                    "SELECT id, parser_profiles FROM watched_directories WHERE source_type = ?",
                    (source_type,),
                ).fetchall()
                for row in watched_rows:
                    merged = parser_profile_string(set(normalize_parser_profiles(row["parser_profiles"])) | desired)
                    conn.execute(
                        "UPDATE watched_directories SET parser_profiles = ?, updated_at = ? WHERE id = ?",
                        (merged, now, row["id"]),
                    )
                file_rows = conn.execute(
                    "SELECT id, parser_profiles FROM files WHERE source_type = ?",
                    (source_type,),
                ).fetchall()
                for row in file_rows:
                    merged = parser_profile_string(set(normalize_parser_profiles(row["parser_profiles"])) | desired)
                    conn.execute(
                        "UPDATE files SET parser_profiles = ?, updated_at = ? WHERE id = ?",
                        (merged, now, row["id"]),
                    )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('speed_ratio', '1.0') ON CONFLICT(key) DO NOTHING"
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('embed_batch_size', ?) ON CONFLICT(key) DO NOTHING",
                (str(EMBED_BATCH_SIZE),),
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('embed_parallel_requests', ?) ON CONFLICT(key) DO NOTHING",
                (str(EMBED_PARALLEL_REQUESTS),),
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('paused', '0') ON CONFLICT(key) DO NOTHING"
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('ingest_worker_count', ?) ON CONFLICT(key) DO NOTHING",
                (str(INGEST_WORKER_COUNT),),
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('scan_worker_count', ?) ON CONFLICT(key) DO NOTHING",
                (str(SCAN_WORKER_COUNT),),
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('embed_servers_json', ?) ON CONFLICT(key) DO NOTHING",
                (json.dumps(DEFAULT_EMBED_SERVERS),),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status = 'pending', started_at = NULL, message = COALESCE(message, 'requeued on startup')
                WHERE status = 'running'
                """
            )

    def add_event(self, level: str, category: str, message: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO events(timestamp, level, category, message) VALUES (?, ?, ?, ?)",
                (utc_now(), level, category, message),
            )
            conn.execute(
                """
                DELETE FROM events
                WHERE id NOT IN (
                    SELECT id FROM events ORDER BY id DESC LIMIT ?
                )
                """,
                (MAX_LOGS,),
            )

    def get_setting(self, key: str, default: str) -> str:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def update_worker_runtime(
        self,
        *,
        pid: int,
        role: str,
        task_type: str,
        current_file: str,
        message: str,
        embed_server: str = "",
        progress_current: int = 0,
        progress_total: int = 0,
        progress_percent: int = 0,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO worker_runtime(
                    pid, role, task_type, current_file, message, embed_server,
                    progress_current, progress_total, progress_percent, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pid) DO UPDATE SET
                    role = excluded.role,
                    task_type = excluded.task_type,
                    current_file = excluded.current_file,
                    message = excluded.message,
                    embed_server = excluded.embed_server,
                    progress_current = excluded.progress_current,
                    progress_total = excluded.progress_total,
                    progress_percent = excluded.progress_percent,
                    updated_at = excluded.updated_at
                """,
                (
                    pid,
                    role,
                    task_type,
                    current_file,
                    message,
                    embed_server,
                    progress_current,
                    progress_total,
                    progress_percent,
                    utc_now(),
                ),
            )

    def clear_worker_runtime(self, pid: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM worker_runtime WHERE pid = ?", (pid,))

    def worker_runtime_rows(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT pid, role, task_type, current_file, message, COALESCE(embed_server, '') AS embed_server,
                       progress_current, progress_total, progress_percent, updated_at
                FROM worker_runtime
                ORDER BY role, pid
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_watched_dirs(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, path_configured, path_resolved, source_type, collection_name, parser_profiles, enabled, recursive,
                       scan_interval_seconds, last_scan_at, last_status, last_error
                FROM watched_directories
                ORDER BY id
                """
            ).fetchall()
        items = []
        for row in rows:
            items.append(
                {
                    "id": row["id"],
                    "path": row["path_configured"],
                    "resolved_path": row["path_resolved"],
                    "exists": os.path.lexists(row["path_configured"]),
                    "enabled": bool(row["enabled"]),
                    "source_type": row["source_type"],
                    "collection_name": row["collection_name"],
                    "parser_profiles": row["parser_profiles"],
                    "recursive": bool(row["recursive"]),
                    "scan_interval_seconds": row["scan_interval_seconds"],
                    "last_scan_at": row["last_scan_at"],
                    "last_status": row["last_status"],
                    "last_error": row["last_error"],
                }
            )
        return items

    def add_watched_dir(
        self,
        path_configured: str,
        source_type: str,
        collection_name: str = VECTOR_COLLECTION,
        parser_profiles: str = "",
        enabled: bool = True,
        recursive: bool = True,
        scan_interval_seconds: int | None = None,
    ) -> int:
        now = utc_now()
        path = str(Path(path_configured))
        resolved = str(Path(path).resolve(strict=False))
        interval = SOURCE_SCAN_INTERVALS.get(source_type, SCAN_INTERVAL_SECONDS) if scan_interval_seconds is None else scan_interval_seconds
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO watched_directories (
                    path_configured, path_resolved, source_type, collection_name, parser_profiles, enabled, recursive,
                    scan_interval_seconds, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    path,
                    resolved,
                    source_type,
                    collection_name or VECTOR_COLLECTION,
                    parser_profile_string(parser_profiles) or default_parser_profiles_for_source(source_type),
                    1 if enabled else 0,
                    1 if recursive else 0,
                    interval,
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def update_watched_dir(
        self,
        watched_id: int,
        path_configured: str,
        source_type: str,
        collection_name: str,
        parser_profiles: str,
        enabled: bool,
        recursive: bool,
        scan_interval_seconds: int,
    ) -> None:
        path = str(Path(path_configured))
        resolved = str(Path(path).resolve(strict=False))
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE watched_directories
                SET path_configured = ?, path_resolved = ?, source_type = ?, collection_name = ?, parser_profiles = ?, enabled = ?, recursive = ?,
                    scan_interval_seconds = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    path,
                    resolved,
                    source_type,
                    collection_name or VECTOR_COLLECTION,
                    parser_profile_string(parser_profiles) or default_parser_profiles_for_source(source_type),
                    1 if enabled else 0,
                    1 if recursive else 0,
                    scan_interval_seconds,
                    utc_now(),
                    watched_id,
                ),
            )

    def delete_watched_dir(self, watched_id: int) -> None:
        with self.connect() as conn:
            row = conn.execute("SELECT id FROM watched_directories WHERE id = ?", (watched_id,)).fetchone()
            if not row:
                return
            conn.execute("DELETE FROM files WHERE watched_directory_id = ?", (watched_id,))
            conn.execute("DELETE FROM watched_directories WHERE id = ?", (watched_id,))

    def queue_task(self, task_type: str, source_path: str, source_type: str, message: str = "") -> None:
        priority = task_priority_for(task_type, source_path, source_type)
        with self.connect() as conn:
            existing = conn.execute(
                """
                SELECT id FROM tasks
                WHERE task_type = ? AND source_path = ? AND status IN ('pending', 'running')
                """,
                (task_type, source_path),
            ).fetchone()
            if existing:
                return
            conn.execute(
                """
                INSERT INTO tasks(task_type, source_path, source_type, task_priority, status, message, created_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?)
                """,
                (task_type, source_path, source_type, priority, message, utc_now()),
            )

    def next_task(self, allowed_task_types: set[str] | None = None) -> sqlite3.Row | None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if allowed_task_types:
                placeholders = ",".join("?" for _ in allowed_task_types)
                row = conn.execute(
                    f"""
                    SELECT * FROM tasks
                    WHERE status = 'pending' AND task_type IN ({placeholders})
                    ORDER BY task_priority ASC, id ASC
                    LIMIT 1
                    """,
                    tuple(sorted(allowed_task_types)),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE status = 'pending'
                    ORDER BY task_priority ASC, id ASC
                    LIMIT 1
                    """
                ).fetchone()
            if not row:
                return None
            updated = conn.execute(
                """
                UPDATE tasks
                SET status = 'running', started_at = ?, message = COALESCE(message, '')
                WHERE id = ? AND status = 'pending'
                """,
                (utc_now(), row["id"]),
            )
            if updated.rowcount != 1:
                return None
            if row["task_type"] in {"index_file", "reindex_mbox", "delete_file", "summarize_file"}:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'done', finished_at = ?, message = 'deduplicated pending task'
                    WHERE id != ? AND task_type = ? AND source_path = ? AND status = 'pending'
                    """,
                    (utc_now(), row["id"], row["task_type"], row["source_path"]),
                )
            return conn.execute("SELECT * FROM tasks WHERE id = ?", (row["id"],)).fetchone()

    def update_task_progress(
        self, task_id: int, current: int, total: int, message: str, status: str = "running"
    ) -> None:
        percent = int((current / total) * 100) if total else 0
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET progress_current = ?, progress_total = ?, progress_percent = ?, message = ?, status = ?
                WHERE id = ?
                """,
                (current, total, percent, message, status, task_id),
            )

    def finish_task(self, task_id: int, message: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'done', finished_at = ?, progress_percent = 100, message = ?
                WHERE id = ?
                """,
                (utc_now(), message, task_id),
            )

    def fail_task(self, task_id: int, error: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'failed', finished_at = ?, last_error = ?, message = ?
                WHERE id = ?
                """,
                (utc_now(), error, error, task_id),
            )

    def get_task_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "running": 0, "failed": 0, "done": 0}
        with self.connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS c FROM tasks GROUP BY status").fetchall()
        for row in rows:
            counts[row["status"]] = row["c"]
        return counts

    def recent_tasks(self, limit: int = 12) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_type, source_path, source_type, task_priority, status, progress_percent, message,
                       created_at, started_at, finished_at, last_error
                FROM tasks
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def threegpp_file_rows(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT source_path, status, summary_text, summary_updated_at
                FROM files
                WHERE source_type = '3gpp' AND status != 'deleted'
                ORDER BY source_path
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def jira_file_rows(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT source_path, status, last_error
                FROM files
                WHERE source_type = 'jira' AND status != 'deleted'
                ORDER BY source_path
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def planned_tasks(self, limit: int = 30) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_type, source_path, source_type, task_priority, status, message, created_at
                FROM tasks
                WHERE status IN ('pending', 'running')
                ORDER BY task_priority ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recompute_pending_task_priorities(self) -> int:
        updated = 0
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, task_type, source_path, source_type, task_priority
                FROM tasks
                WHERE status IN ('pending', 'running')
                """
            ).fetchall()
            for row in rows:
                expected = task_priority_for(row["task_type"], row["source_path"], row["source_type"])
                if int(row["task_priority"] or 0) != expected:
                    conn.execute(
                        "UPDATE tasks SET task_priority = ? WHERE id = ?",
                        (expected, row["id"]),
                    )
                    updated += 1
        return updated

    def recent_events(self, limit: int = 40) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp AS time, level, category, message
                FROM events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_collection_names(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT collection_name
                FROM watched_directories
                WHERE enabled = 1 AND collection_name IS NOT NULL AND TRIM(collection_name) != ''
                ORDER BY collection_name
                """
            ).fetchall()
        names = [str(row["collection_name"]) for row in rows]
        return names or [VECTOR_COLLECTION]

    def db_overview(self) -> list[dict[str, Any]]:
        tables = ["watched_directories", "files", "tasks", "events", "settings"]
        overview: list[dict[str, Any]] = []
        with self.connect() as conn:
            for table in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
                overview.append(
                    {
                        "table": table,
                        "count": count,
                        "columns": columns,
                    }
                )
        return overview

    def table_rows(self, table: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        allowed = {"watched_directories", "files", "tasks", "events", "settings"}
        if table not in allowed:
            raise ValueError("invalid table")
        limit = max(1, min(200, limit))
        offset = max(0, offset)
        with self.connect() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        return {
            "table": table,
            "columns": columns,
            "rows": [dict(row) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_file_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "indexed": 0, "deleted": 0, "error": 0}
        with self.connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS c FROM files GROUP BY status").fetchall()
        for row in rows:
            counts[row["status"]] = row["c"]
        return counts

    def current_task(self) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT task_type, status, source_path, source_type, progress_current, progress_total,
                       progress_percent, started_at, message
                FROM tasks
                WHERE status = 'running'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return {
                "task_type": "idle",
                "phase": "waiting",
                "source": "-",
                "current_file": "-",
                "progress_current": 0,
                "progress_total": 0,
                "progress_percent": 0,
                "started_at": None,
                "updated_at": utc_now(),
                "message": "",
            }
        return {
            "task_type": row["task_type"],
            "phase": row["status"],
            "source": row["source_type"] or "-",
            "current_file": row["source_path"],
            "progress_current": row["progress_current"],
            "progress_total": row["progress_total"],
            "progress_percent": row["progress_percent"],
            "started_at": row["started_at"],
            "updated_at": utc_now(),
            "message": row["message"] or "",
        }

    def update_directory_scan_status(
        self, watched_directory_id: int, status: str, error: str | None = None
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE watched_directories
                SET last_scan_at = ?, last_status = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), status, error, utc_now(), watched_directory_id),
            )

    def upsert_file_record(
        self,
        watched_directory_id: int,
        path: str,
        source_type: str,
        collection_name: str,
        parser_profiles: str,
        mtime: float,
        size: int,
        status: str,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO files (
                    watched_directory_id, source_path, resolved_path, source_type, collection_name, parser_profiles, mtime, size,
                    status, last_seen_at, file_ingest_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    resolved_path = excluded.resolved_path,
                    source_type = excluded.source_type,
                    collection_name = excluded.collection_name,
                    parser_profiles = excluded.parser_profiles,
                    mtime = excluded.mtime,
                    size = excluded.size,
                    status = excluded.status,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    watched_directory_id,
                    path,
                    path,
                    source_type,
                    collection_name,
                    parser_profile_string(parser_profiles),
                    mtime,
                    size,
                    status,
                    now,
                    file_ingest_id(path),
                    now,
                    now,
                ),
            )

    def upsert_file_and_queue_task(
        self,
        watched_directory_id: int,
        path: str,
        source_type: str,
        collection_name: str,
        parser_profiles: str,
        mtime: float,
        size: int,
        changed: bool,
    ) -> bool:
        now = utc_now()
        file_status = "pending" if changed else "indexed"
        queued = False
        task_type = "reindex_mbox" if source_type == "thunderbird" else "index_file"
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO files (
                    watched_directory_id, source_path, resolved_path, source_type, collection_name, parser_profiles, mtime, size,
                    status, last_seen_at, file_ingest_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    watched_directory_id = excluded.watched_directory_id,
                    resolved_path = excluded.resolved_path,
                    source_type = excluded.source_type,
                    collection_name = excluded.collection_name,
                    parser_profiles = excluded.parser_profiles,
                    mtime = excluded.mtime,
                    size = excluded.size,
                    status = CASE
                        WHEN excluded.status = 'pending' THEN 'pending'
                        ELSE files.status
                    END,
                    summary_text = CASE
                        WHEN excluded.status = 'pending' THEN NULL
                        ELSE files.summary_text
                    END,
                    summary_model = CASE
                        WHEN excluded.status = 'pending' THEN NULL
                        ELSE files.summary_model
                    END,
                    summary_updated_at = CASE
                        WHEN excluded.status = 'pending' THEN NULL
                        ELSE files.summary_updated_at
                    END,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    watched_directory_id,
                    path,
                    path,
                    source_type,
                    collection_name,
                    parser_profile_string(parser_profiles),
                    mtime,
                    size,
                    file_status,
                    now,
                    file_ingest_id(path),
                    now,
                    now,
                ),
            )
            if changed:
                existing = conn.execute(
                    """
                    SELECT id FROM tasks
                    WHERE task_type = ? AND source_path = ? AND status IN ('pending', 'running')
                    """,
                    (task_type, path),
                ).fetchone()
                if not existing:
                    conn.execute(
                        """
                        INSERT INTO tasks(task_type, source_path, source_type, task_priority, status, message, created_at)
                        VALUES (?, ?, ?, ?, 'pending', ?, ?)
                        """,
                        (task_type, path, source_type, task_priority_for(task_type, path, source_type), "file changed", now),
                    )
                    queued = True
        return queued

    def get_file_record(self, path: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM files WHERE source_path = ?", (path,)).fetchone()

    def get_file_summaries(self, paths: list[str]) -> dict[str, dict[str, Any]]:
        clean_paths = [path for path in paths if path]
        if not clean_paths:
            return {}
        placeholders = ",".join("?" for _ in clean_paths)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT source_path, summary_text, summary_model, summary_updated_at
                FROM files
                WHERE source_path IN ({placeholders})
                """,
                tuple(clean_paths),
            ).fetchall()
        return {
            str(row["source_path"]): {
                "summary_text": row["summary_text"] or "",
                "summary_model": row["summary_model"] or "",
                "summary_updated_at": row["summary_updated_at"] or "",
            }
            for row in rows
        }

    def mark_file_deleted(self, path: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = 'deleted', updated_at = ?, last_seen_at = ?
                WHERE source_path = ?
                """,
                (utc_now(), utc_now(), path),
            )

    def files_for_directory(self, watched_directory_id: int) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT source_path FROM files WHERE watched_directory_id = ? AND status != 'deleted'",
                (watched_directory_id,),
            ).fetchall()
        return [row["source_path"] for row in rows]

    def mark_file_indexed(self, path: str, digest: str, points_count: int = 0) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = 'indexed', content_hash = ?, last_indexed_at = ?, updated_at = ?,
                    qdrant_points_count = ?, last_error = NULL
                WHERE source_path = ?
                """,
                (digest, utc_now(), utc_now(), points_count, path),
            )

    def update_file_summary(self, path: str, summary_text: str, summary_model: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET summary_text = ?, summary_model = ?, summary_updated_at = ?, updated_at = ?
                WHERE source_path = ?
                """,
                (summary_text, summary_model, utc_now(), utc_now(), path),
            )

    def mark_file_error(self, path: str, error: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = 'error', last_error = ?, updated_at = ?
                WHERE source_path = ?
                """,
                (error, utc_now(), path),
            )

    def retry_failed_files(self, limit: int = FAILED_RETRY_BATCH_SIZE, max_attempts: int = FAILED_RETRY_MAX_ATTEMPTS) -> int:
        queued = 0
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT f.source_path, f.source_type
                FROM files f
                JOIN watched_directories w ON w.id = f.watched_directory_id
                WHERE f.status = 'error'
                  AND w.enabled = 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tasks t
                      WHERE t.source_path = f.source_path
                        AND t.task_type IN ('index_file', 'reindex_mbox')
                        AND t.status IN ('pending', 'running')
                  )
                  AND (
                      SELECT COUNT(*)
                      FROM tasks t2
                      WHERE t2.source_path = f.source_path
                        AND t2.task_type IN ('index_file', 'reindex_mbox')
                        AND t2.status = 'failed'
                  ) < ?
                ORDER BY f.updated_at ASC, f.id ASC
                LIMIT ?
                """,
                (max_attempts, limit),
            ).fetchall()

            for row in rows:
                task_type = "reindex_mbox" if row["source_type"] == "thunderbird" else "index_file"
                conn.execute(
                    """
                    UPDATE files
                    SET status = 'pending', summary_text = NULL, summary_model = NULL, summary_updated_at = NULL, updated_at = ?
                    WHERE source_path = ?
                    """,
                    (utc_now(), row["source_path"]),
                )
                conn.execute(
                    """
                    INSERT INTO tasks(task_type, source_path, source_type, task_priority, status, message, created_at)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        task_type,
                        row["source_path"],
                        row["source_type"],
                        task_priority_for(task_type, row["source_path"], row["source_type"]),
                        "retry failed file",
                        utc_now(),
                    ),
                )
                queued += 1
        return queued

    def requeue_orphan_pending_files(self, limit: int = 25) -> int:
        queued = 0
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT f.source_path, f.source_type
                FROM files f
                JOIN watched_directories w ON w.id = f.watched_directory_id
                WHERE f.status = 'pending'
                  AND w.enabled = 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tasks t
                      WHERE t.source_path = f.source_path
                        AND t.task_type IN ('index_file', 'reindex_mbox', 'delete_file')
                        AND t.status IN ('pending', 'running')
                  )
                ORDER BY f.id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        for row in rows:
            task_type = "reindex_mbox" if row["source_type"] == "thunderbird" else "index_file"
            self.queue_task(task_type, row["source_path"], row["source_type"], "requeued orphan pending file")
            queued += 1
        return queued


class Service:
    def __init__(self, storage: Storage, emit_init_event: bool = True) -> None:
        self.storage = storage
        self.lock = threading.Lock()
        self._embed_rr_index = 0
        self.qdrant_ok = False
        self.qdrant_collections: list[str] = []
        self.ollama_models: list[str] = [LLM_MODEL]
        self._gpu_snapshot: dict[str, Any] = {"ok": False}
        self._gpu_snapshot_ts = 0.0
        if emit_init_event:
            self.storage.add_event("info", "service", "rag-service initialized")

    def is_paused(self) -> bool:
        return self.storage.get_setting("paused", "0") == "1"

    def speed_ratio(self) -> float:
        try:
            return max(0.1, min(1.0, float(self.storage.get_setting("speed_ratio", "1.0"))))
        except ValueError:
            return 1.0

    def set_speed(self, value: int) -> None:
        ratio = max(10, min(100, value)) / 100.0
        self.storage.set_setting("speed_ratio", str(ratio))
        self.storage.add_event("info", "control", f"speed set to {int(ratio * 100)}%")

    def set_paused(self, paused: bool) -> None:
        self.storage.set_setting("paused", "1" if paused else "0")
        self.storage.add_event("info", "control", "service paused" if paused else "service resumed")

    def configured_ingest_worker_count(self) -> int:
        try:
            return max(1, min(32, int(self.storage.get_setting("ingest_worker_count", str(INGEST_WORKER_COUNT)))))
        except ValueError:
            return INGEST_WORKER_COUNT

    def configured_scan_worker_count(self) -> int:
        try:
            return max(1, min(16, int(self.storage.get_setting("scan_worker_count", str(SCAN_WORKER_COUNT)))))
        except ValueError:
            return SCAN_WORKER_COUNT

    def configured_embed_batch_size(self) -> int:
        try:
            return max(1, min(256, int(self.storage.get_setting("embed_batch_size", str(EMBED_BATCH_SIZE)))))
        except ValueError:
            return EMBED_BATCH_SIZE

    def configured_embed_parallel_requests(self) -> int:
        try:
            return max(1, min(16, int(self.storage.get_setting("embed_parallel_requests", str(EMBED_PARALLEL_REQUESTS)))))
        except ValueError:
            return EMBED_PARALLEL_REQUESTS

    def active_ingest_worker_count(self) -> int:
        return sum(
            1
            for process in WORKER_PROCESSES
            if getattr(process, "pid", None) and getattr(process, "name", "").startswith("ingest-worker")
        )

    def active_scan_worker_count(self) -> int:
        return sum(
            1
            for process in WORKER_PROCESSES
            if getattr(process, "pid", None) and getattr(process, "name", "").startswith("scan-worker")
        )

    def set_ingest_worker_count(self, value: int) -> int:
        count = max(1, min(32, int(value)))
        self.storage.set_setting("ingest_worker_count", str(count))
        self.storage.add_event("info", "control", f"ingest worker count set to {count} (restart required)")
        return count

    def set_scan_worker_count(self, value: int) -> int:
        count = max(1, min(16, int(value)))
        self.storage.set_setting("scan_worker_count", str(count))
        self.storage.add_event("info", "control", f"scan worker count set to {count} (restart required)")
        return count

    def set_embed_batch_size(self, value: int) -> int:
        count = max(1, min(256, int(value)))
        self.storage.set_setting("embed_batch_size", str(count))
        self.storage.add_event("info", "control", f"embed batch size set to {count}")
        return count

    def set_embed_parallel_requests(self, value: int) -> int:
        count = max(1, min(16, int(value)))
        self.storage.set_setting("embed_parallel_requests", str(count))
        self.storage.add_event("info", "control", f"embed parallel requests set to {count}")
        return count

    def configured_embed_servers(self) -> list[dict[str, Any]]:
        raw = self.storage.get_setting("embed_servers_json", json.dumps(DEFAULT_EMBED_SERVERS))
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = DEFAULT_EMBED_SERVERS
        servers: list[dict[str, Any]] = []
        for index in range(4):
            item = parsed[index] if index < len(parsed) and isinstance(parsed[index], dict) else {}
            address = str(item.get("address", "") or "").strip()
            try:
                weight = max(1, min(100, int(item.get("weight", 1) or 1)))
            except (TypeError, ValueError):
                weight = 1
            servers.append({"address": address, "weight": weight})
        return servers

    def set_embed_servers(self, servers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index in range(4):
            item = servers[index] if index < len(servers) and isinstance(servers[index], dict) else {}
            address = str(item.get("address", "") or "").strip()
            try:
                weight = max(1, min(100, int(item.get("weight", 1) or 1)))
            except (TypeError, ValueError):
                weight = 1
            normalized.append({"address": address, "weight": weight})
        self.storage.set_setting("embed_servers_json", json.dumps(normalized))
        self.storage.add_event("info", "control", "embedding servers updated")
        return normalized

    def configured_llm_server_address(self) -> str:
        return str(self.storage.get_setting("llm_server_address", DEFAULT_LLM_SERVER_ADDRESS)).strip()

    def set_llm_server_address(self, address: str) -> str:
        normalized = str(address or "").strip()
        self.storage.set_setting("llm_server_address", normalized)
        self.storage.add_event("info", "control", f"llm server updated to {normalized or 'local'}")
        return normalized

    def _embed_url_from_address(self, address: str) -> str:
        base_url = normalize_ollama_base_url(address)
        if not base_url:
            return ""
        parsed = urlparse(base_url)
        netloc = parsed.netloc or parsed.path
        base_path = parsed.path.rstrip("/")
        if base_path.endswith("/api/embed"):
            return f"{parsed.scheme}://{netloc}{base_path}"
        return f"{parsed.scheme}://{netloc}{base_path}/api/embed"

    def configured_generate_url(self) -> str:
        base_url = normalize_ollama_base_url(self.configured_llm_server_address())
        return f"{base_url}/api/generate"

    def configured_llm_tags_url(self) -> str:
        base_url = normalize_ollama_base_url(self.configured_llm_server_address())
        return f"{base_url}/api/tags"

    def choose_embed_url(self) -> str:
        servers = self.configured_embed_servers()
        weighted_urls: list[str] = []
        for server in servers:
            url = self._embed_url_from_address(server.get("address", ""))
            if not url:
                continue
            weighted_urls.extend([url] * max(1, int(server.get("weight", 1) or 1)))
        if not weighted_urls:
            return OLLAMA_EMBED_URL
        with self.lock:
            url = weighted_urls[self._embed_rr_index % len(weighted_urls)]
            self._embed_rr_index += 1
        return url

    def refresh_qdrant(self) -> None:
        try:
            with urlopen(QDRANT_URL, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            names = [item["name"] for item in payload.get("result", {}).get("collections", [])]
            with self.lock:
                became_online = not self.qdrant_ok
                self.qdrant_ok = True
                self.qdrant_collections = names
            if became_online:
                self.storage.add_event("info", "qdrant", "qdrant reachable on port 6333")
        except (URLError, OSError, TimeoutError, json.JSONDecodeError) as exc:
            with self.lock:
                was_online = self.qdrant_ok
                self.qdrant_ok = False
                self.qdrant_collections = []
            if was_online:
                self.storage.add_event("warn", "qdrant", f"qdrant unreachable: {exc}")

    def refresh_ollama_models(self) -> None:
        try:
            models = list_ollama_models()
            if not models:
                models = [LLM_MODEL]
        except Exception:
            models = [LLM_MODEL]
        with self.lock:
            self.ollama_models = models

    def gpu_snapshot(self, proc_snapshot: dict[str, Any]) -> dict[str, Any]:
        now = time.monotonic()
        with self.lock:
            if now - self._gpu_snapshot_ts <= GPU_SNAPSHOT_TTL_SECONDS:
                return dict(self._gpu_snapshot)
        gpu = query_gpu_runtime()
        ollama_runtime = query_ollama_runtime(proc_snapshot)
        snapshot = {
            **gpu,
            **ollama_runtime,
        }
        with self.lock:
            self._gpu_snapshot = snapshot
            self._gpu_snapshot_ts = now
        return dict(snapshot)

    def threegpp_plan(self) -> dict[str, Any]:
        rows = self.storage.threegpp_file_rows()
        return build_3gpp_plan(rows)

    def jira_plan(self) -> dict[str, Any]:
        rows = self.storage.jira_file_rows()
        return build_jira_plan(rows)

    def is_latest_3gpp_file(self, path: str) -> bool:
        info = classify_3gpp_path(path)
        spec_id = info.get("spec_id") or ""
        if not spec_id:
            return False
        latest_by_spec = self.threegpp_plan().get("latest_by_spec", {})
        return latest_by_spec.get(spec_id) == path

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            qdrant_ok = self.qdrant_ok
            qdrant_collections = list(self.qdrant_collections)
            ollama_models = list(self.ollama_models)
        proc_snapshot = read_proc_snapshot()
        gpu_runtime = self.gpu_snapshot(proc_snapshot)
        threegpp_plan = self.threegpp_plan()
        jira_plan = self.jira_plan()
        worker_runtime = {int(item["pid"]): item for item in self.storage.worker_runtime_rows()}
        worker_processes = [
            {
                **process_runtime_info(process.pid, getattr(process, "name", "worker"), proc_snapshot),
                **worker_runtime.get(process.pid, {}),
                "step_age_seconds": seconds_since_iso(worker_runtime.get(process.pid, {}).get("updated_at")),
            }
            for process in WORKER_PROCESSES
            if getattr(process, "pid", None)
        ]
        main_process = {
            **process_runtime_info(os.getpid(), "web", proc_snapshot),
            **worker_runtime.get(os.getpid(), {}),
            "step_age_seconds": seconds_since_iso(worker_runtime.get(os.getpid(), {}).get("updated_at")),
        }
        return {
            "service": {
                "name": "rag-service",
                "host": HOST,
                "port": PORT,
                "started_at": utc_now(),
                "paused": self.is_paused(),
                "speed_ratio": self.speed_ratio(),
                "speed_percent": int(self.speed_ratio() * 100),
                "configured_ingest_workers": self.configured_ingest_worker_count(),
                "active_ingest_workers": self.active_ingest_worker_count(),
                "configured_scan_workers": self.configured_scan_worker_count(),
                "configured_embed_batch_size": self.configured_embed_batch_size(),
                "configured_embed_parallel_requests": self.configured_embed_parallel_requests(),
                "embed_servers": self.configured_embed_servers(),
                "llm_server_address": self.configured_llm_server_address(),
                "active_scan_workers": self.active_scan_worker_count(),
                "worker_restart_required": (
                    self.active_ingest_worker_count() != self.configured_ingest_worker_count()
                    or self.active_scan_worker_count() != self.configured_scan_worker_count()
                ),
            },
            "task": self.storage.current_task(),
            "task_counts": self.storage.get_task_counts(),
            "file_counts": self.storage.get_file_counts(),
            "recent_tasks": self.storage.recent_tasks(),
            "qdrant": {
                "url": QDRANT_URL,
                "ok": qdrant_ok,
                "collections": qdrant_collections,
            },
            "ollama": {
                "default_model": LLM_MODEL,
                "models": ollama_models,
                "runtime": gpu_runtime,
            },
            "runtime": {
                "main_process": main_process,
                "worker_processes": worker_processes,
            },
            "planning": {
                "threegpp": {
                    "summary": threegpp_plan["summary"],
                    "specs": threegpp_plan["specs"],
                    "next_tasks": self.storage.planned_tasks(30),
                },
                "jira": {
                    "summary": jira_plan["summary"],
                    "projects": jira_plan["projects"],
                },
            },
            "watched_dirs": self.storage.get_watched_dirs(),
            "logs": self.storage.recent_events(),
            "db_overview": self.storage.db_overview(),
        }

    def _search_results(
        self,
        query: str,
        limit: int,
        collection_name: str = "",
        source_type: str = "",
    ) -> list[dict[str, Any]]:
        embeddings, _ = embed_texts([query])
        chosen_collection = (collection_name or "").strip()
        chosen_source_type = (source_type or "").strip()
        if chosen_collection and chosen_collection.lower() != "all":
            collection_names = [chosen_collection]
        else:
            collection_names = self.storage.list_collection_names()
        per_collection_limit = max(limit, min(50, limit * 5))
        results = []
        for current_collection_name in collection_names:
            try:
                results.extend(search_qdrant(current_collection_name, embeddings[0], limit=per_collection_limit))
            except HTTPError as exc:
                if exc.code != 404:
                    raise
        if chosen_source_type and chosen_source_type.lower() != "all":
            results = [
                item
                for item in results
                if str((item.get("payload") or {}).get("source_type", "")).strip() == chosen_source_type
            ]
        results.sort(key=lambda item: item.get("score", 0), reverse=True)
        return results[:limit]

    def search(
        self,
        query: str,
        limit: int = SEARCH_TOP_K,
        collection_name: str = "",
        source_type: str = "",
    ) -> dict[str, Any]:
        query = (query or "").strip()
        if not query:
            return {"query": query, "results": []}
        results = self._search_results(query, limit, collection_name=collection_name, source_type=source_type)
        formatted = self._format_search_results(results)
        self.storage.add_event("info", "search", f"search query executed: {query[:80]}")
        return {
            "query": query,
            "results": formatted,
            "collection_name": (collection_name or "").strip(),
            "source_type": (source_type or "").strip(),
        }

    def ask(
        self,
        query: str,
        limit: int = SEARCH_TOP_K,
        model: str | None = None,
        detail_level: str = "standard",
        answer_language: str = "fr",
        collection_name: str = "",
        source_type: str = "",
    ) -> dict[str, Any]:
        query = (query or "").strip()
        if not query:
            return {"query": query, "answer": "", "results": []}
        chosen_model = (model or LLM_MODEL).strip() or LLM_MODEL
        detail_level = (detail_level or "standard").strip().lower()
        if detail_level not in {"standard", "deep"}:
            detail_level = "standard"
        answer_language = (answer_language or "fr").strip().lower()
        if answer_language not in {"fr", "en"}:
            answer_language = "fr"
        results = self._search_results(query, limit, collection_name=collection_name, source_type=source_type)
        formatted = self._format_search_results(results)
        if not formatted:
            return {"query": query, "answer": "No relevant context found.", "results": []}

        context_blocks = []
        for idx, item in enumerate(formatted, start=1):
            summary_text = (item.get("summary_text") or "").strip()
            context_blocks.append(
                "\n".join(
                    [line for line in [
                        f"[Source {idx}]",
                        f"collection: {item['collection_name']}",
                        f"path: {item['source_path']}",
                        f"zip entry: {item.get('zip_entry_path') or '-'}",
                        f"type: {item['source_type']}",
                        f"spec: {item.get('spec_id') or '-'} {item.get('spec_version') or ''} {item.get('spec_release') or ''}".strip(),
                        f"clause: {item.get('clause_id') or '-'} {item.get('clause_title') or ''}".strip(),
                        f"issue: {item.get('issue_key') or '-'}",
                        f"issue title: {item.get('issue_title') or '-'}",
                        f"status: {item.get('issue_status') or '-'}",
                        f"fix version: {item.get('fix_version') or '-'}",
                        f"feature: {item.get('issue_feature') or '-'}",
                        f"chunk: {item['chunk_index']}",
                        f"document summary:\n{summary_text}" if summary_text else "",
                        item["chunk_text"],
                    ] if line]
                )
            )

        response_style = (
            "Write a clear, precise, moderately detailed answer. Explain the mechanism, not just the conclusion."
            if detail_level == "standard"
            else "Write a clear, precise, in-depth answer. Explain how it works step by step, connect the relevant parts, and include important nuances."
        )
        language_instruction = (
            "Write the final answer in French."
            if answer_language == "fr"
            else "Write the final answer in English."
        )
        prompt = (
            "You answer from a local knowledge base.\n"
            "Use only the provided context.\n"
            "If the context is insufficient, say exactly what is missing.\n"
            "Do not invent facts, clauses, releases, procedures, APIs, or behaviors.\n"
            f"{language_instruction}\n"
            f"{response_style}\n"
            "Structure the answer using these sections when relevant:\n"
            "1. Direct answer\n"
            "2. Explanation\n"
            "3. Important details or implications\n"
            "4. Sources used\n"
            "Use inline citations in the body whenever you make a factual statement, like [1] or [2][3].\n"
            "Do not wait until the end to cite the evidence.\n"
            "In 'Sources used', list only the source numbers actually used, like [1], [2].\n"
            "If versions or releases differ, say so explicitly.\n\n"
            f"Requested detail level: {detail_level}\n\n"
            f"Question:\n{query}\n\n"
            "Context:\n"
            + "\n\n".join(context_blocks)
            + "\n\nAnswer:"
        )
        answer = generate_answer(prompt, chosen_model, generate_url=self.configured_generate_url())
        self.storage.add_event("info", "search", f"llm answer generated: {query[:80]}")
        return {
            "query": query,
            "answer": answer,
            "results": formatted,
            "model": chosen_model,
            "detail_level": detail_level,
            "answer_language": answer_language,
            "collection_name": (collection_name or "").strip(),
            "source_type": (source_type or "").strip(),
        }

    def _format_search_results(self, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        summaries = self.storage.get_file_summaries(
            [str((item.get("payload") or {}).get("source_path", "")) for item in results]
        )
        formatted = []
        for item in results:
            payload = item.get("payload") or {}
            summary = summaries.get(str(payload.get("source_path", "")), {})
            formatted.append(
                {
                    "score": item.get("score"),
                    "source_path": payload.get("source_path", ""),
                    "source_type": payload.get("source_type", ""),
                    "collection_name": payload.get("collection_name", ""),
                    "file_name": payload.get("file_name", ""),
                    "zip_entry_path": payload.get("zip_entry_path", ""),
                    "zip_entry_name": payload.get("zip_entry_name", ""),
                    "spec_id": payload.get("spec_id", ""),
                    "spec_version": payload.get("spec_version", ""),
                    "spec_release": payload.get("spec_release", ""),
                    "spec_stage": payload.get("spec_stage", ""),
                    "clause_id": payload.get("clause_id", ""),
                    "clause_title": payload.get("clause_title", ""),
                    "chunk_index": payload.get("chunk_index", 0),
                    "chunk_text": payload.get("chunk_text", ""),
                    "summary_text": summary.get("summary_text", ""),
                    "summary_model": summary.get("summary_model", ""),
                    "mailbox": payload.get("mailbox", ""),
                    "email_subject": payload.get("email_subject", ""),
                    "email_from": payload.get("email_from", ""),
                    "email_to": payload.get("email_to", ""),
                    "email_date": payload.get("email_date", ""),
                    "issue_key": payload.get("issue_key", ""),
                    "issue_title": payload.get("issue_title", ""),
                    "issue_type": payload.get("issue_type", ""),
                    "issue_status": payload.get("issue_status", ""),
                    "issue_priority": payload.get("issue_priority", ""),
                    "fix_version": payload.get("fix_version", ""),
                    "affects_version": payload.get("affects_version", ""),
                    "issue_area": payload.get("issue_area", ""),
                    "issue_feature": payload.get("issue_feature", ""),
                    "issue_component": payload.get("issue_component", ""),
                    "issue_labels": payload.get("issue_labels", ""),
                    "issue_sprint": payload.get("issue_sprint", ""),
                }
            )
        return formatted

    def retry_failed_files(self, limit: int = FAILED_RETRY_BATCH_SIZE) -> int:
        queued = self.storage.retry_failed_files(limit=limit)
        if queued:
            self.storage.add_event("info", "queue", f"manually requeued {queued} failed files")
        else:
            self.storage.add_event("info", "queue", "manual retry found no eligible failed files")
        return queued

    def schedule_scan_tasks(
        self,
        source_type: str | None = None,
        manual: bool = False,
        watched_path: str | None = None,
    ) -> None:
        now_ts = utc_ts()
        for item in self.storage.get_watched_dirs():
            if not item["enabled"]:
                continue
            if watched_path and item["path"] != watched_path:
                continue
            if source_type and item["source_type"] != source_type:
                continue
            if not manual and item["source_type"] not in AUTO_SCAN_SOURCE_TYPES:
                continue
            interval = item["scan_interval_seconds"] or 0
            last_scan_ts = parse_iso_ts(item.get("last_scan_at"))
            if not manual and interval > 0 and last_scan_ts and now_ts - last_scan_ts < interval:
                continue
            self.storage.queue_task("scan_directory", item["path"], item["source_type"], "manual scan" if manual else "scheduled scan")
        if watched_path:
            self.storage.add_event("info", "scan", f"scan task queued for {short_path(watched_path)}")
        else:
            self.storage.add_event("info", "scan", "scan tasks queued")

    def _walk_and_queue_directory_files(self, task: sqlite3.Row, item: dict[str, Any], watched_path: Path) -> tuple[int, int, set[str]]:
        total_seen = 0
        kept_count = 0
        queued_count = 0
        seen_paths: set[str] = set()
        parser_profiles = item.get("parser_profiles") or default_parser_profiles_for_source(item["source_type"])

        for dirpath, dirnames, filenames in os_walk_safe(watched_path):
            dirnames[:] = [name for name in dirnames if name not in EXCLUDED_DIR_NAMES]
            for filename in filenames:
                candidate = Path(dirpath) / filename
                total_seen += 1

                if not should_keep(candidate, item["source_type"], parser_profiles):
                    if total_seen % 50 == 0:
                        self.storage.update_task_progress(
                            task["id"],
                            total_seen,
                            0,
                            f"scanning {short_path(str(candidate))}",
                        )
                        self.storage.update_worker_runtime(
                            pid=os.getpid(),
                            role=multiprocessing.current_process().name,
                            task_type=task["task_type"],
                            current_file=str(candidate),
                            message=f"scanning {short_path(str(candidate))}",
                            progress_current=total_seen,
                            progress_total=0,
                            progress_percent=0,
                        )
                    if total_seen % SCAN_YIELD_EVERY == 0:
                        time.sleep(SCAN_SLEEP_SECONDS * (1 / self.speed_ratio()))
                    continue

                try:
                    stat = candidate.stat()
                except FileNotFoundError:
                    continue

                kept_count += 1
                path_str = str(candidate)
                seen_paths.add(path_str)
                previous = self.storage.get_file_record(path_str)
                previous_mtime = previous["mtime"] if previous else None
                previous_size = previous["size"] if previous else None
                changed = previous is None or previous_mtime != stat.st_mtime or previous_size != stat.st_size
                queued = self.storage.upsert_file_and_queue_task(
                    watched_directory_id=item["id"],
                    path=path_str,
                    source_type=item["source_type"],
                    collection_name=item.get("collection_name") or VECTOR_COLLECTION,
                    parser_profiles=parser_profiles,
                    mtime=stat.st_mtime,
                    size=stat.st_size,
                    changed=changed,
                )
                if queued:
                    queued_count += 1

                if kept_count % 10 == 0 or changed:
                    self.storage.update_task_progress(
                        task["id"],
                        kept_count,
                        0,
                        f"queued {queued_count} after {short_path(path_str)}",
                    )
                    self.storage.update_worker_runtime(
                        pid=os.getpid(),
                        role=multiprocessing.current_process().name,
                        task_type=task["task_type"],
                        current_file=path_str,
                        message=f"queued {queued_count} after {short_path(path_str)}",
                        progress_current=kept_count,
                        progress_total=0,
                        progress_percent=0,
                    )
                if total_seen % SCAN_YIELD_EVERY == 0:
                    time.sleep(SCAN_SLEEP_SECONDS * (1 / self.speed_ratio()))

        return kept_count, queued_count, seen_paths

    def worker_loop(self, worker_name: str, allowed_task_types: set[str], schedule_scans: bool = False) -> None:
        is_scan_only_worker = allowed_task_types == {"scan_directory"}
        self.storage.update_worker_runtime(
            pid=os.getpid(),
            role=worker_name,
            task_type="idle",
            current_file="-",
            message="waiting",
        )
        while True:
            if self.is_paused():
                self.storage.update_worker_runtime(
                    pid=os.getpid(),
                    role=worker_name,
                    task_type="paused",
                    current_file="-",
                    message="paused",
                )
                time.sleep(SCAN_IDLE_SLEEP_SECONDS if is_scan_only_worker else 1)
                continue

            try:
                task = self.storage.next_task(allowed_task_types)
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower():
                    self.storage.update_worker_runtime(
                        pid=os.getpid(),
                        role=worker_name,
                        task_type="idle",
                        current_file="-",
                        message="waiting for database lock",
                    )
                    time.sleep(SCAN_IDLE_SLEEP_SECONDS if is_scan_only_worker else 1)
                    continue
                raise
            if task is None:
                self.storage.update_worker_runtime(
                    pid=os.getpid(),
                    role=worker_name,
                    task_type="idle",
                    current_file="-",
                    message="waiting",
                )
                time.sleep(SCAN_IDLE_SLEEP_SECONDS if is_scan_only_worker else 1)
                continue

            self.storage.update_worker_runtime(
                pid=os.getpid(),
                role=worker_name,
                task_type=task["task_type"],
                current_file=task["source_path"],
                message=task["message"] or "",
                progress_current=task["progress_current"],
                progress_total=task["progress_total"],
                progress_percent=task["progress_percent"],
            )
            try:
                if task["task_type"] == "scan_directory":
                    self._run_scan_directory(task)
                elif task["task_type"] == "index_file":
                    self._run_index_file(task)
                elif task["task_type"] == "summarize_file":
                    self._run_summarize_file(task)
                elif task["task_type"] == "reindex_mbox":
                    self._run_reindex_mbox(task)
                elif task["task_type"] == "delete_file":
                    self._run_delete_file(task)
                else:
                    self.storage.finish_task(task["id"], "ignored unknown task")
            except Exception as exc:
                self.storage.fail_task(task["id"], str(exc))
                self.storage.add_event("error", "task", f"{worker_name}:{task['task_type']} failed: {exc}")
            finally:
                self.storage.update_worker_runtime(
                    pid=os.getpid(),
                    role=worker_name,
                    task_type="idle",
                    current_file="-",
                    message="waiting",
                )

    def _run_scan_directory(self, task: sqlite3.Row) -> None:
        item = next((entry for entry in self.storage.get_watched_dirs() if entry["path"] == task["source_path"]), None)
        if item is None:
            self.storage.finish_task(task["id"], "directory no longer configured")
            return

        watched_path = Path(item["path"])
        if not watched_path.exists():
            self.storage.update_directory_scan_status(item["id"], "error", "path does not exist")
            self.storage.fail_task(task["id"], "path does not exist")
            return

        is_manual_scan = (task["message"] or "").startswith("manual scan")
        if item["source_type"] == "thunderbird" and not is_manual_scan:
            reason = "manual scan only until mailbox parser tuning is complete"
            self.storage.update_directory_scan_status(item["id"], "paused", reason)
            self.storage.finish_task(task["id"], f"{item['source_type']} auto-scan disabled: {reason}")
            return
        if item["source_type"] == "documents" and not is_manual_scan:
            reason = "manual scan only until filtering is improved"
            self.storage.update_directory_scan_status(item["id"], "paused", reason)
            self.storage.finish_task(task["id"], f"{item['source_type']} auto-scan disabled: {reason}")
            return

        self.storage.update_worker_runtime(
            pid=os.getpid(),
            role=multiprocessing.current_process().name,
            task_type=task["task_type"],
            current_file=str(watched_path),
            message=f"walking {short_path(str(watched_path))}",
            progress_current=0,
            progress_total=0,
            progress_percent=0,
        )

        kept_count, queued_count, seen_paths = self._walk_and_queue_directory_files(task, item, watched_path)

        for existing in self.storage.files_for_directory(item["id"]):
            if existing not in seen_paths:
                self.storage.mark_file_deleted(existing)
                self.storage.queue_task("delete_file", existing, item["source_type"], "file removed")

        self.storage.update_directory_scan_status(item["id"], "ok")
        self.storage.finish_task(task["id"], f"scan complete: {kept_count} files kept, {queued_count} queued")
        self.storage.add_event("info", "scan", f"scan complete for {item['source_type']}: {kept_count} files kept, {queued_count} queued")

    def _run_index_file(self, task: sqlite3.Row) -> None:
        if task["source_type"] == "thunderbird":
            self._run_reindex_mbox(task)
            return

        work_started = time.monotonic()

        current_embed_server = ""

        def update_worker(message: str, current: int = 0, total: int = 0, percent: int = 0) -> None:
            self.storage.update_worker_runtime(
                pid=os.getpid(),
                role=multiprocessing.current_process().name,
                task_type=task["task_type"],
                current_file=task["source_path"],
                message=message,
                embed_server=current_embed_server,
                progress_current=current,
                progress_total=total,
                progress_percent=percent,
            )

        def throttle_after_file() -> None:
            sleep_time = throttle_sleep_duration(time.monotonic() - work_started, self.speed_ratio())
            if sleep_time > 0:
                update_worker(f"throttling {sleep_time:.2f}s", 0, 0, 0)
                time.sleep(sleep_time)

        path = Path(task["source_path"])
        file_row = self.storage.get_file_record(str(path))
        collection_name = (file_row["collection_name"] if file_row and file_row["collection_name"] else VECTOR_COLLECTION)
        parser_profiles = (
            file_row["parser_profiles"]
            if file_row and file_row["parser_profiles"]
            else default_parser_profiles_for_source(task["source_type"] or "")
        )
        self.storage.update_task_progress(task["id"], 0, 6, "checking file")
        update_worker("checking file", 0, 6, 0)
        self.storage.update_task_progress(task["id"], 1, 6, "reading file")
        update_worker("reading file", 1, 6, 16)
        try:
            digest, text = read_file_payload_with_timeout(path, parser_profiles)
        except FileNotFoundError:
            self.storage.fail_task(task["id"], "file missing")
            self.storage.mark_file_error(str(path), "file missing")
            throttle_after_file()
            return
        except TimeoutError as exc:
            message = f"file read timeout: {exc}"
            self.storage.fail_task(task["id"], message)
            self.storage.mark_file_error(str(path), message)
            self.storage.add_event("warn", "index", f"timeout on {short_path(str(path))}")
            throttle_after_file()
            return
        except OSError as exc:
            self.storage.fail_task(task["id"], f"file read failed: {exc}")
            self.storage.mark_file_error(str(path), f"file read failed: {exc}")
            throttle_after_file()
            return

        source_type = task["source_type"] or ""
        source_metadata_jira: dict[str, Any] = {}
        profiles = set(normalize_parser_profiles(parser_profiles))
        if source_type == "jira" or "jira_issue" in profiles:
            text, source_metadata_jira = parse_jira_document(path, text)
        if not text:
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "empty text file")
            self.storage.add_event("warn", "index", f"empty text for {short_path(str(path))}")
            throttle_after_file()
            return

        self.storage.update_task_progress(task["id"], 2, 6, "preparing chunks")
        update_worker("preparing chunks", 2, 6, 33)
        if path.suffix.lower() == ".zip" and "archive_zip" in profiles:
            zip_entries = extract_zip_entries(path, source_type, parser_profiles)
            if not zip_entries:
                self.storage.mark_file_indexed(str(path), digest, points_count=0)
                self.storage.finish_task(task["id"], "no indexable files found in zip")
                throttle_after_file()
                return
            source_metadata = {}
            chunk_entries: list[dict[str, Any]] = []
            for zip_entry in zip_entries:
                entry_text = zip_entry["text"]
                entry_meta = (
                    parse_3gpp_filename_metadata(Path(zip_entry["inner_name"]), entry_text)
                    if source_type == "3gpp" or "3gpp_spec" in profiles
                    else {}
                )
                for chunk_entry in build_chunks_for_source(source_type, entry_text, parser_profiles):
                    chunk_entries.append(
                        {
                            **chunk_entry,
                            "zip_entry_path": zip_entry["inner_path"],
                            "zip_entry_name": zip_entry["inner_name"],
                            "entry_spec_id": entry_meta.get("spec_id", ""),
                            "entry_spec_version": entry_meta.get("spec_version", ""),
                            "entry_spec_release": entry_meta.get("spec_release", ""),
                            "entry_spec_stage": entry_meta.get("spec_stage"),
                        }
                    )
            summary_source_text = "\n\n".join(
                f"[ZIP ENTRY: {entry['inner_path']}]\n{entry['text']}" for entry in zip_entries[:20]
            )
        else:
            source_metadata = parse_3gpp_filename_metadata(path, text) if source_type == "3gpp" or "3gpp_spec" in profiles else {}
            chunk_entries = build_chunks_for_source(source_type, text, parser_profiles)
            summary_source_text = text
        if not chunk_entries:
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "no chunks produced")
            throttle_after_file()
            return

        self.storage.update_task_progress(task["id"], 3, 6, "requesting embeddings")
        update_worker("requesting embeddings", 3, 6, 50)
        chunk_texts = [entry["chunk_text"] for entry in chunk_entries]
        embed_batch_size = self.configured_embed_batch_size()
        embed_parallel_requests = self.configured_embed_parallel_requests()
        total_batches_hint = max(1, (len(chunk_texts) + embed_batch_size - 1) // embed_batch_size)

        def on_embed_batch_done(completed_batches: int, total_batches: int, batch_elapsed: float, embed_label: str) -> None:
            nonlocal current_embed_server
            current_embed_server = embed_label
            progress_current = 3 + completed_batches
            progress_total = 3 + total_batches + 2
            progress_message = f"embedded batches {completed_batches}/{total_batches} via {embed_label}"
            self.storage.update_task_progress(
                task["id"],
                progress_current,
                progress_total,
                progress_message,
            )
            update_worker(
                progress_message,
                progress_current,
                progress_total,
                int((progress_current / max(1, progress_total)) * 100),
            )

        all_embeddings, total_batches = embed_texts_parallel(
            chunk_texts,
            batch_size=embed_batch_size,
            parallel_requests=embed_parallel_requests,
            on_batch_done=on_embed_batch_done,
        )
        total_batches = max(total_batches, total_batches_hint)

        if len(all_embeddings) != len(chunk_entries):
            raise RuntimeError("embedding count mismatch")

        self.storage.update_task_progress(task["id"], 3 + total_batches, 3 + total_batches + 2, "syncing qdrant")
        update_worker("syncing qdrant", 3 + total_batches, 3 + total_batches + 2)
        ensure_qdrant_collection(collection_name, len(all_embeddings[0]))
        file_id = file_ingest_id(str(path))
        delete_qdrant_points_by_file(collection_name, file_id)

        points = []
        for idx, (chunk_entry, vector) in enumerate(zip(chunk_entries, all_embeddings)):
            point_id = int(hashlib.sha1(f"{file_id}:{idx}".encode("utf-8")).hexdigest()[:15], 16)
            points.append(
                {
                    "id": point_id,
                    "vector": vector,
                    "payload": {
                        "file_ingest_id": file_id,
                        "source_path": str(path),
                        "source_type": source_type,
                        "collection_name": collection_name,
                        "chunk_index": idx,
                        "chunk_text": chunk_entry["chunk_text"],
                        "file_name": path.name,
                        "clause_id": chunk_entry.get("clause_id", ""),
                        "clause_title": chunk_entry.get("clause_title", ""),
                        "zip_entry_path": chunk_entry.get("zip_entry_path", ""),
                        "zip_entry_name": chunk_entry.get("zip_entry_name", ""),
                        "spec_id": chunk_entry.get("entry_spec_id") or source_metadata.get("spec_id", ""),
                        "spec_version": chunk_entry.get("entry_spec_version") or source_metadata.get("spec_version", ""),
                        "spec_release": chunk_entry.get("entry_spec_release") or source_metadata.get("spec_release", ""),
                        "spec_stage": chunk_entry.get("entry_spec_stage") if chunk_entry.get("entry_spec_stage") is not None else source_metadata.get("spec_stage"),
                        "issue_key": source_metadata_jira.get("issue_key", ""),
                        "issue_title": source_metadata_jira.get("issue_title", ""),
                        "issue_type": source_metadata_jira.get("issue_type", ""),
                        "issue_status": source_metadata_jira.get("issue_status", ""),
                        "issue_priority": source_metadata_jira.get("issue_priority", ""),
                        "fix_version": source_metadata_jira.get("fix_version", ""),
                        "affects_version": source_metadata_jira.get("affects_version", ""),
                        "issue_area": source_metadata_jira.get("issue_area", ""),
                        "issue_feature": source_metadata_jira.get("issue_feature", ""),
                        "issue_component": source_metadata_jira.get("issue_component", ""),
                        "issue_labels": source_metadata_jira.get("issue_labels", ""),
                        "issue_sprint": source_metadata_jira.get("issue_sprint", ""),
                    },
                }
            )

        self.storage.update_task_progress(task["id"], 4 + total_batches, 3 + total_batches + 2, "uploading points")
        update_worker("uploading points", 4 + total_batches, 3 + total_batches + 2)
        upsert_qdrant_points(collection_name, points)
        self.storage.mark_file_indexed(str(path), digest, points_count=len(points))
        if source_type == "3gpp":
            if classify_3gpp_path(str(path))["is_mcptt"] and self.is_latest_3gpp_file(str(path)):
                self.storage.queue_task("summarize_file", str(path), source_type, "deep summary for latest MCPTT version")
            self.storage.finish_task(task["id"], f"fast indexed {len(points)} chunks")
        else:
            summary_embed_url = self.choose_embed_url()
            current_embed_server = compact_embed_server_label(summary_embed_url)
            summary_generate_url = generate_url_from_embed_url(summary_embed_url)
            self.storage.update_task_progress(task["id"], 5 + total_batches, 3 + total_batches + 3, "building document memory")
            update_worker(f"building document memory via {current_embed_server}", 5 + total_batches, 3 + total_batches + 3)
            try:
                summary_text = summarize_document_text(
                    summary_source_text,
                    source_path=str(path),
                    source_type=source_type,
                    model=SUMMARY_MODEL,
                    generate_url=summary_generate_url,
                ).strip()
                if summary_text:
                    self.storage.update_file_summary(str(path), summary_text, SUMMARY_MODEL)
            except Exception as exc:
                self.storage.add_event("warn", "summary", f"summary failed for {short_path(str(path))}: {exc}")
            self.storage.finish_task(task["id"], f"indexed {len(points)} chunks")
        self.storage.add_event("info", "index", f"indexed {short_path(str(path))} ({len(points)} chunks)")
        throttle_after_file()

    def _run_summarize_file(self, task: sqlite3.Row) -> None:
        current_embed_server = ""

        def update_worker(message: str, current: int = 0, total: int = 0, percent: int = 0) -> None:
            self.storage.update_worker_runtime(
                pid=os.getpid(),
                role=multiprocessing.current_process().name,
                task_type=task["task_type"],
                current_file=task["source_path"],
                message=message,
                embed_server=current_embed_server,
                progress_current=current,
                progress_total=total,
                progress_percent=percent,
            )

        path = Path(task["source_path"])
        file_row = self.storage.get_file_record(str(path))
        parser_profiles = (
            file_row["parser_profiles"]
            if file_row and file_row["parser_profiles"]
            else default_parser_profiles_for_source(task["source_type"] or "")
        )
        self.storage.update_task_progress(task["id"], 0, 3, "reading file for deep summary")
        update_worker("reading file for deep summary", 0, 3, 0)
        try:
            _, text = read_file_payload_with_timeout(path, parser_profiles)
        except FileNotFoundError:
            self.storage.fail_task(task["id"], "file missing")
            self.storage.mark_file_error(str(path), "file missing")
            return
        except TimeoutError as exc:
            self.storage.fail_task(task["id"], f"deep summary read timeout: {exc}")
            self.storage.add_event("warn", "summary", f"deep summary timeout on {short_path(str(path))}")
            return
        except OSError as exc:
            self.storage.fail_task(task["id"], f"deep summary read failed: {exc}")
            return

        profiles = set(normalize_parser_profiles(parser_profiles))
        if path.suffix.lower() == ".zip" and "archive_zip" in profiles:
            zip_entries = extract_zip_entries(path, task["source_type"] or "", parser_profiles)
            summary_source_text = "\n\n".join(
                f"[ZIP ENTRY: {entry['inner_path']}]\n{entry['text']}" for entry in zip_entries[:20]
            )
        else:
            summary_source_text = text

        if not summary_source_text.strip():
            self.storage.finish_task(task["id"], "no text available for deep summary")
            return

        summary_embed_url = self.choose_embed_url()
        current_embed_server = compact_embed_server_label(summary_embed_url)
        summary_generate_url = generate_url_from_embed_url(summary_embed_url)
        self.storage.update_task_progress(task["id"], 1, 3, "building deep summary")
        update_worker(f"building deep summary via {current_embed_server}", 1, 3, 33)
        summary_text = summarize_document_text(
            summary_source_text,
            source_path=str(path),
            source_type=task["source_type"] or "",
            model=SUMMARY_MODEL,
            generate_url=summary_generate_url,
        ).strip()
        if summary_text:
            self.storage.update_file_summary(str(path), summary_text, SUMMARY_MODEL)
        self.storage.update_task_progress(task["id"], 3, 3, "deep summary ready")
        update_worker("deep summary ready", 3, 3, 100)
        self.storage.finish_task(task["id"], "deep summary completed")
        self.storage.add_event("info", "summary", f"deep summary ready for {short_path(str(path))}")

    def _run_reindex_mbox(self, task: sqlite3.Row) -> None:
        work_started = time.monotonic()
        current_embed_server = ""

        def update_worker(message: str, current: int = 0, total: int = 0, percent: int = 0) -> None:
            self.storage.update_worker_runtime(
                pid=os.getpid(),
                role=multiprocessing.current_process().name,
                task_type=task["task_type"],
                current_file=task["source_path"],
                message=message,
                embed_server=current_embed_server,
                progress_current=current,
                progress_total=total,
                progress_percent=percent,
            )

        def throttle_after_file() -> None:
            sleep_time = throttle_sleep_duration(time.monotonic() - work_started, self.speed_ratio())
            if sleep_time > 0:
                update_worker(f"throttling {sleep_time:.2f}s", 0, 0, 0)
                time.sleep(sleep_time)

        path = Path(task["source_path"])
        file_row = self.storage.get_file_record(str(path))
        collection_name = (file_row["collection_name"] if file_row and file_row["collection_name"] else VECTOR_COLLECTION)
        parser_profiles = (
            file_row["parser_profiles"]
            if file_row and file_row["parser_profiles"]
            else default_parser_profiles_for_source("thunderbird")
        )

        self.storage.update_task_progress(task["id"], 0, 5, "opening mailbox")
        update_worker("opening mailbox", 0, 5, 0)
        if not path.exists():
            self.storage.fail_task(task["id"], "mailbox file missing")
            self.storage.mark_file_error(str(path), "mailbox file missing")
            throttle_after_file()
            return

        try:
            digest = content_hash(path)
            messages = parse_mbox_messages(path, parser_profiles)
        except Exception as exc:
            self.storage.fail_task(task["id"], f"mailbox parse failed: {exc}")
            self.storage.mark_file_error(str(path), f"mailbox parse failed: {exc}")
            throttle_after_file()
            return

        if not messages:
            delete_qdrant_points_by_file(collection_name, file_ingest_id(str(path)))
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "no messages parsed from mailbox")
            self.storage.add_event("warn", "thunderbird", f"no messages parsed for {short_path(str(path))}")
            throttle_after_file()
            return

        self.storage.update_task_progress(task["id"], 1, 5, f"parsed {len(messages)} messages")
        update_worker(f"parsed {len(messages)} messages", 1, 5, 20)
        chunk_entries: list[dict[str, Any]] = []
        for message in messages:
            chunks = build_chunks_for_source("thunderbird", message["text"], parser_profiles)
            for chunk_index, chunk_entry in enumerate(chunks):
                chunk_entries.append(
                    {
                        **chunk_entry,
                        "message_index": message["message_index"],
                        "subject": message["subject"],
                        "from": message["from"],
                        "to": message["to"],
                        "date": message["date"],
                        "message_id": message["message_id"],
                        "message_chunk_index": chunk_index,
                    }
                )

        if not chunk_entries:
            delete_qdrant_points_by_file(collection_name, file_ingest_id(str(path)))
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "no chunks produced from mailbox")
            throttle_after_file()
            return

        self.storage.update_task_progress(task["id"], 2, 5, "requesting embeddings")
        update_worker("requesting embeddings", 2, 5, 40)
        chunk_texts = [entry["chunk_text"] for entry in chunk_entries]
        embed_batch_size = self.configured_embed_batch_size()
        embed_parallel_requests = self.configured_embed_parallel_requests()
        total_batches_hint = max(1, (len(chunk_texts) + embed_batch_size - 1) // embed_batch_size)

        def on_embed_batch_done(completed_batches: int, total_batches: int, batch_elapsed: float, embed_label: str) -> None:
            nonlocal current_embed_server
            current_embed_server = embed_label
            progress_current = 2 + completed_batches
            progress_total = 2 + total_batches + 2
            progress_message = f"embedded batches {completed_batches}/{total_batches} via {embed_label}"
            self.storage.update_task_progress(
                task["id"],
                progress_current,
                progress_total,
                progress_message,
            )
            update_worker(
                progress_message,
                progress_current,
                progress_total,
                int((progress_current / max(1, progress_total)) * 100),
            )

        all_embeddings, total_batches = embed_texts_parallel(
            chunk_texts,
            batch_size=embed_batch_size,
            parallel_requests=embed_parallel_requests,
            on_batch_done=on_embed_batch_done,
        )
        total_batches = max(total_batches, total_batches_hint)

        if len(all_embeddings) != len(chunk_entries):
            raise RuntimeError("embedding count mismatch in thunderbird pipeline")

        self.storage.update_task_progress(task["id"], 2 + total_batches, 2 + total_batches + 2, "syncing qdrant")
        update_worker("syncing qdrant", 2 + total_batches, 2 + total_batches + 2)
        ensure_qdrant_collection(collection_name, len(all_embeddings[0]))
        mailbox_file_id = file_ingest_id(str(path))
        delete_qdrant_points_by_file(collection_name, mailbox_file_id)

        points = []
        for idx, (chunk_entry, vector) in enumerate(zip(chunk_entries, all_embeddings)):
            point_id = int(hashlib.sha1(f"{mailbox_file_id}:{chunk_entry['message_index']}:{idx}".encode("utf-8")).hexdigest()[:15], 16)
            points.append(
                {
                    "id": point_id,
                    "vector": vector,
                    "payload": {
                        "file_ingest_id": mailbox_file_id,
                        "source_path": str(path),
                        "source_type": "thunderbird",
                        "collection_name": collection_name,
                        "chunk_index": idx,
                        "chunk_text": chunk_entry["chunk_text"],
                        "file_name": path.name,
                        "mailbox": path.name,
                        "email_subject": chunk_entry.get("subject", ""),
                        "email_from": chunk_entry.get("from", ""),
                        "email_to": chunk_entry.get("to", ""),
                        "email_date": chunk_entry.get("date", ""),
                        "email_message_id": chunk_entry.get("message_id", ""),
                        "email_index": chunk_entry.get("message_index", 0),
                        "email_chunk_index": chunk_entry.get("message_chunk_index", 0),
                    },
                }
            )

        self.storage.update_task_progress(task["id"], 3 + total_batches, 2 + total_batches + 2, "uploading points")
        update_worker("uploading points", 3 + total_batches, 2 + total_batches + 2)
        upsert_qdrant_points(collection_name, points)
        self.storage.mark_file_indexed(str(path), digest, points_count=len(points))
        try:
            summary_text = summarize_document_text(
                "\n\n".join(message["text"] for message in messages[:20]),
                source_path=str(path),
                source_type="thunderbird",
                model=SUMMARY_MODEL,
            ).strip()
            if summary_text:
                self.storage.update_file_summary(str(path), summary_text, SUMMARY_MODEL)
        except Exception as exc:
            self.storage.add_event("warn", "summary", f"summary failed for mailbox {short_path(str(path))}: {exc}")
        self.storage.finish_task(task["id"], f"indexed {len(messages)} emails / {len(points)} chunks")
        self.storage.add_event("info", "thunderbird", f"indexed mailbox {short_path(str(path))} ({len(messages)} emails, {len(points)} chunks)")
        throttle_after_file()

    def _run_delete_file(self, task: sqlite3.Row) -> None:
        file_id = file_ingest_id(task["source_path"])
        file_row = self.storage.get_file_record(task["source_path"])
        collection_name = (file_row["collection_name"] if file_row and file_row["collection_name"] else VECTOR_COLLECTION)
        self.storage.update_task_progress(task["id"], 1, 2, "removing qdrant points")
        delete_qdrant_points_by_file(collection_name, file_id)
        self.storage.update_task_progress(task["id"], 2, 2, "delete complete")
        self.storage.finish_task(task["id"], "qdrant points removed")
        self.storage.add_event("info", "delete", f"removed qdrant points for {short_path(task['source_path'])}")


def os_walk_safe(root: Path):
    for dirpath, dirnames, filenames in __import__("os").walk(root, topdown=True, followlinks=False):
        yield dirpath, dirnames, filenames


STORAGE = Storage(DB_PATH)
SERVICE = Service(STORAGE)
WORKER_PROCESSES: list[multiprocessing.Process] = []


def render_index(snapshot: dict[str, Any]) -> str:
    service = snapshot["service"]
    task = snapshot["task"]
    qdrant = snapshot["qdrant"]
    task_counts = snapshot["task_counts"]
    file_counts = snapshot["file_counts"]
    logs = snapshot["logs"]
    watched = snapshot["watched_dirs"]
    recent_tasks = snapshot["recent_tasks"]
    db_overview = snapshot["db_overview"]
    runtime = snapshot.get("runtime", {})
    planning = snapshot.get("planning") or {}
    planning_threegpp = planning.get("threegpp") or {}
    planning_jira = planning.get("jira") or {}
    qdrant_status = "online" if qdrant["ok"] else "offline"
    watched_rows = "\n".join(
        (
            f"<tr class='source-row' id='source-row-{item['id']}' onclick='editSource({item['id']})'>"
            f"<td class='nowrap'>{item['source_type']}</td>"
            f"<td title='{item['path']}'>{short_path(item['path'], 78)}</td>"
            f"<td title='{item.get('collection_name') or VECTOR_COLLECTION}'>{short_path(item.get('collection_name') or VECTOR_COLLECTION, 26)}</td>"
            f"<td title='{item.get('parser_profiles') or ''}'>{short_path(item.get('parser_profiles') or '-', 36)}</td>"
            f"<td>{'ok' if item['exists'] and item['enabled'] else ('off' if not item['enabled'] else 'missing')}</td>"
            f"<td>{item.get('last_status') or '-'}</td>"
            f"<td class='nowrap'><div class='row-actions'>"
            f"<div class='row-action-group primary'>"
            f"<button class='row-action-btn scan' onclick='event.stopPropagation(); queueDirectoryScan({json.dumps(item['path'])})'>Scan</button>"
            f"<button class='row-action-btn edit' onclick='event.stopPropagation(); editSource({item['id']})'>Edit</button>"
            f"</div>"
            f"<div class='row-action-separator' aria-hidden='true'></div>"
            f"<div class='row-action-group danger'>"
            f"<button class='row-action-btn delete' onclick='event.stopPropagation(); removeSource({item['id']})'>Delete</button>"
            f"</div>"
            f"</div></td>"
            f"</tr>"
        )
        for item in watched
    )
    log_items = "\n".join(
        (
            f"<li class='log-item log-{entry['level']}'>"
            f"<div class='log-head'><span>{entry['time']}</span><strong>{entry['level']}</strong><em>{entry['category']}</em></div>"
            f"<div class='log-msg'>{entry['message']}</div>"
            f"</li>"
        )
        for entry in logs
    )
    task_rows = "\n".join(
        f"<tr><td>{item['created_at'] or '-'}</td><td>{item['task_type']}</td><td>{short_path(item['source_path'])}</td><td>{item['status']}</td><td>{item['progress_percent']}%</td><td>{item['message'] or '-'}</td></tr>"
        for item in recent_tasks
    )
    planning_summary = planning_threegpp.get("summary") or {}
    planning_specs = planning_threegpp.get("specs") or []
    planning_tasks = planning_threegpp.get("next_tasks") or []
    jira_summary = planning_jira.get("summary") or {}
    jira_projects = planning_jira.get("projects") or []
    db_rows = "\n".join(
        f"<tr><td><button class='linkish' onclick=\"loadTable('{item['table']}')\">{item['table']}</button></td><td>{item['count']}</td><td>{', '.join(item['columns'][:6])}{' ...' if len(item['columns']) > 6 else ''}</td></tr>"
        for item in db_overview
    )
    planning_spec_rows = "\n".join(
        (
            f"<tr>"
            f"<td>{item.get('spec_id') or '-'}</td>"
            f"<td>{item.get('family') or '-'}</td>"
            f"<td>{item.get('latest_version') or '-'}</td>"
            f"<td>{item.get('versions') or 0}</td>"
            f"<td>{item.get('fast_indexed') or 0}</td>"
            f"<td>{item.get('fast_pending') or 0}</td>"
            f"<td>{item.get('fast_error') or 0}</td>"
            f"<td>{'yes' if item.get('deep_latest_done') else 'no'}</td>"
            f"<td>{'yes' if item.get('deep_latest_pending') else 'no'}</td>"
            f"<td title='{item.get('latest_path') or '-'}'>{short_path(item.get('latest_path') or '-', 72)}</td>"
            f"</tr>"
        )
        for item in planning_specs
    )
    planning_task_rows = "\n".join(
        (
            f"<tr>"
            f"<td>{item.get('task_priority') or '-'}</td>"
            f"<td>{item.get('task_type') or '-'}</td>"
            f"<td>{item.get('source_type') or '-'}</td>"
            f"<td title='{item.get('source_path') or '-'}'>{short_path(item.get('source_path') or '-', 82)}</td>"
            f"<td>{item.get('status') or '-'}</td>"
            f"<td>{item.get('message') or '-'}</td>"
            f"</tr>"
        )
        for item in planning_tasks
    )
    jira_project_rows = "\n".join(
        (
            f"<tr>"
            f"<td>{item.get('project') or '-'}</td>"
            f"<td>{item.get('indexed') or 0}</td>"
            f"<td>{item.get('pending') or 0}</td>"
            f"<td>{item.get('error') or 0}</td>"
            f"<td>{item.get('total') or 0}</td>"
            f"</tr>"
        )
        for item in jira_projects
    )
    runtime_rows = "\n".join(
        (
            f"<tr>"
            f"<td>{item.get('role') or '-'}</td>"
            f"<td>{item.get('pid') or '-'}</td>"
            f"<td title='{item.get('current_file') or '-'}'>{short_path(item.get('current_file') or '-', 56)}</td>"
            f"<td>{item.get('message') or '-'}</td>"
            f"<td title='{item.get('state') or '?'}'>{format_process_state(item.get('state') or '?')}</td>"
            f"<td>{item.get('cpu_now_percent', 0):.1f}%</td>"
            f"<td>{item.get('cpu_percent', 0):.1f}%</td>"
            f"<td>{item.get('tree_cpu_now_percent', 0):.1f}%</td>"
            f"<td>{item.get('rss_mb', 0):.1f} MB</td>"
            f"<td>{item.get('tree_rss_mb', 0):.1f} MB</td>"
            f"<td>{item.get('proc_count', 1)}</td>"
            f"<td>{item.get('step_age_seconds', 0):.1f}s</td>"
            f"<td>{item.get('elapsed_seconds', 0):.1f}s</td>"
            f"<td>{'yes' if item.get('alive') else 'no'}</td>"
            f"</tr>"
        )
        for item in ([runtime.get("main_process")] if runtime.get("main_process") else []) + list(runtime.get("worker_processes", []))
    )
    collections = ", ".join(qdrant["collections"]) if qdrant["collections"] else "-"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>rag-service</title>
  <style>
    :root {{
      --bg: #eef2f7;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d7dee8;
      --accent: #2563eb;
      --ok: #166534;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; color: var(--ink); background: var(--bg); }}
    .wrap {{ width: 100%; max-width: none; margin: 0; padding: 24px 20px 40px; }}
    h1 {{ margin: 0 0 4px; font-size: 28px; }}
    .sub {{ color: var(--muted); margin-bottom: 18px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 12px; }}
    .planning-summary-row {{ display: grid; grid-template-columns: 132px repeat(3, minmax(220px, 1fr)); gap: 12px; margin-bottom: 12px; }}
    .split {{ display: grid; grid-template-columns: minmax(360px, 1.1fr) minmax(420px, 1.4fr); gap: 12px; margin-top: 12px; }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 16px; }}
    .status-only-card {{ padding: 10px 8px; }}
    .label {{ text-transform: uppercase; font-size: 11px; color: var(--muted); letter-spacing: 0.06em; margin-bottom: 6px; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .ok {{ color: var(--ok); }}
    .bad {{ color: var(--bad); }}
    .controls {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-top: 16px; }}
    .embed-server-grid {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 10px; margin-top: 10px; }}
    .embed-server-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 10px; background: #f8fafc; }}
    .embed-server-card .mini-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }}
    .embed-server-card input {{ width: 100%; }}
    .embed-server-weight {{ margin-top: 8px; display: grid; grid-template-columns: 52px 1fr; gap: 8px; align-items: center; }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 14px 18px;
      margin-top: 16px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }}
    .toolbar-left, .toolbar-right {{ display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px; }}
    .toolbar-actions {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #f8fafc;
    }}
    .toolbar-actions button {{ padding: 8px 12px; }}
    .worker-controls {{ display: inline-flex; align-items: center; gap: 8px; }}
    .worker-controls input {{
      width: 76px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      font: inherit;
      background: #fff;
    }}
    .status-chip {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }}
    .status-chip.warn {{ color: #92400e; border-color: #fcd34d; background: #fffbeb; }}
    .config-form {{
      display: grid;
      gap: 14px;
      margin-bottom: 14px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: linear-gradient(180deg, #fbfdff 0%, #f6f9fc 100%);
    }}
    .form-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px 12px; }}
    .field {{ display: grid; gap: 4px; }}
    .field label {{ font-size: 12px; color: var(--muted); font-weight: 600; }}
    .field input[type="text"], .field input[type="number"], .field select {{
      width: 100%;
      padding: 11px 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      font: inherit;
      background: #fff;
    }}
    .field.check {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      min-height: 44px;
    }}
    .field.check label {{ color: var(--ink); font-weight: 500; }}
    .config-inline-checks {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    .parser-section {{
      display: grid;
      gap: 8px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
    }}
    .parser-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 8px 12px; }}
    .parser-item {{ display: flex; align-items: center; gap: 8px; min-height: 28px; }}
    .parser-item label {{ margin: 0; font-size: 12px; color: var(--ink); }}
    .tabs {{
      display: flex;
      gap: 2px;
      margin: 0 0 16px;
      padding: 0 0 8px;
      border-bottom: 1px solid var(--line);
    }}
    .tab-btn {{
      background: transparent;
      color: var(--muted);
      border-radius: 10px;
      padding: 9px 14px;
      min-width: 0;
      font-weight: 500;
    }}
    .tab-btn.active {{
      background: #e8f0ff;
      color: #1d4ed8;
      box-shadow: inset 0 0 0 1px #c7d7fe;
    }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .search-bar {{ display: grid; grid-template-columns: minmax(240px, 1fr) 110px minmax(180px, 240px) 140px minmax(160px, 220px) minmax(140px, 180px) auto; gap: 10px; align-items: end; }}
    .search-actions {{ display: inline-flex; gap: 4px; align-items: center; padding: 4px; border: 1px solid var(--line); border-radius: 12px; background: #eef2ff; }}
    .search-actions button {{ border-radius: 8px; padding: 8px 14px; }}
    .search-actions button.secondary {{ background: #dbe7ff; color: #1d4ed8; }}
    .collection-switch {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding: 6px 0 2px;
    }}
    .collection-switch-btn {{
      background: #f8fafc;
      color: #334155;
      border: 1px solid #cbd5e1;
      border-radius: 999px;
      padding: 7px 12px;
      font-size: 12px;
      font-weight: 600;
    }}
    .collection-switch-btn.active {{
      background: #dbeafe;
      color: #1d4ed8;
      border-color: #93c5fd;
    }}
    .plan-chart-card {{
      display: block;
      width: 100%;
      min-width: 0;
    }}
    .donut-card {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      min-height: 88px;
      width: auto;
      padding: 0;
    }}
    .donut {{
      width: 84px;
      height: 84px;
      border-radius: 50%;
      background: conic-gradient(#2563eb 0deg 120deg, #e5e7eb 120deg 300deg, #dc2626 300deg 360deg);
      position: relative;
      flex: 0 0 auto;
    }}
    .donut::after {{
      content: "";
      position: absolute;
      inset: 14px;
      border-radius: 50%;
      background: #fff;
      border: 1px solid var(--line);
    }}
    .donut-center {{
      position: absolute;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 1;
      font-size: 11px;
      font-weight: 700;
      color: #334155;
      text-align: center;
      line-height: 1.15;
      padding: 0 14px;
    }}
    @media (max-width: 960px) {{
      .embed-server-grid {{ grid-template-columns: 1fr; }}
      .planning-summary-row {{
        grid-template-columns: 1fr;
      }}
      .plan-chart-card {{
        width: 100%;
      }}
      .donut-card {{
        min-height: 88px;
      }}
    }}
    .search-results {{ display: grid; gap: 10px; margin-top: 14px; }}
    .result-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px; background: #f8fafc; }}
    .result-card:target {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12); }}
    .result-head {{ display: flex; gap: 10px; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }}
    .result-title {{ font-size: 14px; font-weight: 700; word-break: break-word; }}
    .result-score {{ font-size: 12px; color: var(--muted); white-space: nowrap; }}
    .result-source-num {{ display: inline-flex; align-items: center; justify-content: center; min-width: 26px; height: 26px; padding: 0 8px; border-radius: 999px; background: #dbe7ff; color: #1d4ed8; font-size: 12px; font-weight: 700; }}
    .result-meta {{ display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; color: var(--muted); margin-bottom: 8px; }}
    .result-summary {{ margin: 10px 0; padding: 10px 12px; border: 1px solid var(--line); border-radius: 8px; background: #ffffff; }}
    .result-summary .label {{ margin-bottom: 4px; }}
    .result-summary-text {{ white-space: pre-wrap; font-size: 12px; line-height: 1.45; color: var(--ink); }}
    .result-chunk {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; line-height: 1.45; white-space: pre-wrap; word-break: break-word; color: var(--ink); }}
    .answer-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 14px; background: #ffffff; margin-top: 14px; }}
    .answer-text {{ white-space: pre-wrap; line-height: 1.5; }}
    .answer-text a {{ color: var(--accent); text-decoration: none; font-weight: 600; }}
    .answer-text a:hover {{ text-decoration: underline; }}
    button {{ background: var(--accent); color: #fff; border: 0; border-radius: 8px; padding: 9px 14px; cursor: pointer; font: inherit; }}
    button.secondary {{ background: #374151; }}
    button.ghost {{ background: #e5edf9; color: #1f2937; }}
    button.linkish {{ background: transparent; color: var(--accent); padding: 0; border-radius: 0; display: inline-block; margin-right: 8px; white-space: nowrap; }}
    .row-actions {{
      display: inline-flex;
      gap: 10px;
      align-items: center;
      flex-wrap: nowrap;
    }}
    .row-action-group {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .row-action-group.primary {{
      padding-right: 2px;
    }}
    .row-action-group.danger {{
      padding-left: 4px;
    }}
    .row-action-separator {{
      width: 1px;
      align-self: stretch;
      background: #d7deea;
      margin: 1px 0;
    }}
    .row-action-btn {{
      border: 1px solid var(--line);
      border-radius: 10px;
      min-width: 60px;
      padding: 7px 12px;
      font-weight: 600;
      font-size: 12px;
      line-height: 1;
      white-space: nowrap;
      background: #fff;
      cursor: pointer;
      box-shadow: 0 1px 0 rgba(15, 23, 42, 0.03);
    }}
    .row-action-btn.scan {{
      color: #1d4ed8;
      background: #e8f0ff;
      border-color: #93c5fd;
    }}
    .row-action-btn.edit {{
      color: #0f766e;
      background: #ecfeff;
      border-color: #99f6e4;
    }}
    .row-action-btn.delete {{
      color: #ffffff;
      background: #dc2626;
      border-color: #b91c1c;
      box-shadow: 0 0 0 2px #fee2e2;
    }}
    .row-action-btn:hover {{
      filter: brightness(0.97);
    }}
    input[type="range"] {{ width: 240px; accent-color: var(--accent); }}
    progress {{ width: 100%; height: 16px; accent-color: var(--accent); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 14px; }}
    #tab-config th, #tab-config td {{ font-size: 12px; padding: 8px 6px; }}
    .db-table th, .db-table td {{ font-size: 12px; padding: 7px 6px; }}
    .table-wrap {{ overflow: auto; width: 100%; }}
    .nowrap {{ white-space: nowrap; }}
    .config-table {{ border-top: 1px solid var(--line); padding-top: 10px; }}
    .runtime-table td, .runtime-table th {{ font-size: 12px; padding: 7px 6px; }}
    .edit-banner {{
      display: none;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      background: #eff6ff;
      color: #1d4ed8;
      margin-bottom: 12px;
    }}
    .edit-banner.active {{ display: flex; }}
    .edit-banner strong {{ color: #1e3a8a; }}
    .source-row {{ cursor: pointer; }}
    .source-row:hover {{ background: #f8fbff; }}
    .source-row.active {{ background: #eef4ff; }}
    ul.logs {{ list-style: none; padding: 0; margin: 0; display: grid; gap: 6px; max-height: 420px; overflow: auto; }}
    .log-item {{ display: grid; gap: 3px; border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px; background: #f8fafc; }}
    .log-head {{ display: flex; gap: 8px; align-items: center; font-size: 11px; color: var(--muted); white-space: nowrap; overflow: hidden; }}
    .log-head strong {{ color: var(--ink); text-transform: uppercase; font-size: 10px; letter-spacing: 0.05em; }}
    .log-head em {{ color: var(--accent); font-style: normal; font-size: 11px; }}
    .log-msg {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; line-height: 1.35; word-break: break-word; }}
    .log-error {{ border-color: #fecaca; background: #fef2f2; }}
    .log-warn {{ border-color: #fde68a; background: #fffbeb; }}
    .log-info {{ border-color: var(--line); background: #f8fafc; }}
    @media (max-width: 900px) {{ .split {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>rag-service</h1>
    <div class="sub">Ingest status on port {service['port']}. Build: <strong>{UI_BUILD_ID}</strong></div>

    <div class="grid">
      <section class="card"><div class="label">Service</div><div id="service-state" class="value">{'paused' if service['paused'] else 'running'}</div><div id="service-speed" class="muted">speed {service['speed_percent']}%</div></section>
      <section class="card"><div class="label">Qdrant</div><div id="qdrant-state" class="value {'ok' if qdrant['ok'] else 'bad'}">{qdrant_status}</div><div id="qdrant-collections" class="muted">Collections: {collections}</div></section>
      <section class="card"><div class="label">Tasks</div><div id="task-counts-main" class="value">{task_counts['pending']} pending</div><div id="task-counts-sub" class="muted">{task_counts['running']} running, {task_counts['failed']} failed</div></section>
      <section class="card"><div class="label">Files</div><div id="file-counts-main" class="value">{file_counts['indexed']} indexed</div><div id="file-counts-sub" class="muted">{file_counts['pending']} pending, {file_counts['error']} error</div></section>
      <section class="card">
        <div class="label">Ollama / GPU</div>
        <div id="gpu-main" class="value">-</div>
        <div id="gpu-sub" class="muted">loading...</div>
      </section>
    </div>

    <section class="card">
      <div class="label">Current Task</div>
      <div id="current-file" style="font-size:18px;font-weight:700;margin-bottom:6px;">{short_path(task['current_file'])}</div>
      <div id="current-task-meta" class="muted" style="margin-bottom:10px;">{task['task_type']} · {task['message'] or task['source']} · {task['progress_current']} / {task['progress_total']} · {task['progress_percent']}%</div>
      <progress id="current-progress" max="100" value="{task['progress_percent']}"></progress>
      <div class="toolbar">
        <div class="toolbar-left">
          <label for="speed">Speed</label>
          <input id="speed" type="range" min="10" max="100" value="{service['speed_percent']}" oninput="speedValue.textContent=this.value + '%'" onchange="setSpeed(this.value)">
          <strong id="speedValue">{service['speed_percent']}%</strong>
          <div class="worker-controls">
            <label for="ingest-workers">Ingest workers</label>
            <input id="ingest-workers" type="number" min="1" max="32" step="1" value="{service['configured_ingest_workers']}">
            <button class="ghost" onclick="setIngestWorkers()">Apply</button>
          </div>
          <div class="worker-controls">
            <label for="scan-workers">Scan workers</label>
            <input id="scan-workers" type="number" min="1" max="16" step="1" value="{service['configured_scan_workers']}">
            <button class="ghost" onclick="setScanWorkers()">Apply</button>
          </div>
          <div class="worker-controls">
            <label for="embed-batch-size">Embed batch</label>
            <input id="embed-batch-size" type="number" min="1" max="256" step="1" value="{service['configured_embed_batch_size']}">
            <button class="ghost" onclick="setEmbedBatchSize()">Apply</button>
          </div>
          <div class="worker-controls">
            <label for="embed-parallel-requests">Embed parallel</label>
            <input id="embed-parallel-requests" type="number" min="1" max="16" step="1" value="{service['configured_embed_parallel_requests']}">
            <button class="ghost" onclick="setEmbedParallelRequests()">Apply</button>
          </div>
          <span id="worker-restart-chip" class="status-chip{' warn' if service['worker_restart_required'] else ''}">
            {'restart required' if service['worker_restart_required'] else f'scan {service["active_scan_workers"]}/{service["configured_scan_workers"]} · ingest {service["active_ingest_workers"]}/{service["configured_ingest_workers"]}'}
          </span>
          <button class="ghost" onclick="restartService()">Restart service</button>
        </div>
        <div class="toolbar-right toolbar-actions">
          <button onclick="queueScan()">Scan now</button>
          <button class="ghost" onclick="retryFailed()">Retry failed</button>
          <button onclick="togglePause({str(not service['paused']).lower()})">{'Resume' if service['paused'] else 'Pause'}</button>
          <button class="secondary" onclick="window.location.reload()">Refresh</button>
        </div>
      </div>
      <div class="embed-server-grid">
        {''.join(
            f'''
            <div class="embed-server-card">
              <div class="mini-label">Embed server {i + 1}</div>
              <input id="embed-server-address-{i}" type="text" placeholder="IP or URL (optional)" value="{html.escape((service['embed_servers'][i]['address'] if i < len(service['embed_servers']) else ''))}">
              <div class="embed-server-weight">
                <label for="embed-server-weight-{i}">Weight</label>
                <input id="embed-server-weight-{i}" type="number" min="1" max="100" step="1" value="{int(service['embed_servers'][i]['weight'] if i < len(service['embed_servers']) else 1)}">
              </div>
            </div>
            '''
            for i in range(4)
        )}
      </div>
      <div class="controls" style="margin-top:10px;">
        <button class="ghost" onclick="saveEmbedServers()">Apply embed servers</button>
        <span id="embed-servers-status" class="muted" style="margin-left:10px;"></span>
      </div>
      <div class="controls" style="margin-top:10px;">
        <div class="worker-controls" style="min-width:360px;">
          <label for="llm-server-address">LLM server</label>
          <input id="llm-server-address" type="text" placeholder="IP or URL (optional, empty = 127.0.0.1)" value="{html.escape(service.get('llm_server_address') or '')}">
        </div>
        <button class="ghost" onclick="saveLlmServer()">Apply LLM server</button>
        <span id="llm-server-status" class="muted" style="margin-left:10px;"></span>
      </div>
    </section>

    <div class="tabs">
      <button id="tab-overview-btn" class="tab-btn active" onclick="showTab('overview')">Overview</button>
      <button id="tab-config-btn" class="tab-btn" onclick="showTab('config')">Configuration</button>
      <button id="tab-planning-btn" class="tab-btn" onclick="showTab('planning')">Planning</button>
      <button id="tab-search-btn" class="tab-btn" onclick="showTab('search')">Search</button>
      <button id="tab-db-btn" class="tab-btn" onclick="showTab('db')">SQLite</button>
    </div>

    <div id="tab-overview" class="tab-panel active">
      <section class="card" style="margin-bottom:12px;">
        <div class="label">Runtime Workers</div>
        <div class="muted" style="font-size:12px;margin-bottom:8px;">State legend: R = running on CPU, S = sleeping/waiting, D = blocked on I/O, T = stopped, Z = zombie.</div>
        <div class="table-wrap">
          <table class="runtime-table">
            <thead><tr><th>Role</th><th>PID</th><th>Current file</th><th>Message</th><th>Embed server</th><th>State</th><th>CPU now</th><th>Avg CPU</th><th>Tree CPU</th><th>Memory</th><th>Tree Mem</th><th>Procs</th><th>Step age</th><th>Age</th><th>Alive</th></tr></thead>
            <tbody id="runtime-workers-body">{runtime_rows}</tbody>
          </table>
        </div>
      </section>
      <section class="card">
        <div class="label">Recent Tasks</div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Created</th><th>Type</th><th>Path</th><th>Status</th><th>%</th><th>Message</th></tr></thead>
            <tbody id="recent-tasks-body">{task_rows}</tbody>
          </table>
        </div>
      </section>

      <section class="card" style="margin-top:12px;">
        <div class="label">Live Logs</div>
        <ul id="logs-list" class="logs">{log_items}</ul>
      </section>
    </div>

    <div id="tab-config" class="tab-panel">
      <section class="card">
          <div class="label">Watched Directories</div>
          <input id="source-id" type="hidden">
          <div id="edit-banner" class="edit-banner">
            <div>
              <div class="label" style="margin-bottom:2px;">Editing Source</div>
              <strong id="edit-banner-path">New source</strong>
            </div>
            <button class="ghost" onclick="resetSourceForm()">Cancel editing</button>
          </div>
          <div class="config-form">
            <div class="form-grid">
            <div class="field" style="grid-column: span 3;">
              <label for="source-path">Source path</label>
              <input id="source-path" type="text" placeholder="/path/to/source">
            </div>
            <div class="field">
              <label for="source-type">Source type</label>
              <select id="source-type" onchange="applyDefaultParsersForType()">
                <option value="documents">documents</option>
                <option value="code">code</option>
                <option value="jira">jira</option>
                <option value="3gpp">3gpp</option>
                <option value="thunderbird">thunderbird</option>
              </select>
            </div>
            <div class="field">
              <label for="source-collection">Qdrant collection</label>
              <input id="source-collection" type="text" value="{VECTOR_COLLECTION}" placeholder="global_knowledge">
            </div>
            <div class="field">
              <label for="source-interval">Scan interval (seconds)</label>
              <input id="source-interval" type="number" min="0" step="1" value="300">
            </div>
            </div>
            <div class="parser-section">
              <label>Parser profiles</label>
              <div id="parser-profiles" class="parser-grid">
                <div class="parser-item"><input id="parser-generic_text" type="checkbox" name="parser-profile" value="generic_text"><label for="parser-generic_text">generic_text</label></div>
                <div class="parser-item"><input id="parser-code" type="checkbox" name="parser-profile" value="code"><label for="parser-code">code</label></div>
                <div class="parser-item"><input id="parser-pdf" type="checkbox" name="parser-profile" value="pdf"><label for="parser-pdf">pdf</label></div>
                <div class="parser-item"><input id="parser-office" type="checkbox" name="parser-profile" value="office"><label for="parser-office">office</label></div>
                <div class="parser-item"><input id="parser-archive_zip" type="checkbox" name="parser-profile" value="archive_zip"><label for="parser-archive_zip">archive_zip</label></div>
                <div class="parser-item"><input id="parser-jira_issue" type="checkbox" name="parser-profile" value="jira_issue"><label for="parser-jira_issue">jira_issue</label></div>
                <div class="parser-item"><input id="parser-3gpp_spec" type="checkbox" name="parser-profile" value="3gpp_spec"><label for="parser-3gpp_spec">3gpp_spec</label></div>
                <div class="parser-item"><input id="parser-thunderbird_mbox" type="checkbox" name="parser-profile" value="thunderbird_mbox"><label for="parser-thunderbird_mbox">thunderbird_mbox</label></div>
                <div class="parser-item"><input id="parser-email_attachments" type="checkbox" name="parser-profile" value="email_attachments"><label for="parser-email_attachments">email_attachments</label></div>
              </div>
            </div>
            <div class="config-inline-checks">
              <div class="field check">
                <input id="source-enabled" type="checkbox" checked>
                <label for="source-enabled">Enabled</label>
              </div>
              <div class="field check">
                <input id="source-recursive" type="checkbox" checked>
                <label for="source-recursive">Recursive</label>
              </div>
            </div>
          </div>
          <div class="controls" style="margin-top:0;margin-bottom:12px;">
            <button id="save-source-btn" onclick="saveSource()">Save source</button>
            <button class="ghost" onclick="resetSourceForm()">Clear</button>
          </div>
          <div class="table-wrap config-table">
            <table>
              <thead><tr><th>Type</th><th>Path</th><th>Collection</th><th>Parsers</th><th>State</th><th>Last scan</th><th>Actions</th></tr></thead>
              <tbody>{watched_rows}</tbody>
            </table>
          </div>
      </section>
    </div>

    <div id="tab-planning" class="tab-panel">
      <div class="planning-summary-row">
        <section class="card plan-chart-card status-only-card">
          <div id="plan-3gpp-chart" class="donut-card"></div>
        </section>
        <section class="card"><div class="label">MCPTT Fast</div><div id="plan-mcptt-fast-main" class="value">{planning_summary.get('mcptt_fast_indexed', 0)} indexed</div><div id="plan-mcptt-fast-sub" class="muted">{planning_summary.get('mcptt_fast_pending', 0)} pending, {planning_summary.get('mcptt_fast_error', 0)} error</div></section>
        <section class="card"><div class="label">MCPTT Deep Latest</div><div id="plan-mcptt-deep-main" class="value">{planning_summary.get('mcptt_latest_deep_done', 0)} done</div><div id="plan-mcptt-deep-sub" class="muted">{planning_summary.get('mcptt_latest_deep_pending', 0)} pending</div></section>
        <section class="card"><div class="label">Other 3GPP Fast</div><div id="plan-other-fast-main" class="value">{planning_summary.get('other_fast_indexed', 0)} indexed</div><div id="plan-other-fast-sub" class="muted">{planning_summary.get('other_fast_pending', 0)} pending, {planning_summary.get('other_fast_error', 0)} error</div></section>
      </div>
      <div class="planning-summary-row">
        <section class="card plan-chart-card status-only-card">
          <div id="plan-jira-chart" class="donut-card"></div>
        </section>
        <section class="card"><div class="label">Jira Indexed</div><div id="plan-jira-indexed-main" class="value">{jira_summary.get('indexed', 0)} indexed</div><div id="plan-jira-indexed-sub" class="muted">{jira_summary.get('projects', 0)} project(s)</div></section>
        <section class="card"><div class="label">Jira Pending</div><div id="plan-jira-pending-main" class="value">{jira_summary.get('pending', 0)} pending</div><div id="plan-jira-pending-sub" class="muted">to ingest first</div></section>
        <section class="card"><div class="label">Jira Error</div><div id="plan-jira-error-main" class="value">{jira_summary.get('error', 0)} error</div><div id="plan-jira-error-sub" class="muted">needs retry or review</div></section>
      </div>
      <section class="card" style="margin-bottom:12px;">
        <div class="label">Jira Project Progress</div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Project</th><th>Indexed</th><th>Pending</th><th>Error</th><th>Total</th></tr></thead>
            <tbody id="planning-jira-body">{jira_project_rows}</tbody>
          </table>
        </div>
      </section>
      <section class="card">
        <div class="label">3GPP Spec Progress</div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Spec</th><th>Family</th><th>Latest</th><th>Versions</th><th>Fast indexed</th><th>Pending</th><th>Error</th><th>Deep latest done</th><th>Deep latest pending</th><th>Latest path</th></tr></thead>
            <tbody id="planning-specs-body">{planning_spec_rows}</tbody>
          </table>
        </div>
      </section>
      <section class="card" style="margin-top:12px;">
        <div class="label">Next Tasks</div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Priority</th><th>Type</th><th>Source</th><th>Path</th><th>Status</th><th>Message</th></tr></thead>
            <tbody id="planning-tasks-body">{planning_task_rows}</tbody>
          </table>
        </div>
      </section>
    </div>

    <div id="tab-search" class="tab-panel">
      <section class="card">
        <div class="label">Search Global Knowledge</div>
        <div class="search-bar">
          <div class="field" style="grid-column: 1 / -1;">
            <label>Collection view</label>
            <div id="search-collection-switch" class="collection-switch"></div>
          </div>
          <div class="field">
            <label for="search-query">Query</label>
            <input id="search-query" type="text" placeholder="Search indexed knowledge...">
          </div>
          <div class="field">
            <label for="search-limit">Top results</label>
            <input id="search-limit" type="number" min="1" max="20" step="1" value="{SEARCH_TOP_K}">
          </div>
          <div class="field">
            <label for="search-model">LLM model</label>
            <select id="search-model">
              <option value="{LLM_MODEL}">{LLM_MODEL}</option>
            </select>
          </div>
          <div class="field">
            <label for="search-detail">Answer detail</label>
            <select id="search-detail">
              <option value="standard">standard</option>
              <option value="deep">deep</option>
            </select>
          </div>
          <div class="field">
            <label for="search-language">Answer language</label>
            <select id="search-language">
              <option value="fr" selected>French</option>
              <option value="en">English</option>
            </select>
          </div>
          <div class="field">
            <label for="search-collection">Collection</label>
            <select id="search-collection">
              <option value="">all</option>
            </select>
          </div>
          <div class="field">
            <label for="search-source-type">Source type</label>
            <select id="search-source-type">
              <option value="">all</option>
            </select>
          </div>
          <div class="search-actions">
            <button onclick="runSearch()">Search</button>
            <button class="secondary" onclick="runAsk()">Ask LLM</button>
          </div>
        </div>
        <div id="search-meta" class="muted" style="margin-top:12px;">No query executed yet.</div>
        <div id="search-answer"></div>
        <div id="search-results" class="search-results"></div>
      </section>
    </div>

    <div id="tab-db" class="tab-panel">
      <section class="card">
        <div class="label">SQLite Overview</div>
        <table class="db-table">
          <thead><tr><th>Table</th><th>Rows</th><th>Columns</th></tr></thead>
          <tbody id="db-overview-body">{db_rows}</tbody>
        </table>
      </section>

      <section class="card" style="margin-top:12px;">
        <div class="label">Records</div>
        <div id="db-meta" class="muted" style="margin-bottom:10px;">Select a table.</div>
        <div style="overflow:auto; max-height: 560px;">
          <table id="db-records-table" class="db-table">
            <thead id="db-records-head"></thead>
            <tbody id="db-records-body"></tbody>
          </table>
        </div>
        <div class="controls">
          <button class="secondary" onclick="changePage(-1)">Prev</button>
          <button class="secondary" onclick="changePage(1)">Next</button>
        </div>
      </section>
    </div>
  </div>
  <script>
    let currentTable = null;
    let currentOffset = 0;
    let currentLimit = 50;
    let pendingWorkerUpdate = false;
    let editingEmbedServers = false;
    let editingLlmServer = false;

    function shortPath(path, maxLen = 110) {{
      if (!path || path.length <= maxLen) return path || '-';
      return '...' + path.slice(-(maxLen - 3));
    }}

    function formatProcessState(code) {{
      const labels = {{
        R: 'Running',
        S: 'Sleeping',
        D: 'Blocked I/O',
        T: 'Stopped',
        Z: 'Zombie',
        I: 'Idle',
        '?': 'Unknown'
      }};
      const key = String(code || '?');
      return `${{key}} - ${{labels[key] || 'Unknown'}}`;
    }}

    function showTab(name) {{
      for (const panel of document.querySelectorAll('.tab-panel')) panel.classList.remove('active');
      for (const btn of document.querySelectorAll('.tab-btn')) btn.classList.remove('active');
      document.getElementById(`tab-${{name}}`).classList.add('active');
      document.getElementById(`tab-${{name}}-btn`).classList.add('active');
    }}

    function renderLogs(logs) {{
      const container = document.getElementById('logs-list');
      container.innerHTML = logs.map((entry) => `
        <li class="log-item log-${{entry.level}}">
          <div class="log-head">
            <span>${{entry.time}}</span>
            <strong>${{entry.level}}</strong>
            <em>${{entry.category}}</em>
          </div>
          <div class="log-msg">${{entry.message}}</div>
        </li>
      `).join('');
    }}

    function renderTasks(tasks) {{
      const body = document.getElementById('recent-tasks-body');
      body.innerHTML = tasks.map((task) => `
        <tr>
          <td>${{task.created_at || '-'}}</td>
          <td>${{task.task_type}}</td>
          <td>${{shortPath(task.source_path)}}</td>
          <td>${{task.status}}</td>
          <td>${{task.progress_percent}}%</td>
          <td>${{task.message || '-'}} </td>
        </tr>
      `).join('');
    }}

    function renderRuntimeWorkers(runtime) {{
      const body = document.getElementById('runtime-workers-body');
      if (!body) return;
      const rows = [];
      if (runtime && runtime.main_process) rows.push(runtime.main_process);
      for (const item of ((runtime && runtime.worker_processes) || [])) rows.push(item);
      body.innerHTML = rows.map((item) => `
        <tr>
          <td>${{escapeHtml(item.role || '-')}}</td>
          <td>${{item.pid || '-'}}</td>
          <td title="${{escapeHtml(item.current_file || '-')}}">${{escapeHtml(shortPath(item.current_file || '-', 56))}}</td>
          <td>${{escapeHtml(item.message || '-')}}</td>
          <td>${{escapeHtml(item.embed_server || '-')}}</td>
          <td title="${{escapeHtml(item.state || '?')}}">${{escapeHtml(formatProcessState(item.state || '?'))}}</td>
          <td>${{Number(item.cpu_now_percent || 0).toFixed(1)}}%</td>
          <td>${{Number(item.cpu_percent || 0).toFixed(1)}}%</td>
          <td>${{Number(item.tree_cpu_now_percent || 0).toFixed(1)}}%</td>
          <td>${{Number(item.rss_mb || 0).toFixed(1)}} MB</td>
          <td>${{Number(item.tree_rss_mb || 0).toFixed(1)}} MB</td>
          <td>${{item.proc_count ?? 1}}</td>
          <td>${{Number(item.step_age_seconds || 0).toFixed(1)}}s</td>
          <td>${{Number(item.elapsed_seconds || 0).toFixed(1)}}s</td>
          <td>${{item.alive ? 'yes' : 'no'}}</td>
        </tr>
      `).join('');
    }}

    function renderOllamaRuntime(ollama) {{
      const runtime = (ollama && ollama.runtime) || {{}};
      const gpuMain = document.getElementById('gpu-main');
      const gpuSub = document.getElementById('gpu-sub');
      if (!gpuMain || !gpuSub) return;
      if (!runtime.ok) {{
        gpuMain.textContent = 'unavailable';
        gpuMain.className = 'value bad';
        gpuSub.textContent = runtime.error || 'GPU metrics unavailable';
        return;
      }}
      gpuMain.textContent = `${{Number(runtime.gpu_util || 0).toFixed(0)}}% GPU`;
      gpuMain.className = 'value ok';
      const totalMem = Number(runtime.gpu_memory_total_mb || 0);
      const usedMem = Number(runtime.gpu_memory_used_mb || 0);
      const ollamaMem = Number(runtime.ollama_gpu_memory_mb || 0);
      const runnerCount = Number(runtime.runner_count || 0);
      const runnerThreads = Number(runtime.runner_threads || 0);
      const runnerCpuNow = Number(runtime.runner_cpu_now_percent || 0);
      const runnerCpuCores = runnerCpuNow / 100.0;
      gpuSub.textContent = `GPU mem ${{usedMem.toFixed(0)}}/${{totalMem.toFixed(0)}} MB · Ollama GPU ${{ollamaMem.toFixed(0)}} MB · runners ${{runnerCount}} · threads ${{runnerThreads}} · CPU now ${{runnerCpuNow.toFixed(1)}}% (~${{runnerCpuCores.toFixed(1)}} cores)`;
    }}

    function renderPlanning(planning) {{
      const data = (planning && planning.threegpp) || {{}};
      const jira = (planning && planning.jira) || {{}};
      const summary = data.summary || {{}};
      const jiraSummary = jira.summary || {{}};
      const specs = data.specs || [];
      const tasks = data.next_tasks || [];
      const jiraProjects = jira.projects || [];

      const setText = (id, value) => {{
        const el = document.getElementById(id);
        if (el) el.textContent = value;
      }};

      setText('plan-mcptt-fast-main', `${{summary.mcptt_fast_indexed || 0}} indexed`);
      setText('plan-mcptt-fast-sub', `${{summary.mcptt_fast_pending || 0}} pending, ${{summary.mcptt_fast_error || 0}} error`);
      setText('plan-mcptt-deep-main', `${{summary.mcptt_latest_deep_done || 0}} done`);
      setText('plan-mcptt-deep-sub', `${{summary.mcptt_latest_deep_pending || 0}} pending`);
      setText('plan-other-fast-main', `${{summary.other_fast_indexed || 0}} indexed`);
      setText('plan-other-fast-sub', `${{summary.other_fast_pending || 0}} pending, ${{summary.other_fast_error || 0}} error`);
      setText('plan-jira-indexed-main', `${{jiraSummary.indexed || 0}} indexed`);
      setText('plan-jira-indexed-sub', `${{jiraSummary.projects || 0}} project(s)`);
      setText('plan-jira-pending-main', `${{jiraSummary.pending || 0}} pending`);
      setText('plan-jira-pending-sub', `to ingest first`);
      setText('plan-jira-error-main', `${{jiraSummary.error || 0}} error`);
      setText('plan-jira-error-sub', `needs retry or review`);
      renderPlanDonut('plan-3gpp-chart', '3GPP', {{
        indexed: (summary.mcptt_fast_indexed || 0) + (summary.other_fast_indexed || 0),
        pending: (summary.mcptt_fast_pending || 0) + (summary.other_fast_pending || 0) + (summary.mcptt_latest_deep_pending || 0),
        error: (summary.mcptt_fast_error || 0) + (summary.other_fast_error || 0),
      }});
      renderPlanDonut('plan-jira-chart', 'Jira', jiraSummary);

      const specsBody = document.getElementById('planning-specs-body');
      if (specsBody) {{
        specsBody.innerHTML = specs.map((item) => `
          <tr>
            <td>${{escapeHtml(item.spec_id || '-')}}</td>
            <td>${{escapeHtml(item.family || '-')}}</td>
            <td>${{escapeHtml(item.latest_version || '-')}}</td>
            <td>${{item.versions || 0}}</td>
            <td>${{item.fast_indexed || 0}}</td>
            <td>${{item.fast_pending || 0}}</td>
            <td>${{item.fast_error || 0}}</td>
            <td>${{item.deep_latest_done ? 'yes' : 'no'}}</td>
            <td>${{item.deep_latest_pending ? 'yes' : 'no'}}</td>
            <td title="${{escapeHtml(item.latest_path || '-')}}">${{escapeHtml(shortPath(item.latest_path || '-', 72))}}</td>
          </tr>
        `).join('');
      }}

      const tasksBody = document.getElementById('planning-tasks-body');
      if (tasksBody) {{
        tasksBody.innerHTML = tasks.map((item) => `
          <tr>
            <td>${{item.task_priority ?? '-'}}</td>
            <td>${{escapeHtml(item.task_type || '-')}}</td>
            <td>${{escapeHtml(item.source_type || '-')}}</td>
            <td title="${{escapeHtml(item.source_path || '-')}}">${{escapeHtml(shortPath(item.source_path || '-', 82))}}</td>
            <td>${{escapeHtml(item.status || '-')}}</td>
            <td>${{escapeHtml(item.message || '-')}}</td>
          </tr>
        `).join('');
      }}

      const jiraBody = document.getElementById('planning-jira-body');
      if (jiraBody) {{
        jiraBody.innerHTML = jiraProjects.map((item) => `
          <tr>
            <td>${{escapeHtml(item.project || '-')}}</td>
            <td>${{item.indexed || 0}}</td>
            <td>${{item.pending || 0}}</td>
            <td>${{item.error || 0}}</td>
            <td>${{item.total || 0}}</td>
          </tr>
        `).join('');
      }}
    }}

    function renderPlanDonut(targetId, label, stats) {{
      const target = document.getElementById(targetId);
      if (!target) return;
      const indexed = Number(stats.indexed || 0);
      const pending = Number(stats.pending || 0);
      const error = Number(stats.error || 0);
      const total = Math.max(1, indexed + pending + error);
      const indexedPct = ((indexed / total) * 100).toFixed(2);
      const indexedDeg = Math.round((indexed / total) * 360);
      const pendingDeg = Math.round((pending / total) * 360);
      const errorStart = indexedDeg + pendingDeg;
      target.innerHTML = `
        <div class="donut" style="background: conic-gradient(
          #2563eb 0deg ${{indexedDeg}}deg,
          #cbd5e1 ${{indexedDeg}}deg ${{indexedDeg + pendingDeg}}deg,
          #dc2626 ${{errorStart}}deg 360deg
        );">
          <div class="donut-center">${{indexedPct}}%</div>
        </div>
      `;
    }}

    function escapeHtml(value) {{
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function linkifyCitations(text, results) {{
      const maxRef = Array.isArray(results) ? results.length : 0;
      return escapeHtml(text || '').replace(/\\[(\\d+)\\]/g, (match, rawNumber) => {{
        const number = Number(rawNumber);
        if (!Number.isInteger(number) || number < 1 || number > maxRef) return match;
        return `<a href="#result-${{number}}" title="Go to source ${{number}}">[${{number}}]</a>`;
      }});
    }}

    function renderSearchResults(data) {{
      const meta = document.getElementById('search-meta');
      const answer = document.getElementById('search-answer');
      const container = document.getElementById('search-results');
      const results = data.results || [];
      const filters = [];
      if (data.collection_name) filters.push(`collection=${{data.collection_name}}`);
      if (data.source_type) filters.push(`type=${{data.source_type}}`);
      meta.textContent = data.query
        ? `${{results.length}} result(s) for "${{data.query}}"${{filters.length ? ` · ${{filters.join(' · ')}}` : ''}}`
        : 'No query executed yet.';
      if (data.answer) {{
        answer.innerHTML = `
          <article class="answer-card">
            <div class="label">LLM Answer${{data.model ? ` · ${{data.model}}` : ''}}${{data.detail_level ? ` · ${{data.detail_level}}` : ''}}${{data.answer_language ? ` · ${{data.answer_language}}` : ''}}</div>
            <div class="answer-text">${{linkifyCitations(data.answer, results)}}</div>
          </article>
        `;
      }} else {{
        answer.innerHTML = '';
      }}
      if (!results.length) {{
        container.innerHTML = data.query ? '<div class="muted">No results.</div>' : '';
        return;
      }}
      container.innerHTML = results.map((item, index) => `
        <article class="result-card" id="result-${{index + 1}}">
          <div class="result-head">
            <div style="display:flex; gap:10px; align-items:flex-start;">
              <span class="result-source-num">[${{index + 1}}]</span>
              <div class="result-title" title="${{escapeHtml(item.zip_entry_path || item.source_path || '')}}">${{escapeHtml(shortPath(item.zip_entry_path || item.source_path || item.file_name || '-', 160))}}</div>
            </div>
            <div class="result-score">score ${{Number(item.score || 0).toFixed(3)}}</div>
          </div>
          <div class="result-meta">
            <span>type: ${{escapeHtml(item.source_type || '-')}}</span>
            <span>collection: ${{escapeHtml(item.collection_name || '-')}}</span>
            <span>file: ${{escapeHtml(item.file_name || '-')}}</span>
            <span>zip entry: ${{escapeHtml(item.zip_entry_path || '-')}}</span>
            <span>mailbox: ${{escapeHtml(item.mailbox || '-')}}</span>
            <span>spec: ${{escapeHtml(item.spec_id || '-')}}</span>
            <span>version: ${{escapeHtml(item.spec_version || '-')}}</span>
            <span>release: ${{escapeHtml(item.spec_release || '-')}}</span>
            <span>clause: ${{escapeHtml(item.clause_id || '-')}}</span>
            <span>chunk: ${{item.chunk_index ?? 0}}</span>
          </div>
          ${{item.issue_key || item.fix_version || item.issue_feature ? `
            <div class="result-meta">
              <span>issue: ${{escapeHtml(item.issue_key || '-')}}</span>
              <span>title: ${{escapeHtml(item.issue_title || '-')}}</span>
              <span>status: ${{escapeHtml(item.issue_status || '-')}}</span>
              <span>fix version: ${{escapeHtml(item.fix_version || '-')}}</span>
              <span>feature: ${{escapeHtml(item.issue_feature || '-')}}</span>
              <span>component: ${{escapeHtml(item.issue_component || '-')}}</span>
            </div>
          ` : ''}}
          ${{item.email_subject || item.email_from || item.email_date ? `
            <div class="result-meta">
              <span>subject: ${{escapeHtml(item.email_subject || '-')}}</span>
              <span>from: ${{escapeHtml(item.email_from || '-')}}</span>
              <span>date: ${{escapeHtml(item.email_date || '-')}}</span>
            </div>
          ` : ''}}
          ${{item.clause_title ? `<div class="result-meta"><span>clause title: ${{escapeHtml(item.clause_title)}}</span></div>` : ''}}
          ${{item.summary_text ? `
            <div class="result-summary">
              <div class="label">${{`Document Memory${{item.summary_model ? ` · ${{escapeHtml(item.summary_model)}}` : ''}}`}}</div>
              <div class="result-summary-text">${{escapeHtml(item.summary_text)}}</div>
            </div>
          ` : ''}}
          <div class="result-chunk">${{escapeHtml(item.chunk_text || '')}}</div>
        </article>
      `).join('');
    }}

    function updateModelOptions(ollama) {{
      const select = document.getElementById('search-model');
      if (!select) return;
      const current = select.value || (ollama && ollama.default_model) || '{LLM_MODEL}';
      const models = (ollama && ollama.models && ollama.models.length) ? ollama.models : ['{LLM_MODEL}'];
      select.innerHTML = models.map((model) => `<option value="${{escapeHtml(model)}}">${{escapeHtml(model)}}</option>`).join('');
      if (models.includes(current)) {{
        select.value = current;
      }} else {{
        select.value = models[0];
      }}
    }}

    function updateSearchFilters(data) {{
      const collectionSelect = document.getElementById('search-collection');
      const sourceTypeSelect = document.getElementById('search-source-type');
      const collectionSwitch = document.getElementById('search-collection-switch');
      if (!collectionSelect || !sourceTypeSelect) return;

      const currentCollection = collectionSelect.value;
      const currentSourceType = sourceTypeSelect.value;

      const collections = new Set((data.qdrant && data.qdrant.collections) || []);
      for (const item of (data.watched_dirs || [])) {{
        if (item.collection_name) collections.add(item.collection_name);
      }}
      const sourceTypes = new Set();
      for (const item of (data.watched_dirs || [])) {{
        if (item.source_type) sourceTypes.add(item.source_type);
      }}

      collectionSelect.innerHTML = ['<option value="">all</option>']
        .concat(Array.from(collections).sort().map((name) => `<option value="${{escapeHtml(name)}}">${{escapeHtml(name)}}</option>`))
        .join('');
      sourceTypeSelect.innerHTML = ['<option value="">all</option>']
        .concat(Array.from(sourceTypes).sort().map((name) => `<option value="${{escapeHtml(name)}}">${{escapeHtml(name)}}</option>`))
        .join('');

      if (Array.from(collections).includes(currentCollection)) collectionSelect.value = currentCollection;
      if (Array.from(sourceTypes).includes(currentSourceType)) sourceTypeSelect.value = currentSourceType;

      if (collectionSwitch) {{
        const selected = collectionSelect.value || '';
        const ordered = [''].concat(Array.from(collections).sort());
        collectionSwitch.innerHTML = ordered.map((name) => {{
          const value = name || '';
          const label = value || 'all';
          const active = value === selected ? ' active' : '';
          return `<button class="collection-switch-btn${{active}}" onclick="setSearchCollection('${{escapeHtml(value)}}')">${{escapeHtml(label)}}</button>`;
        }}).join('');
      }}
    }}

    function setSearchCollection(value) {{
      const select = document.getElementById('search-collection');
      if (!select) return;
      select.value = value || '';
      updateSearchFilters(window.__lastStatusData || {{}});
    }}

    async function loadDbOverview() {{
      const response = await fetch('/api/db/overview', {{ cache: 'no-store' }});
      if (!response.ok) return;
      const data = await response.json();
      document.getElementById('db-overview-body').innerHTML = data.tables.map((item) => `
        <tr>
          <td><button class="linkish" onclick="loadTable('${{item.table}}')">${{item.table}}</button></td>
          <td>${{item.count}}</td>
          <td>${{item.columns.slice(0, 6).join(', ')}}${{item.columns.length > 6 ? ' ...' : ''}}</td>
        </tr>
      `).join('');
    }}

    async function loadTable(table, offset = 0) {{
      currentTable = table;
      currentOffset = Math.max(0, offset);
      const response = await fetch(`/api/db/table?name=${{encodeURIComponent(table)}}&offset=${{currentOffset}}&limit=${{currentLimit}}`, {{ cache: 'no-store' }});
      if (!response.ok) return;
      const data = await response.json();
      document.getElementById('db-meta').textContent = `${{data.table}} · rows ${{Math.min(data.offset + 1, data.total)}}-${{Math.min(data.offset + data.rows.length, data.total)}} / ${{data.total}}`;
      document.getElementById('db-records-head').innerHTML = `<tr>${{data.columns.map((col) => `<th>${{col}}</th>`).join('')}}</tr>`;
      document.getElementById('db-records-body').innerHTML = data.rows.map((row) => `
        <tr>
          ${{data.columns.map((col) => `<td title="${{String(row[col] ?? '')}}">${{shortPath(String(row[col] ?? ''), 80)}}</td>`).join('')}}
        </tr>
      `).join('');
    }}

    async function runSearch() {{
      const query = document.getElementById('search-query').value.trim();
      const limit = Number(document.getElementById('search-limit').value || '{SEARCH_TOP_K}');
      const collection = document.getElementById('search-collection').value;
      const sourceType = document.getElementById('search-source-type').value;
      if (!query) {{
        renderSearchResults({{ query: '', answer: '', results: [] }});
        return;
      }}
      const meta = document.getElementById('search-meta');
      meta.textContent = 'Searching...';
      const response = await fetch(`/api/search?q=${{encodeURIComponent(query)}}&limit=${{encodeURIComponent(limit)}}&collection=${{encodeURIComponent(collection)}}&source_type=${{encodeURIComponent(sourceType)}}`, {{ cache: 'no-store' }});
      if (!response.ok) {{
        meta.textContent = 'Search failed.';
        return;
      }}
      const data = await response.json();
      renderSearchResults(data);
    }}

    async function runAsk() {{
      const query = document.getElementById('search-query').value.trim();
      const limit = Number(document.getElementById('search-limit').value || '{SEARCH_TOP_K}');
      const model = document.getElementById('search-model').value;
      const detail = document.getElementById('search-detail').value;
      const language = document.getElementById('search-language').value;
      const collection = document.getElementById('search-collection').value;
      const sourceType = document.getElementById('search-source-type').value;
      if (!query) {{
        renderSearchResults({{ query: '', answer: '', results: [] }});
        return;
      }}
      const meta = document.getElementById('search-meta');
      meta.textContent = 'Generating answer...';
      const response = await fetch(`/api/ask?q=${{encodeURIComponent(query)}}&limit=${{encodeURIComponent(limit)}}&model=${{encodeURIComponent(model)}}&detail=${{encodeURIComponent(detail)}}&language=${{encodeURIComponent(language)}}&collection=${{encodeURIComponent(collection)}}&source_type=${{encodeURIComponent(sourceType)}}`, {{ cache: 'no-store' }});
      if (!response.ok) {{
        meta.textContent = 'LLM answer failed.';
        return;
      }}
      const data = await response.json();
      renderSearchResults(data);
    }}

    function changePage(delta) {{
      if (!currentTable) return;
      const nextOffset = Math.max(0, currentOffset + delta * currentLimit);
      loadTable(currentTable, nextOffset);
    }}

    function updateEditingState(item = null) {{
      const banner = document.getElementById('edit-banner');
      const bannerPath = document.getElementById('edit-banner-path');
      const saveButton = document.getElementById('save-source-btn');
      for (const row of document.querySelectorAll('.source-row')) {{
        row.classList.remove('active');
      }}
      if (!item) {{
        banner.classList.remove('active');
        bannerPath.textContent = 'New source';
        saveButton.textContent = 'Save source';
        return;
      }}
      banner.classList.add('active');
      bannerPath.textContent = item.path || `Source #${{item.id}}`;
      saveButton.textContent = 'Update source';
      const activeRow = document.getElementById(`source-row-${{item.id}}`);
      if (activeRow) activeRow.classList.add('active');
    }}

    function getSelectedParserProfiles() {{
      return Array.from(document.querySelectorAll('input[name="parser-profile"]:checked'))
        .map((input) => input.value)
        .sort()
        .join(',');
    }}

    function setParserCheckboxes(value) {{
      const selected = new Set(String(value || '').split(',').map((item) => item.trim()).filter(Boolean));
      for (const input of document.querySelectorAll('input[name="parser-profile"]')) {{
        input.checked = selected.has(input.value);
      }}
    }}

    function resetSourceForm() {{
      document.getElementById('source-id').value = '';
      document.getElementById('source-path').value = '';
      document.getElementById('source-type').value = 'documents';
      document.getElementById('source-collection').value = '{VECTOR_COLLECTION}';
      setParserCheckboxes('archive_zip,generic_text,pdf,office');
      document.getElementById('source-enabled').checked = true;
      document.getElementById('source-recursive').checked = true;
      document.getElementById('source-interval').value = 300;
      updateEditingState(null);
    }}

    function defaultParsersForType(sourceType) {{
      const defaults = {{
        documents: 'archive_zip,generic_text,pdf,office',
        code: 'archive_zip,generic_text,code',
        jira: 'jira_issue',
        thunderbird: 'thunderbird_mbox,email_attachments',
        '3gpp': 'archive_zip,generic_text,pdf,office,3gpp_spec'
      }};
      return defaults[sourceType] || 'generic_text';
    }}

    function applyDefaultParsersForType() {{
      const id = document.getElementById('source-id').value;
      if (id) return;
      const sourceType = document.getElementById('source-type').value;
      setParserCheckboxes(defaultParsersForType(sourceType));
    }}

    function editSource(id) {{
      const item = (window.__watchedDirs || []).find((entry) => entry.id === id);
      if (!item) return;
      document.getElementById('source-id').value = String(item.id);
      document.getElementById('source-path').value = item.path || '';
      document.getElementById('source-type').value = item.source_type || 'documents';
      document.getElementById('source-collection').value = item.collection_name || '{VECTOR_COLLECTION}';
      setParserCheckboxes(item.parser_profiles || '');
      document.getElementById('source-enabled').checked = !!item.enabled;
      document.getElementById('source-recursive').checked = !!item.recursive;
      document.getElementById('source-interval').value = item.scan_interval_seconds ?? 300;
      updateEditingState(item);
      document.getElementById('source-path').focus();
    }}

    async function saveSource() {{
      const id = document.getElementById('source-id').value;
      const payload = {{
        path: document.getElementById('source-path').value.trim(),
        source_type: document.getElementById('source-type').value,
        collection_name: document.getElementById('source-collection').value.trim() || '{VECTOR_COLLECTION}',
        parser_profiles: getSelectedParserProfiles(),
        enabled: document.getElementById('source-enabled').checked,
        recursive: document.getElementById('source-recursive').checked,
        scan_interval_seconds: Number(document.getElementById('source-interval').value || '0')
      }};
      if (!payload.path) return;
      const method = id ? 'POST' : 'PUT';
      const url = id ? `/api/watched-directories/${{id}}` : '/api/watched-directories';
      await fetch(url, {{
        method,
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }});
      resetSourceForm();
      await refreshStatus();
      window.location.reload();
    }}

    async function removeSource(id) {{
      await fetch(`/api/watched-directories/${{id}}`, {{ method: 'DELETE' }});
      await refreshStatus();
      window.location.reload();
    }}

    async function refreshStatus() {{
      const response = await fetch('/api/status', {{ cache: 'no-store' }});
      if (!response.ok) return;
      const data = await response.json();
      window.__lastStatusData = data;

      document.getElementById('service-state').textContent = data.service.paused ? 'paused' : 'running';
      document.getElementById('service-speed').textContent = `speed ${{data.service.speed_percent}}%`;
      const ingestInput = document.getElementById('ingest-workers');
      const scanInput = document.getElementById('scan-workers');
      const embedBatchInput = document.getElementById('embed-batch-size');
      const embedParallelInput = document.getElementById('embed-parallel-requests');
      const active = document.activeElement;
      if (!pendingWorkerUpdate && active !== ingestInput) {{
        ingestInput.value = data.service.configured_ingest_workers;
      }}
      if (!pendingWorkerUpdate && active !== scanInput) {{
        scanInput.value = data.service.configured_scan_workers;
      }}
      if (!pendingWorkerUpdate && active !== embedBatchInput) {{
        embedBatchInput.value = data.service.configured_embed_batch_size;
      }}
      if (!pendingWorkerUpdate && active !== embedParallelInput) {{
        embedParallelInput.value = data.service.configured_embed_parallel_requests;
      }}
      if (!pendingWorkerUpdate && !editingEmbedServers) {{
        for (let i = 0; i < 4; i++) {{
          const addr = document.getElementById(`embed-server-address-${{i}}`);
          const weight = document.getElementById(`embed-server-weight-${{i}}`);
          if (addr) {{
            addr.value = (data.service.embed_servers?.[i]?.address || '');
          }}
          if (weight) {{
            weight.value = Number(data.service.embed_servers?.[i]?.weight || 1);
          }}
        }}
      }}
      if (!pendingWorkerUpdate && !editingLlmServer) {{
        const llmInput = document.getElementById('llm-server-address');
        if (llmInput) {{
          llmInput.value = data.service.llm_server_address || '';
        }}
      }}
      const workerChip = document.getElementById('worker-restart-chip');
      if (workerChip) {{
        workerChip.textContent = data.service.worker_restart_required
          ? 'restart required'
          : `scan ${{data.service.active_scan_workers}}/${{data.service.configured_scan_workers}} · ingest ${{data.service.active_ingest_workers}}/${{data.service.configured_ingest_workers}}`;
        workerChip.className = `status-chip${{data.service.worker_restart_required ? ' warn' : ''}}`;
      }}
      document.getElementById('qdrant-state').textContent = data.qdrant.ok ? 'online' : 'offline';
      document.getElementById('qdrant-state').className = `value ${{data.qdrant.ok ? 'ok' : 'bad'}}`;
      document.getElementById('qdrant-collections').textContent = `Collections: ${{data.qdrant.collections.length ? data.qdrant.collections.join(', ') : '-'}}`;
      document.getElementById('task-counts-main').textContent = `${{data.task_counts.pending}} pending`;
      document.getElementById('task-counts-sub').textContent = `${{data.task_counts.running}} running, ${{data.task_counts.failed}} failed`;
      document.getElementById('file-counts-main').textContent = `${{data.file_counts.indexed}} indexed`;
      document.getElementById('file-counts-sub').textContent = `${{data.file_counts.pending}} pending, ${{data.file_counts.error}} error`;
      document.getElementById('current-file').textContent = shortPath(data.task.current_file);
      document.getElementById('current-task-meta').textContent =
        `${{data.task.task_type}} · ${{data.task.message || data.task.source}} · ${{data.task.progress_current}} / ${{data.task.progress_total}} · ${{data.task.progress_percent}}%`;
      document.getElementById('current-progress').value = data.task.progress_percent;
      window.__watchedDirs = data.watched_dirs || [];
      updateModelOptions(data.ollama);
      renderOllamaRuntime(data.ollama);
      updateSearchFilters(data);
      renderRuntimeWorkers(data.runtime);
      renderPlanning(data.planning);
      renderTasks(data.recent_tasks);
      renderLogs(data.logs);
    }}

    async function setSpeed(value) {{
      await fetch('/api/control/speed', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ speed_percent: Number(value) }})
      }});
    }}
    async function setIngestWorkers() {{
      const input = document.getElementById('ingest-workers');
      const value = Number(input.value || '1');
      pendingWorkerUpdate = true;
      const response = await fetch('/api/control/worker-count', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ ingest_workers: value }})
      }});
      if (response.ok) {{
        const data = await response.json();
        input.value = data.configured_ingest_workers;
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}
    async function setScanWorkers() {{
      const input = document.getElementById('scan-workers');
      const value = Number(input.value || '1');
      pendingWorkerUpdate = true;
      const response = await fetch('/api/control/scan-worker-count', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ scan_workers: value }})
      }});
      if (response.ok) {{
        const data = await response.json();
        input.value = data.configured_scan_workers;
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}
    async function setEmbedBatchSize() {{
      const input = document.getElementById('embed-batch-size');
      const value = Number(input.value || '1');
      pendingWorkerUpdate = true;
      const response = await fetch('/api/control/embed-batch-size', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ embed_batch_size: value }})
      }});
      if (response.ok) {{
        const data = await response.json();
        input.value = data.configured_embed_batch_size;
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}
    async function setEmbedParallelRequests() {{
      const input = document.getElementById('embed-parallel-requests');
      const value = Number(input.value || '1');
      pendingWorkerUpdate = true;
      const response = await fetch('/api/control/embed-parallel-requests', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ embed_parallel_requests: value }})
      }});
      if (response.ok) {{
        const data = await response.json();
        input.value = data.configured_embed_parallel_requests;
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}
    async function saveEmbedServers() {{
      pendingWorkerUpdate = true;
      const status = document.getElementById('embed-servers-status');
      if (status) {{
        status.textContent = 'Saving...';
        status.style.color = '';
      }}
      const servers = [];
      for (let i = 0; i < 4; i++) {{
        servers.push({{
          address: (document.getElementById(`embed-server-address-${{i}}`)?.value || '').trim(),
          weight: Number(document.getElementById(`embed-server-weight-${{i}}`)?.value || '1')
        }});
      }}
      const response = await fetch('/api/control/embed-servers', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ servers }})
      }});
      if (response.ok) {{
        const data = await response.json();
        const configured = Array.isArray(data.embed_servers) ? data.embed_servers : servers;
        for (let i = 0; i < 4; i++) {{
          const server = configured[i] || {{ address: '', weight: 1 }};
          const addr = document.getElementById(`embed-server-address-${{i}}`);
          const weight = document.getElementById(`embed-server-weight-${{i}}`);
          if (addr) addr.value = server.address || '';
          if (weight) weight.value = Number(server.weight || 1);
        }}
        if (status) {{
          status.textContent = 'Saved';
          status.style.color = '#166534';
        }}
        editingEmbedServers = false;
      }} else {{
        if (status) {{
          status.textContent = 'Save failed';
          status.style.color = '#b91c1c';
        }}
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}

    function bindEmbedServerInputs() {{
      for (let i = 0; i < 4; i++) {{
        const addr = document.getElementById(`embed-server-address-${{i}}`);
        const weight = document.getElementById(`embed-server-weight-${{i}}`);
        for (const input of [addr, weight]) {{
          if (!input) continue;
          input.addEventListener('focus', () => {{
            editingEmbedServers = true;
          }});
          input.addEventListener('input', () => {{
            editingEmbedServers = true;
          }});
        }}
      }}
    }}
    async function saveLlmServer() {{
      pendingWorkerUpdate = true;
      const status = document.getElementById('llm-server-status');
      const input = document.getElementById('llm-server-address');
      if (status) {{
        status.textContent = 'Saving...';
        status.style.color = '';
      }}
      const address = (input?.value || '').trim();
      const response = await fetch('/api/control/llm-server', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ address }})
      }});
      if (response.ok) {{
        const data = await response.json();
        if (input) {{
          input.value = data.llm_server_address || '';
        }}
        if (status) {{
          status.textContent = 'Saved';
          status.style.color = '#166534';
        }}
        editingLlmServer = false;
      }} else {{
        if (status) {{
          status.textContent = 'Save failed';
          status.style.color = '#b91c1c';
        }}
      }}
      pendingWorkerUpdate = false;
      await refreshStatus();
    }}

    function bindLlmServerInput() {{
      const input = document.getElementById('llm-server-address');
      if (!input) return;
      input.addEventListener('focus', () => {{
        editingLlmServer = true;
      }});
      input.addEventListener('input', () => {{
        editingLlmServer = true;
      }});
    }}
    async function restartService() {{
      const button = event?.target;
      if (button) {{
        button.disabled = true;
        button.textContent = 'Restarting...';
      }}
      await fetch('/api/control/restart-service', {{ method: 'POST' }});
      setTimeout(() => window.location.reload(), 2500);
    }}
    async function togglePause(value) {{
      await fetch('/api/control/pause', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ paused: value }})
      }});
      window.location.reload();
    }}
    async function queueScan() {{
      const button = event?.target;
      if (button) {{
        button.disabled = true;
        button.textContent = 'Queued...';
      }}
      await fetch('/api/control/scan', {{ method: 'POST' }});
      await refreshStatus();
      if (button) {{
        setTimeout(() => {{
          button.disabled = false;
          button.textContent = 'Scan now';
        }}, 1200);
      }}
    }}
    async function queueDirectoryScan(path) {{
      const response = await fetch('/api/control/scan-directory', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ path }})
      }});
      if (!response.ok) return;
      await refreshStatus();
    }}
    async function retryFailed() {{
      const button = event?.target;
      if (button) {{
        button.disabled = true;
        button.textContent = 'Retrying...';
      }}
      await fetch('/api/control/retry-failed', {{ method: 'POST' }});
      await refreshStatus();
      if (button) {{
        setTimeout(() => {{
          button.disabled = false;
          button.textContent = 'Retry failed';
        }}, 1200);
      }}
    }}
    loadDbOverview();
    resetSourceForm();
    bindEmbedServerInputs();
    bindLlmServerInput();
    document.getElementById('search-query').addEventListener('keydown', (event) => {{
      if (event.key === 'Enter' && event.shiftKey) runAsk();
      else if (event.key === 'Enter') runSearch();
    }});
    setInterval(refreshStatus, 2000);
  </script>
</body>
</html>"""


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "rag-service/0.2"

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        if self.path == "/":
            body = render_index(SERVICE.snapshot()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/api/ask"):
            from urllib.parse import parse_qs, urlparse

            query = parse_qs(urlparse(self.path).query)
            search_query = query.get("q", [""])[0]
            limit = int(query.get("limit", [str(SEARCH_TOP_K)])[0])
            model = query.get("model", [""])[0]
            detail = query.get("detail", ["standard"])[0]
            language = query.get("language", ["fr"])[0]
            collection_name = query.get("collection", [""])[0]
            source_type = query.get("source_type", [""])[0]
            limit = max(1, min(20, limit))
            self._send_json(
                SERVICE.ask(
                    search_query,
                    limit=limit,
                    model=model,
                    detail_level=detail,
                    answer_language=language,
                    collection_name=collection_name,
                    source_type=source_type,
                )
            )
            return
        if self.path.startswith("/api/search"):
            from urllib.parse import parse_qs, urlparse

            query = parse_qs(urlparse(self.path).query)
            search_query = query.get("q", [""])[0]
            limit = int(query.get("limit", [str(SEARCH_TOP_K)])[0])
            collection_name = query.get("collection", [""])[0]
            source_type = query.get("source_type", [""])[0]
            limit = max(1, min(20, limit))
            self._send_json(SERVICE.search(search_query, limit=limit, collection_name=collection_name, source_type=source_type))
            return
        if self.path == "/api/status":
            self._send_json(SERVICE.snapshot())
            return
        if self.path == "/api/db/overview":
            self._send_json({"tables": STORAGE.db_overview()})
            return
        if self.path.startswith("/api/db/table"):
            from urllib.parse import parse_qs, urlparse

            query = parse_qs(urlparse(self.path).query)
            table = query.get("name", [""])[0]
            limit = int(query.get("limit", ["50"])[0])
            offset = int(query.get("offset", ["0"])[0])
            self._send_json(STORAGE.table_rows(table, limit=limit, offset=offset))
            return
        if self.path == "/health":
            self._send_json({"ok": True, "service": "rag-service", "port": PORT})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        if self.path.startswith("/api/watched-directories/"):
            payload = self._read_json()
            watched_id = int(self.path.rsplit("/", 1)[-1])
            STORAGE.update_watched_dir(
                watched_id=watched_id,
                path_configured=payload.get("path", ""),
                source_type=payload.get("source_type", "documents"),
                collection_name=payload.get("collection_name", VECTOR_COLLECTION),
                parser_profiles=payload.get("parser_profiles", ""),
                enabled=bool(payload.get("enabled", True)),
                recursive=bool(payload.get("recursive", True)),
                scan_interval_seconds=int(payload.get("scan_interval_seconds", 0)),
            )
            self._send_json({"ok": True, "id": watched_id})
            return
        if self.path == "/api/control/speed":
            payload = self._read_json()
            SERVICE.set_speed(int(payload.get("speed_percent", 100)))
            self._send_json({"ok": True, "speed_percent": int(SERVICE.speed_ratio() * 100)})
            return
        if self.path == "/api/control/worker-count":
            payload = self._read_json()
            count = SERVICE.set_ingest_worker_count(int(payload.get("ingest_workers", INGEST_WORKER_COUNT)))
            self._send_json(
                {
                    "ok": True,
                    "configured_ingest_workers": count,
                    "active_ingest_workers": SERVICE.active_ingest_worker_count(),
                    "restart_required": SERVICE.active_ingest_worker_count() != count,
                }
            )
            return
        if self.path == "/api/control/scan-worker-count":
            payload = self._read_json()
            count = SERVICE.set_scan_worker_count(int(payload.get("scan_workers", SCAN_WORKER_COUNT)))
            self._send_json(
                {
                    "ok": True,
                    "configured_scan_workers": count,
                    "active_scan_workers": SERVICE.active_scan_worker_count(),
                    "restart_required": SERVICE.active_scan_worker_count() != count,
                }
            )
            return
        if self.path == "/api/control/embed-batch-size":
            payload = self._read_json()
            count = SERVICE.set_embed_batch_size(int(payload.get("embed_batch_size", EMBED_BATCH_SIZE)))
            self._send_json({"ok": True, "configured_embed_batch_size": count})
            return
        if self.path == "/api/control/embed-parallel-requests":
            payload = self._read_json()
            count = SERVICE.set_embed_parallel_requests(int(payload.get("embed_parallel_requests", EMBED_PARALLEL_REQUESTS)))
            self._send_json({"ok": True, "configured_embed_parallel_requests": count})
            return
        if self.path == "/api/control/embed-servers":
            payload = self._read_json()
            servers = payload.get("servers", []) if isinstance(payload, dict) else []
            configured = SERVICE.set_embed_servers(servers if isinstance(servers, list) else [])
            self._send_json({"ok": True, "embed_servers": configured})
            return
        if self.path == "/api/control/llm-server":
            payload = self._read_json()
            address = payload.get("address", "") if isinstance(payload, dict) else ""
            configured = SERVICE.set_llm_server_address(str(address))
            self._send_json({"ok": True, "llm_server_address": configured})
            return
        if self.path == "/api/control/restart-service":
            STORAGE.add_event("info", "control", "service restart requested from UI")
            self._send_json({"ok": True, "message": "restart requested"})
            trigger_service_restart()
            return
        if self.path == "/api/control/pause":
            payload = self._read_json()
            SERVICE.set_paused(bool(payload.get("paused", True)))
            self._send_json({"ok": True, "paused": SERVICE.is_paused()})
            return
        if self.path == "/api/control/scan":
            SERVICE.schedule_scan_tasks(manual=True)
            self._send_json({"ok": True, "message": "scan queued"})
            return
        if self.path == "/api/control/scan-directory":
            payload = self._read_json()
            watched_path = str(payload.get("path", "")).strip()
            if not watched_path:
                self._send_json({"error": "missing path"}, status=400)
                return
            SERVICE.schedule_scan_tasks(manual=True, watched_path=watched_path)
            self._send_json({"ok": True, "message": "directory scan queued", "path": watched_path})
            return
        if self.path == "/api/control/retry-failed":
            payload = self._read_json()
            limit = int(payload.get("limit", FAILED_RETRY_BATCH_SIZE)) if payload else FAILED_RETRY_BATCH_SIZE
            limit = max(1, min(100, limit))
            queued = SERVICE.retry_failed_files(limit=limit)
            self._send_json({"ok": True, "queued": queued})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_PUT(self) -> None:
        if self.path == "/api/watched-directories":
            payload = self._read_json()
            watched_id = STORAGE.add_watched_dir(
                path_configured=payload.get("path", ""),
                source_type=payload.get("source_type", "documents"),
                collection_name=payload.get("collection_name", VECTOR_COLLECTION),
                parser_profiles=payload.get("parser_profiles", ""),
                enabled=bool(payload.get("enabled", True)),
                recursive=bool(payload.get("recursive", True)),
                scan_interval_seconds=int(payload.get("scan_interval_seconds", 0)),
            )
            self._send_json({"ok": True, "id": watched_id}, status=201)
            return
        self._send_json({"error": "not found"}, status=404)

    def do_DELETE(self) -> None:
        if self.path.startswith("/api/watched-directories/"):
            watched_id = int(self.path.rsplit("/", 1)[-1])
            STORAGE.delete_watched_dir(watched_id)
            self._send_json({"ok": True, "id": watched_id})
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        message = fmt % args
        if (
            "\"GET /api/status HTTP/" in message
            or "\"GET /health HTTP/" in message
            or "\"GET / HTTP/" in message
        ):
            return
        STORAGE.add_event("info", "http", message)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def worker_process_entry(worker_name: str, allowed_task_types: tuple[str, ...], schedule_scans: bool = False) -> None:
    local_storage = Storage(DB_PATH)
    local_storage.clear_worker_runtime(os.getpid())
    atexit.register(lambda: local_storage.clear_worker_runtime(os.getpid()))
    local_service = Service(local_storage, emit_init_event=False)
    local_service.worker_loop(worker_name, set(allowed_task_types), schedule_scans=schedule_scans)


def state_refresh_loop() -> None:
    while True:
        try:
            SERVICE.refresh_qdrant()
            SERVICE.refresh_ollama_models()
        except Exception:
            pass
        time.sleep(5)


def scheduler_loop() -> None:
    last_scan_schedule = utc_ts()
    last_pending_reconcile = 0.0
    while True:
        try:
            if not SERVICE.is_paused() and utc_ts() - last_scan_schedule >= SCAN_INTERVAL_SECONDS:
                SERVICE.schedule_scan_tasks()
                last_scan_schedule = utc_ts()
            if not SERVICE.is_paused() and utc_ts() - last_pending_reconcile >= SCAN_MAINTENANCE_INTERVAL_SECONDS:
                queued = STORAGE.requeue_orphan_pending_files()
                if queued:
                    STORAGE.add_event("info", "queue", f"requeued {queued} orphan pending files")
                last_pending_reconcile = utc_ts()
        except Exception as exc:
            try:
                STORAGE.add_event("warn", "scheduler", f"scheduler loop error: {exc}")
            except Exception:
                pass
        time.sleep(2)


def trigger_service_restart() -> None:
    def _restart() -> None:
        time.sleep(1)
        try:
            subprocess.Popen(
                ["systemctl", "--user", "restart", "rag-service"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as exc:
            try:
                STORAGE.add_event("error", "control", f"restart failed: {exc}")
            except Exception:
                pass

    threading.Thread(target=_restart, daemon=True).start()


def stop_worker_processes() -> None:
    processes = list(WORKER_PROCESSES)
    WORKER_PROCESSES.clear()
    for process in processes:
        if process.is_alive():
            process.terminate()
    for process in processes:
        process.join(timeout=2)


def main() -> None:
    STORAGE.add_event("info", "service", f"binding http server on {HOST}:{PORT}")
    STORAGE.clear_worker_runtime(os.getpid())
    reprioritized = STORAGE.recompute_pending_task_priorities()
    if reprioritized:
        STORAGE.add_event("info", "queue", f"reprioritized {reprioritized} pending/running tasks")
    atexit.register(stop_worker_processes)
    threading.Thread(
        target=state_refresh_loop,
        daemon=True,
    ).start()
    threading.Thread(
        target=scheduler_loop,
        daemon=True,
    ).start()
    ctx = multiprocessing.get_context("spawn")
    scan_worker_count = SERVICE.configured_scan_worker_count()
    for worker_index in range(scan_worker_count):
        process = ctx.Process(
            name=f"scan-worker-{worker_index + 1}",
            target=worker_process_entry,
            args=(f"scan-worker-{worker_index + 1}", ("scan_directory",), False),
        )
        process.start()
        WORKER_PROCESSES.append(process)
    ingest_worker_count = SERVICE.configured_ingest_worker_count()
    for worker_index in range(ingest_worker_count):
        process = ctx.Process(
            name=f"ingest-worker-{worker_index + 1}",
            target=worker_process_entry,
            args=(f"ingest-worker-{worker_index + 1}", ("index_file", "summarize_file", "delete_file", "reindex_mbox"), False),
        )
        process.start()
        WORKER_PROCESSES.append(process)
    STORAGE.add_event(
        "info",
        "service",
        f"started {scan_worker_count} scan worker processes and {ingest_worker_count} ingest worker processes",
    )
    with ReusableThreadingHTTPServer((HOST, PORT), RequestHandler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--read-file-payload":
        print(json.dumps(_build_file_payload(sys.argv[2], sys.argv[3] if len(sys.argv) >= 4 else "")), flush=True)
        raise SystemExit(0)
    main()
