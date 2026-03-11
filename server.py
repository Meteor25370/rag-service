#!/usr/bin/env python3
import email
import html
import hashlib
import json
import mailbox
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from email import policy
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


HOST = "0.0.0.0"
PORT = int(os.environ.get("RAG_SERVICE_PORT", "6334"))
SERVICE_HOME = Path(os.environ.get("RAG_SERVICE_HOME", str(Path(__file__).resolve().parent)))
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = SERVICE_HOME / "rag-service.db"
QDRANT_URL = "http://127.0.0.1:6333/collections"
OLLAMA_EMBED_URL = "http://127.0.0.1:11434/api/embed"
OLLAMA_GENERATE_URL = "http://127.0.0.1:11434/api/generate"
OLLAMA_TAGS_URL = "http://127.0.0.1:11434/api/tags"
EMBED_MODEL = "embeddinggemma:latest"
LLM_MODEL = os.environ.get("RAG_SERVICE_LLM_MODEL", "qwen2.5-coder:32b")
SUMMARY_MODEL = os.environ.get("RAG_SERVICE_SUMMARY_MODEL", LLM_MODEL)
VECTOR_COLLECTION = "global_knowledge"
THREE_GPP_COLLECTION = "mcx_3gpp"
SCAN_INTERVAL_SECONDS = 300
MAX_LOGS = 300
INDEX_SLEEP_SECONDS = 0.25
CHUNK_SIZE = 1400
CHUNK_OVERLAP = 180
EMBED_BATCH_SIZE = 8
SEARCH_TOP_K = 6
INGEST_WORKER_COUNT = int(os.environ.get("RAG_SERVICE_INGEST_WORKERS", "4"))
FILE_IO_TIMEOUT_SECONDS = int(os.environ.get("RAG_SERVICE_FILE_IO_TIMEOUT_SECONDS", "20"))
FAILED_RETRY_BATCH_SIZE = int(os.environ.get("RAG_SERVICE_FAILED_RETRY_BATCH_SIZE", "8"))
FAILED_RETRY_MAX_ATTEMPTS = int(os.environ.get("RAG_SERVICE_FAILED_RETRY_MAX_ATTEMPTS", "3"))
SUMMARY_MAX_CHARS = int(os.environ.get("RAG_SERVICE_SUMMARY_MAX_CHARS", "12000"))
THUNDERBIRD_ATTACHMENT_MAX_BYTES = int(os.environ.get("RAG_SERVICE_THUNDERBIRD_ATTACHMENT_MAX_BYTES", str(10 * 1024 * 1024)))
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
    ("/home/egarcia/qdrant/rag-docs/streamwide", "documents", VECTOR_COLLECTION, "generic_text,pdf,office"),
    ("/home/egarcia/qdrant/rag-docs/email-agent", "code", VECTOR_COLLECTION, "generic_text,code"),
    ("/home/egarcia/qdrant/rag-docs/thunderbird", "thunderbird", VECTOR_COLLECTION, "thunderbird_mbox,email_attachments"),
    ("/data/3gpp", "3gpp", THREE_GPP_COLLECTION, "generic_text,pdf,office,3gpp_spec"),
]
AUTO_SCAN_SOURCE_TYPES = {"code"}
SOURCE_SCAN_INTERVALS = {
    "code": 300,
    "documents": 0,
    "thunderbird": 0,
    "3gpp": 0,
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
    ".zip",
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


def should_keep(path: Path, source_type: str, parser_profiles: str | list[str] | None = None) -> bool:
    if not path.is_file():
        return False

    if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
        return False

    if ".sbd" in path.parts:
        return False

    suffix = path.suffix.lower()
    if suffix in EXCLUDED_SUFFIXES:
        return False

    profiles = set(normalize_parser_profiles(parser_profiles))

    if source_type == "thunderbird" or "thunderbird_mbox" in profiles:
        return path.name not in {".DS_Store"}

    if suffix == ".pdf" and "pdf" not in profiles:
        return False
    if suffix in OFFICE_SUFFIXES and "office" not in profiles:
        return False
    if suffix in {".py", ".js", ".ts", ".jsx", ".tsx", ".java", ".kt", ".c", ".cpp", ".h", ".hpp", ".sh", ".sql"}:
        if "code" not in profiles and source_type == "code":
            return False

    return suffix in ALLOWED_SUFFIXES


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
    return [{"chunk_text": piece, "clause_id": "", "clause_title": ""} for piece in chunk_text(text)]


def read_text_file(path: Path, parser_profiles: str | list[str] | None = None) -> str:
    profiles = set(normalize_parser_profiles(parser_profiles))
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


def normalize_text(text: str) -> str:
    text = text.replace("\x00", " ")
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines).strip()


def clean_html_text(raw_html: str) -> str:
    text = re.sub(r"<[^<]+?>", " ", raw_html or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def embed_texts(texts: list[str]) -> list[list[float]]:
    response = http_json(
        "POST",
        OLLAMA_EMBED_URL,
        {
            "model": EMBED_MODEL,
            "input": texts,
        },
    )
    embeddings = response.get("embeddings") or []
    if not embeddings:
        raise RuntimeError("ollama returned no embeddings")
    return embeddings


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
    payload = http_json("GET", OLLAMA_TAGS_URL, timeout=20)
    models = []
    for item in payload.get("models", []) or []:
        name = item.get("name")
        if name:
            models.append(str(name))
    return models


def generate_answer(prompt: str, model: str) -> str:
    response = http_json(
        "POST",
        OLLAMA_GENERATE_URL,
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
    return generate_answer(prompt, model)


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_db()
        self._bootstrap_defaults()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
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
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('speed_ratio', '1.0') ON CONFLICT(key) DO NOTHING"
            )
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('paused', '0') ON CONFLICT(key) DO NOTHING"
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
                INSERT INTO tasks(task_type, source_path, source_type, status, message, created_at)
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (task_type, source_path, source_type, message, utc_now()),
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
                    ORDER BY id
                    LIMIT 1
                    """,
                    tuple(sorted(allowed_task_types)),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE status = 'pending'
                    ORDER BY id
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
                SELECT id, task_type, source_path, source_type, status, progress_percent, message,
                       created_at, started_at, finished_at, last_error
                FROM tasks
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

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
                        INSERT INTO tasks(task_type, source_path, source_type, status, message, created_at)
                        VALUES (?, ?, ?, 'pending', ?, ?)
                        """,
                        (task_type, path, source_type, "file changed", now),
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
                    SET status = 'pending', updated_at = ?
                    WHERE source_path = ?
                    """,
                    (utc_now(), row["source_path"]),
                )
                conn.execute(
                    """
                    INSERT INTO tasks(task_type, source_path, source_type, status, message, created_at)
                    VALUES (?, ?, ?, 'pending', ?, ?)
                    """,
                    (task_type, row["source_path"], row["source_type"], "retry failed file", utc_now()),
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
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.lock = threading.Lock()
        self.qdrant_ok = False
        self.qdrant_collections: list[str] = []
        self.ollama_models: list[str] = [LLM_MODEL]
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

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            qdrant_ok = self.qdrant_ok
            qdrant_collections = list(self.qdrant_collections)
            ollama_models = list(self.ollama_models)
        return {
            "service": {
                "name": "rag-service",
                "host": HOST,
                "port": PORT,
                "started_at": utc_now(),
                "paused": self.is_paused(),
                "speed_ratio": self.speed_ratio(),
                "speed_percent": int(self.speed_ratio() * 100),
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
        embeddings = embed_texts([query])
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
                        f"type: {item['source_type']}",
                        f"spec: {item.get('spec_id') or '-'} {item.get('spec_version') or ''} {item.get('spec_release') or ''}".strip(),
                        f"clause: {item.get('clause_id') or '-'} {item.get('clause_title') or ''}".strip(),
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
        prompt = (
            "You answer from a local knowledge base.\n"
            "Use only the provided context.\n"
            "If the context is insufficient, say exactly what is missing.\n"
            "Do not invent facts, clauses, releases, procedures, APIs, or behaviors.\n"
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
        answer = generate_answer(prompt, chosen_model)
        self.storage.add_event("info", "search", f"llm answer generated: {query[:80]}")
        return {
            "query": query,
            "answer": answer,
            "results": formatted,
            "model": chosen_model,
            "detail_level": detail_level,
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

    def schedule_scan_tasks(self, source_type: str | None = None, manual: bool = False) -> None:
        now_ts = utc_ts()
        for item in self.storage.get_watched_dirs():
            if not item["enabled"]:
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
                    if total_seen % 250 == 0:
                        self.storage.update_task_progress(
                            task["id"],
                            total_seen,
                            0,
                            f"scanning {short_path(str(candidate))}",
                        )
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

                if kept_count % 25 == 0 or changed:
                    self.storage.update_task_progress(
                        task["id"],
                        kept_count,
                        0,
                        f"queued {queued_count} after {short_path(path_str)}",
                    )

        return kept_count, queued_count, seen_paths

    def worker_loop(self, worker_name: str, allowed_task_types: set[str], schedule_scans: bool = False) -> None:
        last_scan_schedule = utc_ts()
        last_pending_reconcile = 0.0
        while True:
            if schedule_scans:
                self.refresh_qdrant()
                self.refresh_ollama_models()
            if schedule_scans and not self.is_paused() and utc_ts() - last_scan_schedule >= SCAN_INTERVAL_SECONDS:
                self.schedule_scan_tasks()
                last_scan_schedule = utc_ts()
            if schedule_scans and not self.is_paused() and utc_ts() - last_pending_reconcile >= 15:
                queued = self.storage.requeue_orphan_pending_files()
                if queued:
                    self.storage.add_event("info", "queue", f"requeued {queued} orphan pending files")
                last_pending_reconcile = utc_ts()

            if self.is_paused():
                time.sleep(1)
                continue

            task = self.storage.next_task(allowed_task_types)
            if task is None:
                time.sleep(1)
                continue

            try:
                if task["task_type"] == "scan_directory":
                    self._run_scan_directory(task)
                elif task["task_type"] == "index_file":
                    self._run_index_file(task)
                elif task["task_type"] == "reindex_mbox":
                    self._run_reindex_mbox(task)
                elif task["task_type"] == "delete_file":
                    self._run_delete_file(task)
                else:
                    self.storage.finish_task(task["id"], "ignored unknown task")
            except Exception as exc:
                self.storage.fail_task(task["id"], str(exc))
                self.storage.add_event("error", "task", f"{worker_name}:{task['task_type']} failed: {exc}")

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

        path = Path(task["source_path"])
        file_row = self.storage.get_file_record(str(path))
        collection_name = (file_row["collection_name"] if file_row and file_row["collection_name"] else VECTOR_COLLECTION)
        parser_profiles = (
            file_row["parser_profiles"]
            if file_row and file_row["parser_profiles"]
            else default_parser_profiles_for_source(task["source_type"] or "")
        )
        self.storage.update_task_progress(task["id"], 0, 6, "checking file")
        self.storage.update_task_progress(task["id"], 1, 6, "reading file")
        try:
            digest, text = read_file_payload_with_timeout(path, parser_profiles)
        except FileNotFoundError:
            self.storage.fail_task(task["id"], "file missing")
            self.storage.mark_file_error(str(path), "file missing")
            return
        except TimeoutError as exc:
            message = f"file read timeout: {exc}"
            self.storage.fail_task(task["id"], message)
            self.storage.mark_file_error(str(path), message)
            self.storage.add_event("warn", "index", f"timeout on {short_path(str(path))}")
            return
        except OSError as exc:
            self.storage.fail_task(task["id"], f"file read failed: {exc}")
            self.storage.mark_file_error(str(path), f"file read failed: {exc}")
            return

        source_type = task["source_type"] or ""
        if not text:
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "empty text file")
            self.storage.add_event("warn", "index", f"empty text for {short_path(str(path))}")
            return

        self.storage.update_task_progress(task["id"], 2, 6, "preparing chunks")
        profiles = set(normalize_parser_profiles(parser_profiles))
        source_metadata = parse_3gpp_filename_metadata(path, text) if source_type == "3gpp" or "3gpp_spec" in profiles else {}
        chunk_entries = build_chunks_for_source(source_type, text, parser_profiles)
        if not chunk_entries:
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "no chunks produced")
            return

        self.storage.update_task_progress(task["id"], 3, 6, "requesting embeddings")
        all_embeddings: list[list[float]] = []
        chunk_texts = [entry["chunk_text"] for entry in chunk_entries]
        total_batches = max(1, (len(chunk_texts) + EMBED_BATCH_SIZE - 1) // EMBED_BATCH_SIZE)
        for batch_index in range(total_batches):
            start = batch_index * EMBED_BATCH_SIZE
            end = start + EMBED_BATCH_SIZE
            embeddings = embed_texts(chunk_texts[start:end])
            all_embeddings.extend(embeddings)
            self.storage.update_task_progress(
                task["id"],
                3 + batch_index,
                3 + total_batches + 2,
                f"embedded batch {batch_index + 1}/{total_batches}",
            )
            time.sleep(INDEX_SLEEP_SECONDS * (1 / self.speed_ratio()))

        if len(all_embeddings) != len(chunk_entries):
            raise RuntimeError("embedding count mismatch")

        self.storage.update_task_progress(task["id"], 3 + total_batches, 3 + total_batches + 2, "syncing qdrant")
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
                        "spec_id": source_metadata.get("spec_id", ""),
                        "spec_version": source_metadata.get("spec_version", ""),
                        "spec_release": source_metadata.get("spec_release", ""),
                        "spec_stage": source_metadata.get("spec_stage"),
                    },
                }
            )

        self.storage.update_task_progress(task["id"], 4 + total_batches, 3 + total_batches + 2, "uploading points")
        upsert_qdrant_points(collection_name, points)
        self.storage.mark_file_indexed(str(path), digest, points_count=len(points))
        self.storage.update_task_progress(task["id"], 5 + total_batches, 3 + total_batches + 3, "building document memory")
        try:
            summary_text = summarize_document_text(
                text,
                source_path=str(path),
                source_type=source_type,
                model=SUMMARY_MODEL,
            ).strip()
            if summary_text:
                self.storage.update_file_summary(str(path), summary_text, SUMMARY_MODEL)
        except Exception as exc:
            self.storage.add_event("warn", "summary", f"summary failed for {short_path(str(path))}: {exc}")
        self.storage.finish_task(task["id"], f"indexed {len(points)} chunks")
        self.storage.add_event("info", "index", f"indexed {short_path(str(path))} ({len(points)} chunks)")

    def _run_reindex_mbox(self, task: sqlite3.Row) -> None:
        path = Path(task["source_path"])
        file_row = self.storage.get_file_record(str(path))
        collection_name = (file_row["collection_name"] if file_row and file_row["collection_name"] else VECTOR_COLLECTION)
        parser_profiles = (
            file_row["parser_profiles"]
            if file_row and file_row["parser_profiles"]
            else default_parser_profiles_for_source("thunderbird")
        )

        self.storage.update_task_progress(task["id"], 0, 5, "opening mailbox")
        if not path.exists():
            self.storage.fail_task(task["id"], "mailbox file missing")
            self.storage.mark_file_error(str(path), "mailbox file missing")
            return

        try:
            digest = content_hash(path)
            messages = parse_mbox_messages(path, parser_profiles)
        except Exception as exc:
            self.storage.fail_task(task["id"], f"mailbox parse failed: {exc}")
            self.storage.mark_file_error(str(path), f"mailbox parse failed: {exc}")
            return

        if not messages:
            delete_qdrant_points_by_file(collection_name, file_ingest_id(str(path)))
            self.storage.mark_file_indexed(str(path), digest, points_count=0)
            self.storage.finish_task(task["id"], "no messages parsed from mailbox")
            self.storage.add_event("warn", "thunderbird", f"no messages parsed for {short_path(str(path))}")
            return

        self.storage.update_task_progress(task["id"], 1, 5, f"parsed {len(messages)} messages")
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
            return

        self.storage.update_task_progress(task["id"], 2, 5, "requesting embeddings")
        all_embeddings: list[list[float]] = []
        chunk_texts = [entry["chunk_text"] for entry in chunk_entries]
        total_batches = max(1, (len(chunk_texts) + EMBED_BATCH_SIZE - 1) // EMBED_BATCH_SIZE)
        for batch_index in range(total_batches):
            start = batch_index * EMBED_BATCH_SIZE
            end = start + EMBED_BATCH_SIZE
            embeddings = embed_texts(chunk_texts[start:end])
            all_embeddings.extend(embeddings)
            self.storage.update_task_progress(
                task["id"],
                2 + batch_index,
                2 + total_batches + 2,
                f"embedded batch {batch_index + 1}/{total_batches}",
            )
            time.sleep(INDEX_SLEEP_SECONDS * (1 / self.speed_ratio()))

        if len(all_embeddings) != len(chunk_entries):
            raise RuntimeError("embedding count mismatch in thunderbird pipeline")

        self.storage.update_task_progress(task["id"], 2 + total_batches, 2 + total_batches + 2, "syncing qdrant")
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
    qdrant_status = "online" if qdrant["ok"] else "offline"
    watched_rows = "\n".join(
        (
            f"<tr>"
            f"<td>{item['source_type']}</td>"
            f"<td title='{item['path']}'>{short_path(item['path'])}</td>"
            f"<td>{item.get('collection_name') or VECTOR_COLLECTION}</td>"
            f"<td title='{item.get('parser_profiles') or ''}'>{short_path(item.get('parser_profiles') or '-', 48)}</td>"
            f"<td>{'yes' if item['exists'] else 'no'}</td>"
            f"<td>{'on' if item['enabled'] else 'off'}</td>"
            f"<td>{item.get('last_status') or '-'}</td>"
            f"<td><button class='linkish' onclick='editSource({item['id']})'>Edit</button> "
            f"<button class='linkish' onclick='removeSource({item['id']})'>Delete</button></td>"
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
    db_rows = "\n".join(
        f"<tr><td><button class='linkish' onclick=\"loadTable('{item['table']}')\">{item['table']}</button></td><td>{item['count']}</td><td>{', '.join(item['columns'][:6])}{' ...' if len(item['columns']) > 6 else ''}</td></tr>"
        for item in db_overview
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
    .split {{ display: grid; grid-template-columns: minmax(360px, 1.1fr) minmax(420px, 1.4fr); gap: 12px; margin-top: 12px; }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 16px; }}
    .label {{ text-transform: uppercase; font-size: 11px; color: var(--muted); letter-spacing: 0.06em; margin-bottom: 6px; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .ok {{ color: var(--ok); }}
    .bad {{ color: var(--bad); }}
    .controls {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-top: 16px; }}
    .form-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px 12px; margin-bottom: 12px; }}
    .field {{ display: grid; gap: 4px; }}
    .field label {{ font-size: 12px; color: var(--muted); }}
    .field input[type="text"], .field input[type="number"], .field select {{ width: 100%; padding: 9px 10px; border: 1px solid var(--line); border-radius: 8px; font: inherit; }}
    .field.check {{ display: flex; align-items: center; gap: 8px; padding-top: 22px; }}
    .tabs {{ display: flex; gap: 8px; margin: 0 0 12px; }}
    .tab-btn {{ background: #e5e7eb; color: var(--ink); }}
    .tab-btn.active {{ background: var(--accent); color: #fff; }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .search-bar {{ display: grid; grid-template-columns: minmax(240px, 1fr) 110px minmax(180px, 240px) 140px minmax(160px, 220px) minmax(140px, 180px) auto; gap: 10px; align-items: end; }}
    .search-actions {{ display: inline-flex; gap: 4px; align-items: center; padding: 4px; border: 1px solid var(--line); border-radius: 12px; background: #eef2ff; }}
    .search-actions button {{ border-radius: 8px; padding: 8px 14px; }}
    .search-actions button.secondary {{ background: #dbe7ff; color: #1d4ed8; }}
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
    button.linkish {{ background: transparent; color: var(--accent); padding: 0; border-radius: 0; }}
    input[type="range"] {{ width: 240px; accent-color: var(--accent); }}
    progress {{ width: 100%; height: 16px; accent-color: var(--accent); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 14px; }}
    .db-table th, .db-table td {{ font-size: 12px; padding: 7px 6px; }}
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
    <div class="sub">Ingest status on port {service['port']}.</div>

    <div class="grid">
      <section class="card"><div class="label">Service</div><div id="service-state" class="value">{'paused' if service['paused'] else 'running'}</div><div id="service-speed" class="muted">speed {service['speed_percent']}%</div></section>
      <section class="card"><div class="label">Qdrant</div><div id="qdrant-state" class="value {'ok' if qdrant['ok'] else 'bad'}">{qdrant_status}</div><div id="qdrant-collections" class="muted">Collections: {collections}</div></section>
      <section class="card"><div class="label">Tasks</div><div id="task-counts-main" class="value">{task_counts['pending']} pending</div><div id="task-counts-sub" class="muted">{task_counts['running']} running, {task_counts['failed']} failed</div></section>
      <section class="card"><div class="label">Files</div><div id="file-counts-main" class="value">{file_counts['indexed']} indexed</div><div id="file-counts-sub" class="muted">{file_counts['pending']} pending, {file_counts['error']} error</div></section>
    </div>

    <section class="card">
      <div class="label">Current Task</div>
      <div id="current-file" style="font-size:18px;font-weight:700;margin-bottom:6px;">{short_path(task['current_file'])}</div>
      <div id="current-task-meta" class="muted" style="margin-bottom:10px;">{task['task_type']} · {task['message'] or task['source']} · {task['progress_current']} / {task['progress_total']} · {task['progress_percent']}%</div>
      <progress id="current-progress" max="100" value="{task['progress_percent']}"></progress>
      <div class="controls">
        <label for="speed">Speed</label>
        <input id="speed" type="range" min="10" max="100" value="{service['speed_percent']}" oninput="speedValue.textContent=this.value + '%'" onchange="setSpeed(this.value)">
        <strong id="speedValue">{service['speed_percent']}%</strong>
        <button onclick="queueScan()">Scan now</button>
        <button onclick="retryFailed()">Retry failed</button>
        <button onclick="togglePause({str(not service['paused']).lower()})">{'Resume' if service['paused'] else 'Pause'}</button>
        <button class="secondary" onclick="window.location.reload()">Refresh</button>
      </div>
    </section>

    <div class="tabs">
      <button id="tab-overview-btn" class="tab-btn active" onclick="showTab('overview')">Overview</button>
      <button id="tab-search-btn" class="tab-btn" onclick="showTab('search')">Search</button>
      <button id="tab-db-btn" class="tab-btn" onclick="showTab('db')">SQLite</button>
    </div>

    <div id="tab-overview" class="tab-panel active">
      <div class="split">
        <section class="card">
          <div class="label">Watched Directories</div>
          <input id="source-id" type="hidden">
          <div class="form-grid">
            <div class="field" style="grid-column: span 2;">
              <label for="source-path">Source path</label>
              <input id="source-path" type="text" placeholder="/path/to/source">
            </div>
            <div class="field">
              <label for="source-type">Source type</label>
              <select id="source-type" onchange="applyDefaultParsersForType()">
                <option value="documents">documents</option>
                <option value="code">code</option>
                <option value="3gpp">3gpp</option>
                <option value="thunderbird">thunderbird</option>
              </select>
            </div>
            <div class="field">
              <label for="source-collection">Qdrant collection</label>
              <input id="source-collection" type="text" value="{VECTOR_COLLECTION}" placeholder="global_knowledge">
            </div>
            <div class="field" style="grid-column: span 2;">
              <label for="source-parsers">Parser profiles</label>
              <input id="source-parsers" type="text" value="generic_text,pdf,office" placeholder="generic_text,pdf,office">
            </div>
            <div class="field">
              <label for="source-interval">Scan interval (seconds)</label>
              <input id="source-interval" type="number" min="0" step="1" value="300">
            </div>
            <div class="field check">
              <input id="source-enabled" type="checkbox" checked>
              <label for="source-enabled">Enabled</label>
            </div>
            <div class="field check">
              <input id="source-recursive" type="checkbox" checked>
              <label for="source-recursive">Recursive</label>
            </div>
          </div>
          <div class="controls" style="margin-top:0;margin-bottom:12px;">
            <button onclick="saveSource()">Save source</button>
            <button class="secondary" onclick="resetSourceForm()">Clear</button>
          </div>
          <table>
            <thead><tr><th>Type</th><th>Path</th><th>Collection</th><th>Parsers</th><th>Exists</th><th>Enabled</th><th>Last scan</th><th>Actions</th></tr></thead>
            <tbody>{watched_rows}</tbody>
          </table>
        </section>
        <section class="card">
          <div class="label">Recent Tasks</div>
          <table>
            <thead><tr><th>Created</th><th>Type</th><th>Path</th><th>Status</th><th>%</th><th>Message</th></tr></thead>
            <tbody id="recent-tasks-body">{task_rows}</tbody>
          </table>
        </section>
      </div>

      <section class="card" style="margin-top:12px;">
        <div class="label">Live Logs</div>
        <ul id="logs-list" class="logs">{log_items}</ul>
      </section>
    </div>

    <div id="tab-search" class="tab-panel">
      <section class="card">
        <div class="label">Search Global Knowledge</div>
        <div class="search-bar">
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

    function shortPath(path, maxLen = 110) {{
      if (!path || path.length <= maxLen) return path || '-';
      return '...' + path.slice(-(maxLen - 3));
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
            <div class="label">LLM Answer${{data.model ? ` · ${{data.model}}` : ''}}${{data.detail_level ? ` · ${{data.detail_level}}` : ''}}</div>
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
              <div class="result-title" title="${{escapeHtml(item.source_path || '')}}">${{escapeHtml(shortPath(item.source_path || item.file_name || '-', 160))}}</div>
            </div>
            <div class="result-score">score ${{Number(item.score || 0).toFixed(3)}}</div>
          </div>
          <div class="result-meta">
            <span>type: ${{escapeHtml(item.source_type || '-')}}</span>
            <span>collection: ${{escapeHtml(item.collection_name || '-')}}</span>
            <span>file: ${{escapeHtml(item.file_name || '-')}}</span>
            <span>mailbox: ${{escapeHtml(item.mailbox || '-')}}</span>
            <span>spec: ${{escapeHtml(item.spec_id || '-')}}</span>
            <span>version: ${{escapeHtml(item.spec_version || '-')}}</span>
            <span>release: ${{escapeHtml(item.spec_release || '-')}}</span>
            <span>clause: ${{escapeHtml(item.clause_id || '-')}}</span>
            <span>chunk: ${{item.chunk_index ?? 0}}</span>
          </div>
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
      const collection = document.getElementById('search-collection').value;
      const sourceType = document.getElementById('search-source-type').value;
      if (!query) {{
        renderSearchResults({{ query: '', answer: '', results: [] }});
        return;
      }}
      const meta = document.getElementById('search-meta');
      meta.textContent = 'Generating answer...';
      const response = await fetch(`/api/ask?q=${{encodeURIComponent(query)}}&limit=${{encodeURIComponent(limit)}}&model=${{encodeURIComponent(model)}}&detail=${{encodeURIComponent(detail)}}&collection=${{encodeURIComponent(collection)}}&source_type=${{encodeURIComponent(sourceType)}}`, {{ cache: 'no-store' }});
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

    function resetSourceForm() {{
      document.getElementById('source-id').value = '';
      document.getElementById('source-path').value = '';
      document.getElementById('source-type').value = 'documents';
      document.getElementById('source-collection').value = '{VECTOR_COLLECTION}';
      document.getElementById('source-parsers').value = 'generic_text,pdf,office';
      document.getElementById('source-enabled').checked = true;
      document.getElementById('source-recursive').checked = true;
      document.getElementById('source-interval').value = 300;
    }}

    function defaultParsersForType(sourceType) {{
      const defaults = {{
        documents: 'generic_text,pdf,office',
        code: 'generic_text,code',
        thunderbird: 'thunderbird_mbox,email_attachments',
        '3gpp': 'generic_text,pdf,office,3gpp_spec'
      }};
      return defaults[sourceType] || 'generic_text';
    }}

    function applyDefaultParsersForType() {{
      const id = document.getElementById('source-id').value;
      if (id) return;
      const sourceType = document.getElementById('source-type').value;
      document.getElementById('source-parsers').value = defaultParsersForType(sourceType);
    }}

    function editSource(id) {{
      const item = (window.__watchedDirs || []).find((entry) => entry.id === id);
      if (!item) return;
      document.getElementById('source-id').value = String(item.id);
      document.getElementById('source-path').value = item.path || '';
      document.getElementById('source-type').value = item.source_type || 'documents';
      document.getElementById('source-collection').value = item.collection_name || '{VECTOR_COLLECTION}';
      document.getElementById('source-parsers').value = item.parser_profiles || '';
      document.getElementById('source-enabled').checked = !!item.enabled;
      document.getElementById('source-recursive').checked = !!item.recursive;
      document.getElementById('source-interval').value = item.scan_interval_seconds ?? 300;
    }}

    async function saveSource() {{
      const id = document.getElementById('source-id').value;
      const payload = {{
        path: document.getElementById('source-path').value.trim(),
        source_type: document.getElementById('source-type').value,
        collection_name: document.getElementById('source-collection').value.trim() || '{VECTOR_COLLECTION}',
        parser_profiles: document.getElementById('source-parsers').value.trim(),
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

      document.getElementById('service-state').textContent = data.service.paused ? 'paused' : 'running';
      document.getElementById('service-speed').textContent = `speed ${{data.service.speed_percent}}%`;
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
      updateSearchFilters(data);
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
            collection_name = query.get("collection", [""])[0]
            source_type = query.get("source_type", [""])[0]
            limit = max(1, min(20, limit))
            self._send_json(
                SERVICE.ask(
                    search_query,
                    limit=limit,
                    model=model,
                    detail_level=detail,
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
        if self.path == "/api/control/pause":
            payload = self._read_json()
            SERVICE.set_paused(bool(payload.get("paused", True)))
            self._send_json({"ok": True, "paused": SERVICE.is_paused()})
            return
        if self.path == "/api/control/scan":
            SERVICE.schedule_scan_tasks(manual=True)
            self._send_json({"ok": True, "message": "scan queued"})
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


def main() -> None:
    STORAGE.add_event("info", "service", f"binding http server on {HOST}:{PORT}")
    threading.Thread(
        target=SERVICE.worker_loop,
        args=("scan-worker", {"scan_directory"}, True),
        daemon=True,
    ).start()
    for worker_index in range(INGEST_WORKER_COUNT):
        threading.Thread(
            target=SERVICE.worker_loop,
            args=(f"ingest-worker-{worker_index + 1}", {"index_file", "delete_file", "reindex_mbox"}, False),
            daemon=True,
        ).start()
    STORAGE.add_event("info", "service", f"started {INGEST_WORKER_COUNT} ingest workers")
    with ReusableThreadingHTTPServer((HOST, PORT), RequestHandler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--read-file-payload":
        print(json.dumps(_build_file_payload(sys.argv[2], sys.argv[3] if len(sys.argv) >= 4 else "")), flush=True)
        raise SystemExit(0)
    main()
