# rag-service

Local ingest service bound to port `6334`.

## Run

```bash
cd /home/egarcia/rag-service
python3 server.py
```

## Systemd

```bash
cd /home/egarcia/rag-service
bash deploy/install-systemd-service.sh
systemctl --user status rag-service --no-pager
```

Then open:

- `http://127.0.0.1:6334/`

## Endpoints

- `/` HTML dashboard
- `/api/status` JSON status
- `/api/control/speed` POST `{"speed_percent": 50}`
- `/api/control/pause` POST `{"paused": true}`
- `/health`

## Current behavior

- Qdrant status is probed on `http://127.0.0.1:6333/collections`
- state is persisted in `rag-service.db`
- when deployed through `systemd --user`, runtime state is stored under `~/.rag-service/data/rag-service.db`
- default watched directories are bootstrapped automatically
- a background worker schedules `scan_directory`, `index_file`, and `delete_file` tasks
- indexing is currently metadata-only placeholder logic
- Qdrant upload and document parsing are the next step
