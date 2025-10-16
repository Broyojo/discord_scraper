# Discord Channel Archiver — DCE‑Native, Resumable Export (Clean Spec)

**Owner:** David “Broyojo” Andrews
**Status:** Working v1.0 (Python implementation)
**Purpose:** Implement a resumable, additive archival pipeline for a Discord text channel (and its threads) by orchestrating **DiscordChatExporter (DCE)** in Docker with windowing, checkpoints, manifest‑level deduplication, and optional compaction — **preserving DCE JSON output exactly**.

---

## 1) Scope

### 1.1 Functional Requirements

* **Backfill** full history of a given channel ID (including active/archived threads) into **unaltered DCE JSON files**.
* **Resumability:** crash mid‑run → rerun the affected window(s) without data loss; outputs are safe to overwrite.
* **Incremental sync:** add only new messages since the **high‑water mark (HWM)**.
* **Deterministic windowing** (time or ID) with **small overlaps** to avoid fencepost misses.
* **Deduplication** by message ID via **manifest selection** (no in‑file edits).
* **Verification** per window and manifest-level sanity checks.
* **CLI interface** accepting bot token + channel ID plus runtime flags (no external config yet).
* **Optional compaction** command to merge manifest-selected partitions into a single DCE-formatted JSON export.

### 1.2 Non‑Goals

* Reconstructing edit history before the archiver is running (Gateway listener would be needed).
* Bulk media download beyond metadata (optional follow‑up job).

### 1.3 Assumptions

* Access via **bot token** (View Channel + Read Message History). No self‑bots.
* DCE CLI is available via a controlled **Docker image**.

---

## 2) System Overview

The tool is an **orchestrator** around DCE. It plans **windows**, runs DCE with `--after/--before/--partition`, writes DCE JSON files into deterministic paths, **catalogs** each file (counts/min/max id/ts) and records them in a **manifest** for deduped consumption. The **state store** tracks window lifecycle and the **HWM** for incremental sync.

**Components:**

1. **Planner** — computes window boundaries (time/ID) with overlap.
2. **Runner** — invokes DCE in Docker for a specific window; writes to a temp folder then promotes to final location on success.
3. **Cataloger** — parses DCE JSON files to compute stats; updates manifest/state. **Never modifies file contents.**
4. **Manifest Deduper** — assigns message‑ID ownership to exactly one file per overlap policy; downstream tools read **only selected files** (current policy: left‑biased by window start).
5. **State Store** — JSON file tracking windows, retries, and per‑channel HWM.
6. **Validator** — per‑window and global checks; identifies gaps.
7. **Incremental Sync** — single open‑ended window from HWM→now; updates HWM.
8. **Thread Strategy** — relies on DCE `--include-threads` flag (thread enumeration is a future enhancement).
9. **Progress Reporter** — renders `tqdm` progress bars for backfill windows and incremental sync stages while streaming Docker output.

---

## 3) Data Layout & Formats

### 3.1 Directory Structure

```
archive/
  channel=<channel_id>/
    backfill/
      dt=YYYY/MM/DD/                 # default: calendar-day windows
        dce-part-00000.json          # exact DCE output (may be multiple per window)
        dce-part-00001.json
    incremental/
      run=<YYYYMMDD-HHMMSS>/
        dce-part-00000.json
    _state/
      state.json                     # window registry + HWM
      manifest.json                  # catalog of files + ownership for dedupe
      logs/                          # optional structured logs per run
    tmp/                             # scratch during active export; cleaned on success
```

**Thread‑advanced mode** may add: `threads/channel=<thread_id>/...` under the parent channel directory.

### 3.2 DCE JSON Files (Preserved)

* Exactly the files that DCE emits for `--format Json` (with `--partition` if used).
* No rewriting, reformatting, or re‑serialization. Compression may be applied at **filesystem**/storage layer only.

### 3.3 Manifest Schema (`_state/manifest.json`)

```json
{
  "channel_id": "123456789012345678",
  "version": 1,
  "overlap_ms": 300000,
  "files": [
    {
      "path": "archive/channel=123/backfill/dt=2024/03/14/dce-part-00000.json",
      "window_id": "2024-03-14",
      "start_ms": 1710374400000,
      "end_ms": 1710460800000,
      "count": 51234,
      "min_id": "1050000000000000000",
      "max_id": "1050000099999999999",
      "observed_min_ts": "2024-03-14T00:00:02.111Z",
      "observed_max_ts": "2024-03-14T23:59:59.987Z",
      "owns": true                  
    }
  ],
  "high_water_id": "1061234567890123456",
  "ranges": {"first_ts": "2017-08-01T00:00:00Z", "last_ts": "2025-10-15T23:59:59Z"}
}
```

* `owns`: true/false marks whether this file is included in the deduped read set (set by deduper policy).

### 3.4 State File (`_state/state.json`)

```json
{
  "channel_id": "123456789012345678",
  "strategy": "calendar-day",
  "overlap_ms": 300000,
  "windows": [
    {
      "window_id": "2024-03-14",
      "start_ms": 1710374400000,
      "end_ms": 1710460800000,
      "status": "done",
      "try_count": 1,
      "files": ["archive/channel=123/.../dce-part-00000.json"],
      "stats": {"count": 51234, "min_id": "...", "max_id": "..."}
    },
    {"window_id": "2024-03-15", "status": "pending"}
  ],
  "high_water_id": "1061234567890123456"
}
```

### 3.5 Snowflake ↔ Timestamp

* `DISCORD_EPOCH_MS = 1420070400000`
* `ts_ms = (snowflake >> 22) + DISCORD_EPOCH_MS`
* `snowflake_floor(ts_ms) = ((ts_ms - DISCORD_EPOCH_MS) << 22)`

---

## 4) Algorithms & Flows

### 4.1 Window Planning

* Default: **calendar‑day** windows; configurable: calendar‑hour or calendar‑week windows.
* Adjacent windows get **±overlap_ms** (e.g., 5m). Overlap is handled at manifest level (file selection), not by rewriting files.
* Boundaries are computed as snowflake floors for `--after/--before`.

### 4.2 Export (Runner)

1. Compute bounds for window `W = [start_ms, end_ms)`:

   * `start_id = snowflake_floor(start_ms - overlap_ms)`
   * `end_id   = snowflake_floor(end_ms + overlap_ms)`
2. Run DCE in Docker:

```
docker run --rm \
  -v "$ARCHIVE_ROOT:/out" \
  -e DCE_TOKEN="$TOKEN" "$DCE_IMAGE" export \
  --token "$DCE_TOKEN" \
  --channel $CHANNEL_ID \
  --format Json \
  --after $start_id --before $end_id \
  --include-threads All \
  --partition 50000 \
  --output /out/tmp/$WINDOW_ID/dce.json
```

3. On success: move `/out/tmp/$WINDOW_ID/*` → `backfill/dt=YYYY/MM/DD/` and record paths in state.
4. On failure: leave window `pending` (or `failed` if non‑recoverable); manual retry resumes the same window.

### 4.3 Catalog & Manifest Deduplication (No Mutation)

* **Cataloger** parses each produced DCE JSON file, computes `count`, `min_id`, `max_id`, `observed_min_ts`, `observed_max_ts`; updates `manifest.json` entry for the file.
* **Deduper policy:** overlapping windows are resolved with a deterministic **left‑biased** rule (the earliest window keeps ownership). Only files with `owns=true` participate in downstream reads.
* Ownership is calculated at **file granularity** using `min_id/max_id`. If partial overlaps are frequent, shrink partitions or enhance the manifest logic (future work).
* Downstream tools read **only files with `owns=true`** for a deduped view.

### 4.4 Verification

* Per‑window checks (performed during ingestion):

  * Record total `count`, `min_id`, `max_id` per exported file.
  * Persist per-window status (`pending`, `running`, `done`) and retry count.
* Manifest verification command re-reads files to validate:

  * File existence and message counts.
  * `min_id`/`max_id` parity with manifest metadata.
  * Overlap detection among `owns=true` files.
* Future enhancements: coverage histograms and distinct-ID rollups.

### 4.5 Incremental Sync

1. Read `high_water_id` (HWM).
2. Run a single DCE export with `--after (HWM - overlap)` and no `--before` into `incremental/run=<stamp>/`, streaming container output through a `tqdm` progress bar.
3. Catalog resulting files; update manifest (incremental files default to `owns=true`).
4. Set `HWM = max(HWM, max_id_over_incremental)` and persist it in both state and manifest metadata.

### 4.6 Consolidation (Optional)

* Maintain only the manifest as the deduped list of files. The `combine` command can merge manifest-selected partitions into a single DCE-formatted JSON file without altering the originals.
* Future: generate larger rollups on a schedule (e.g., monthly) while keeping raw partitions intact.

---

## 5) CLI Specification

### 5.1 Commands

```
archiver backfill \
  --token $DISCORD_TOKEN \
  --channel-id 123... \
  [--start 2017-08-01] [--end 2025-10-16] \
  [--window calendar-day|calendar-hour|calendar-week] \
  [--overlap 5m] \
  [--out ./archive] \
  [--dce-image ghcr.io/broyojo/dce:latest] \
  [--dce-partition 50000] \
  [--include-threads all|active|none] \
  [--state ./archive/channel=<id>/_state/state.json] \
  [--resume] [--dry-run] [--verify-only]

archiver sync \
  --token $DISCORD_TOKEN \
  --channel-id 123... \
  [--overlap 5m] [--out ./archive] [--dce-image ...] [--include-threads ...]

archiver verify --out ./archive --channel-id 123... [--manifest-file ...]
archiver list-windows --state ./archive/channel=<id>/_state/state.json
archiver combine --out ./archive --channel-id 123... \
  [--manifest-file ...] [--output-file ...] [--include-unowned] [--overwrite] [--dedupe]
```

### 5.2 Config File (TOML)

*Not yet implemented.* CLI flags and environment variables are the current configuration surfaces.

### 5.3 Env Vars

* `DISCORD_TOKEN` (overridden by `--token`).

---

## 6) DCE in Docker

### 6.1 Image

```dockerfile
FROM mcr.microsoft.com/dotnet/runtime-deps:8.0
ARG DCE_VERSION=2.44.0
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/* \
 && curl -L -o /tmp/dce.zip https://github.com/Tyrrrz/DiscordChatExporter/releases/download/${DCE_VERSION}/DiscordChatExporter.Cli.zip \
 && unzip /tmp/dce.zip -d /opt/dce && rm /tmp/dce.zip
WORKDIR /opt/dce
ENTRYPOINT ["/opt/dce/DiscordChatExporter.Cli"]
```

### 6.2 Runner Invocation

```
docker run --rm \
  -v "$PWD/archive:/out" \
  -e DCE_TOKEN=$TOKEN $DCE_IMAGE export \
  --token "$DCE_TOKEN" \
  --channel $CHANNEL_ID \
  --format Json \
  --after $START_ID \
  --before $END_ID \
  --include-threads All \
  --partition 50000 \
  --output /out/tmp/$WINDOW_ID/dce.json
```

*Implementation detail:* stdout/stderr from the container is piped through the Python orchestrator so progress bars remain intact while surfacing DCE status messages.

---

## 7) Failure Handling & Resume

* Window statuses: `pending → running → done` (or `failed`).
* On crash: rerun any non‑`done` windows; outputs are **overwritten** safely after successful rerun.
* Errors bubble up with container logs preserved in stdout; operator reruns the command once issues are resolved.

---

## 8) Validation & Health

* **Per‑window:** counts, min/max id, observed min/max timestamps persisted in state/manifest.
* **Manifest verify:** replays catalog stats and flags overlaps or mismatches.
* **High-water tracking:** stored in both state and manifest; sync increases it monotonically.
* Future work: coverage charts, end-to-end distinct ID reconciliation, structured health reports.

---

## 9) Observability

* `tqdm` progress bars for backfill windows, sync export, and per-file cataloging.
* DCE stdout/stderr streamed through the progress writer so container messages remain visible.
* Future work: structured logs, metrics (windows_processed, retries, etc.).

---

## 10) Security

* Never persist tokens in state; allow `--token-file` backed by tmpfs.
* Use bot token with minimal perms; avoid self‑botting.

---

## 11) Performance Notes

* Start with 1‑day windows; tune by observed counts (target ~50k–150k msgs/window). Smaller windows improve recovery.
* 6M msgs ~ **5–7 GB** raw DCE JSON; can be reduced by filesystem compression.
* Current runner is single-threaded; concurrency is future work once rate limiting policy is better understood.

---

## 12) Testing Plan

1. Unit: snowflake math; planner edges.
2. Integration: export a known‑size channel; crash mid‑window; verify re‑run correctness and manifest dedupe.
3. Load: synthetic multi‑million window loop; measure throughput and retry behavior.
4. Chaos: SIGKILL during write; ensure temp→final promotion logic leaves no partials in final paths.

---

## 13) Implementation Notes

* Language: Python 3.12 (`tqdm` for progress, standard library JSON parsing).
* Promotion pattern: write under `tmp/` then **atomic rename/move** on success.
* Cataloger currently loads each DCE file into memory; consider streaming (`ijson`) for very large partitions.
* Manifest and state updates use atomic temp-file writes to avoid corruption.
* `combine` command concatenates manifest-selected files without mutating original DCE payloads; optional message-ID dedupe is available.

---

## 14) Runbook

* **Backfill:**

```
archiver backfill --token $(pass show discord/bot_token) \
  --channel-id 123... --start 2017-08-01 --end 2025-10-16 \
  --window calendar-day --overlap 5m \
  --out ./archive --dce-partition 50000 --resume
```

* **Incremental (cron):**

```
archiver sync --token $DISCORD_TOKEN --channel-id 123... --out ./archive --overlap 5m
```

* **Verify:**

```
archiver verify --out ./archive --channel-id 123...
```

* **Combine:**

```
archiver combine --out ./archive --channel-id 123... --output-file ./archive/channel=123/combined/dce.json
```

---

## 15) Future Extensions

* Thread‑first orchestration for higher parallelism.
* Optional compactor to produce monthly **DCE‑schema** merged files.
* Media fetcher with checksums and bounded concurrency.
* Gateway listener to capture edits/deletes into an auxiliary event log.
* Config file parsing and richer policy controls (ownership strategies, auto-splitting).

---

**End of clean spec.**
