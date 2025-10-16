# Discord Channel Archiver — DCE‑Native, Resumable Export (Clean Spec)

**Owner:** David “Broyojo” Andrews
**Status:** Draft v1.0 (clean copy)
**Purpose:** Implement a resumable, additive archival pipeline for a Discord text channel (and its threads) by orchestrating **DiscordChatExporter (DCE)** in Docker with windowing, checkpoints, and manifest‑level deduplication — **preserving DCE JSON output exactly**.

---

## 1) Scope

### 1.1 Functional Requirements

* **Backfill** full history of a given channel ID (including active/archived threads) into **unaltered DCE JSON files**.
* **Resumability:** crash mid‑run → rerun the affected window(s) without data loss; outputs are safe to overwrite.
* **Incremental sync:** add only new messages since the **high‑water mark (HWM)**.
* **Deterministic windowing** (time or ID) with **small overlaps** to avoid fencepost misses.
* **Deduplication** by message ID via **manifest selection** (no in‑file edits).
* **Verification** per window and global coverage checks.
* **CLI interface** accepting bot token + channel ID plus config/overrides.

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
4. **Manifest Deduper** — assigns message‑ID ownership to exactly one file per overlap policy; downstream tools read **only selected files**.
5. **State Store** — JSON file tracking windows, retries, and per‑channel HWM.
6. **Validator** — per‑window and global checks; identifies gaps.
7. **Incremental Sync** — single open‑ended window from HWM→now; updates HWM.
8. **Thread Strategy** — either `--include-threads All` (simple) or enumerate thread IDs and treat each as a subchannel (advanced mode).

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

* Default: **calendar‑day** windows; configurable: calendar‑hour, week, or message‑count windows.
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
4. On failure: leave window `pending` (or `failed` if non‑recoverable); retry with backoff. Optional **auto‑shrink** window if retries exceed threshold.

### 4.3 Catalog & Manifest Deduplication (No Mutation)

* **Cataloger** parses each produced DCE JSON file, computes `count`, `min_id`, `max_id`, `observed_min_ts`, `observed_max_ts`; updates `manifest.json` entry for the file.
* **Deduper policy:** for overlapping windows, set `owns=true` on exactly one file for any overlapping message IDs. Deterministic rule examples:

  * Prefer file whose window **start** is closest to the message timestamp, or
  * Prefer the **earlier** window (left‑biased), or
  * Prefer **later** window (right‑biased).
* Ownership is calculated at **file granularity** using `min_id/max_id`. If two files’ ranges overlap, and policy prefers left‑biased, mark the right file `owns=false` for that overlap range. (If partial overlaps are frequent, switch to smaller partitions or switch policy to per‑ID index in the manifest.)
* Downstream tools read **only files with `owns=true`** for a deduped view.

### 4.4 Verification

* Per‑window checks:

  * `stats.count > 0` unless known quiet window.
  * All message timestamps inside `[start_ms - overlap_ms, end_ms + overlap_ms)`.
  * Snowflake monotonicity sanity (min→max).
* Global checks:

  * Coverage by day; flag long zero runs in otherwise busy periods.
  * `distinct(id)` across **owned** files equals total messages.

### 4.5 Incremental Sync

1. Read `high_water_id` (HWM).
2. Run single DCE export with `--after (HWM - overlap)` and no `--before` into `incremental/run=<stamp>/`.
3. Catalog files; update manifest (set ownership true for incremental files unless they overlap with last backfill window per policy).
4. Set `HWM = max(HWM, max_id_over_incremental)`.

### 4.6 Consolidation (Optional)

* Maintain only the manifest as the deduped list of files. Optionally generate **compacted DCE‑schema** exports (e.g., monthly) for convenience; originals remain immutable.

---

## 5) CLI Specification

### 5.1 Commands

```
archiver backfill \
  --token $DISCORD_TOKEN \
  --channel-id 123... \
  [--start 2017-08-01] [--end 2025-10-16] \
  [--window calendar-day|calendar-hour|messages:N] \
  [--overlap 5m] \
  [--out ./archive] \
  [--dce-image ghcr.io/broyojo/dce:latest] \
  [--dce-partition 50000] \
  [--include-threads all|none] \
  [--concurrency 2] \
  [--state ./archive/channel=<id>/_state/state.json] \
  [--resume] [--dry-run] [--verify-only]

archiver sync \
  --token $DISCORD_TOKEN \
  --channel-id 123... \
  [--overlap 5m] [--out ./archive]

archiver verify --out ./archive --channel-id 123...
archiver list-windows --state ./archive/channel=<id>/_state/state.json
```

### 5.2 Config File (TOML)

```toml
[channel]
id = "123456789012345678"

[planner]
mode = "calendar-day"
overlap = "5m"
start = "2017-08-01"
end   = "2025-10-16"

[dce]
image = "ghcr.io/broyojo/dce:latest"
partition = 50000
include_threads = "All"

[io]
out_dir = "./archive"
state_file = "./archive/channel=123/_state/state.json"
```

### 5.3 Env Vars

* `DISCORD_TOKEN` (overridden by `--token`)
* `ARCHIVER_OUT_DIR`, `ARCHIVER_STATE_FILE`

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

---

## 7) Failure Handling & Resume

* Window statuses: `pending → running → done` (or `failed`).
* On crash: rerun any non‑`done` windows; outputs are **overwritten** safely.
* Retries with exponential backoff; optional **auto‑shrink** (split window) after N failures.
* Errors: auth/permission → `failed` and halt; 429/5xx → retried by DCE during run, and by orchestrator across runs.

---

## 8) Validation & Health

* **Per‑window:** counts, min/max id, observed min/max timestamps.
* **Global:** coverage chart by day; `distinct(id)` over **owned** files equals total; HWM monotonic increase after `sync`.
* Emit `_state/manifest.json` as the source of truth for downstream reads.

---

## 9) Observability

* Structured logs per window (start/end, exit code, produced files, stats, retries).
* Metrics (optional): windows_processed, files_owned, rows_estimated, retries, last_run_seconds.

---

## 10) Security

* Never persist tokens in state; allow `--token-file` backed by tmpfs.
* Use bot token with minimal perms; avoid self‑botting.

---

## 11) Performance Notes

* Start with 1‑day windows; tune by observed counts (target ~50k–150k msgs/window). Smaller windows improve recovery.
* 6M msgs ~ **5–7 GB** raw DCE JSON; can be reduced by filesystem compression.
* Concurrency: 1–2 windows in flight typically stable under Discord rate limits.

---

## 12) Testing Plan

1. Unit: snowflake math; planner edges.
2. Integration: export a known‑size channel; crash mid‑window; verify re‑run correctness and manifest dedupe.
3. Load: synthetic multi‑million window loop; measure throughput and retry behavior.
4. Chaos: SIGKILL during write; ensure temp→final promotion logic leaves no partials in final paths.

---

## 13) Implementation Notes

* Language: Go/Rust preferred; Python acceptable with streaming JSON parse.
* Promotion pattern: write under `tmp/` then **atomic rename/move** on success.
* Cataloger should stream‑parse `messages[]` to avoid large memory use.
* Manifest updates should be atomic (write to temp then rename) to avoid corruption.

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

---

## 15) Future Extensions

* Thread‑first orchestration for higher parallelism.
* Optional compactor to produce monthly **DCE‑schema** merged files.
* Media fetcher with checksums and bounded concurrency.
* Gateway listener to capture edits/deletes into an auxiliary event log.

---

**End of clean spec.**