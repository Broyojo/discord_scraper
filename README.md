# Discord Channel Archiver

A resilient orchestration layer around [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter) that backfills and incrementally syncs channel history into unmodified DCE JSON files. It keeps track of window progress, manifests, and a high-water mark so you can resume long-running exports without duplicating data. Extra utilities help you merge partitions and inspect archives.

## Features

- **Resumable backfill:** deterministic calendar windows (day/hour/week) with overlap, state tracking, and safe reruns.
- **Incremental sync:** open-ended window from the manifest high-water mark with automatic state and manifest updates.
- **Manifest + state store:** JSON metadata captures ownership, per-file stats, and per-window lifecycle.
- **Progress-aware logging:** `tqdm` progress bars wrap window exports and file cataloging while streaming Docker output.
- **Partition combiner:** optional command to stitch manifest-selected partitions into a single DCE-formatted JSON file (with optional per-ID dedupe).

## Requirements

- Python 3.12+
- Docker (used to run DiscordChatExporter)
- [uv](https://github.com/astral-sh/uv) or another environment manager to install dependencies (`tqdm`)
- Discord bot token with **Read Message History** and **View Channel** permissions for the target channel(s)

## Installation

```bash
# install dependencies (uses uv)
uv sync

# alternatively, create a virtualenv and use pip
pip install -e .
```

> **Note:** The project uses the latest DiscordChatExporter CLI image by default (`tyrrrz/discordchatexporter:stable`). Override with `--dce-image` if you prefer another tag.

## Usage

Run the CLI via `uv run` (or your preferred Python invoker). Each command accepts `--help` for full usage.

### Backfill (historical export)

```bash
uv run main.py backfill \
  --token "$DISCORD_TOKEN" \
  --channel-id 123456789012345678 \
  --start 2017-08-01 \
  --end 2025-01-01 \
  --window calendar-day \
  --overlap 5m \
  --out ./archive \
  --dce-image tyrrrz/discordchatexporter:stable \
  --resume
```

This will:

1. Plan daily windows with a 5-minute overlap.
2. Stream DiscordChatExporter output through Docker, writing the raw JSON into `archive/channel=<id>/backfill/dt=YYYY/MM/DD/`.
3. Update `archive/channel=<id>/_state/state.json` and `manifest.json` with ownership and coverage info.
4. Show a progress bar that survives DCE console output.

Use `--dry-run` to preview window boundaries or `--verify-only` to check manifest consistency without exporting.

### Incremental sync

```bash
uv run main.py sync \
  --token "$DISCORD_TOKEN" \
  --channel-id 123456789012345678 \
  --out ./archive \
  --overlap 5m
```

Sync loads the high-water mark from the state/manifest, exports a single window ending “now”, and appends files under `incremental/run=<timestamp>/`. Progress bars track both the export and per-file cataloging.

### Verification

```bash
uv run main.py verify --out ./archive --channel-id 123456789012345678
```

This replays the manifest, ensuring each owned file exists, message counts match, and owned ranges do not overlap.

### Combine partitions

```bash
uv run main.py combine \
  --out ./archive \
  --channel-id 123456789012345678 \
  --output-file ./archive/channel=123456789012345678/combined/dce-combined.json \
  --dedupe
```

`combine` concatenates manifest-selected files (default: owned-only) into a single DCE-formatted JSON. The originals remain untouched. Use `--include-unowned` to merge every partition regardless of ownership and `--dedupe` to remove duplicate message IDs while combining.

### Inspect window plan

```bash
uv run main.py list-windows --state ./archive/channel=123456789012345678/_state/state.json
```

Prints each planned window, its status, and scheduled timestamps—handy for debugging or monitoring progress.

## Directory Layout

```
archive/
  channel=<channel_id>/
    backfill/dt=YYYY/MM/DD/dce-part-00000.json
    incremental/run=<YYYYMMDD-HHMMSS>/dce-part-00000.json
    combined/ (optional combined outputs)
    _state/
      state.json
      manifest.json
    tmp/
```

- `state.json` tracks per-window lifecycle, retries, and the high-water mark.
- `manifest.json` tracks per-file stats (`count`, `min_id`, `max_id`, date ranges) and an `owns` flag that drives deduplication.
- `tmp/` buffers Docker output until a window completes; files are then atomically promoted.

## Example Scripts

The `scripts/` directory contains runnable examples that wire the CLI together:

- `scripts/scrape.sh` — wraps a backfill run for the `#general` channel (`699326565351162036`) using a preconfigured image and date range. Set `BOBBYBOT_TOKEN` in your environment before running.
- `scripts/analyze.sh` — combines the manifest-owned files and pipes the merged JSON into `chat-analytics` (via `bunx`) to produce an HTML report (`report.html`), then opens it locally.

Use these as starting points for cron jobs or automation pipelines.

## How It Works

1. **Plan windows** based on the requested cadence (day/hour/week) with a configured overlap to avoid fencepost misses.
2. **Export via Docker**, streaming `DiscordChatExporter` output while keeping a progress bar visible.
3. **Promote outputs** from a temporary directory to `backfill/` (or `incremental/`) once the window succeeds.
4. **Catalog results**, computing per-file message counts, ID ranges, and timestamps.
5. **Update metadata** (`state.json` + `manifest.json`) atomically so reruns pick up where they left off.
6. **Deduplicate** by marking exactly one partition per overlapping range as `owns=true`. Downstream consumers read only owned files.

## Development Notes

- The catalog step currently loads each DCE JSON fully into memory. For extremely large partitions, swap in a streaming parser such as `ijson`.
- Progress bars use `tqdm`. When running under CI or piping output, consider disabling the bars with `FORCE_TERMINAL=1` or `tqdm`'s environment options if necessary.
- Docker logs include DCE banners/messages; they’re routed through `tqdm.write()` so they don’t disrupt progress.
