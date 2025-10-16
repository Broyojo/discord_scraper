from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from tqdm import tqdm

DISCORD_EPOCH_MS = 1420070400000


@dataclass(slots=True)
class Window:
    window_id: str
    start_ms: int
    end_ms: int


@dataclass(slots=True)
class WindowState:
    window_id: str
    start_ms: int
    end_ms: int
    status: str = "pending"
    try_count: int = 0
    files: List[str] = field(default_factory=list)
    stats: Dict[str, Any] | None = None


@dataclass(slots=True)
class ManifestEntry:
    path: str
    window_id: str
    start_ms: int
    end_ms: int
    count: int
    min_id: str | None
    max_id: str | None
    observed_min_ts: str | None
    observed_max_ts: str | None
    owns: bool = True


def ensure_tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_datetime(value: str) -> datetime:
    try:
        if value.isdigit():
            # treat as epoch seconds
            return datetime.fromtimestamp(int(value), tz=UTC)
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return ensure_tz(dt)
    except ValueError as exc:
        msg = f"Unable to parse datetime value '{value}': {exc}"
        raise argparse.ArgumentTypeError(msg) from exc


def parse_date_or_datetime(value: str) -> datetime:
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return datetime.fromisoformat(value).replace(tzinfo=UTC)
    return parse_datetime(value)


def parse_duration_to_ms(value: str) -> int:
    units = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}
    value = value.strip().lower()
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            num = float(value[: -len(suffix)])
            return int(num * multiplier)
    if value.isdigit():
        return int(value)
    raise argparse.ArgumentTypeError(
        f"Invalid duration '{value}'. Expected formats like '5m', '30s', '1000ms'."
    )


def snowflake_floor(ts_ms: int) -> int:
    if ts_ms < DISCORD_EPOCH_MS:
        ts_ms = DISCORD_EPOCH_MS
    return (int(ts_ms) - DISCORD_EPOCH_MS) << 22


def snowflake_to_ts_ms(snowflake: int | str) -> int:
    value = int(snowflake)
    return (value >> 22) + DISCORD_EPOCH_MS


def ts_ms_to_iso(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    return dt.isoformat().replace("+00:00", "Z")


def parse_dce_timestamp(value: str) -> datetime:
    """Parse ISO strings emitted by DCE (accepts trailing Z or +00:00)."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def format_json_block(value: Any, indent_level: int) -> str:
    """Render JSON with two-space indentation aligned to the given base indent."""
    raw = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)
    indent_prefix = "  " * indent_level
    return raw.replace("\n", "\n" + indent_prefix)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False, dir=str(path.parent)
    ) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


class WindowPlanner:
    def __init__(self, mode: str, overlap_ms: int):
        self.mode = mode
        self.overlap_ms = overlap_ms

    def plan(self, start: datetime, end: datetime) -> List[Window]:
        start = ensure_tz(start)
        end = ensure_tz(end)
        if start >= end:
            raise ValueError("start must be earlier than end")
        if self.mode == "calendar-day":
            return self._plan_calendar(start, end, timedelta(days=1))
        if self.mode == "calendar-hour":
            return self._plan_calendar(start, end, timedelta(hours=1))
        if self.mode == "calendar-week":
            return self._plan_calendar(start, end, timedelta(days=7))
        raise ValueError(f"Unsupported window mode '{self.mode}'")

    def _plan_calendar(
        self, start: datetime, end: datetime, delta: timedelta
    ) -> List[Window]:
        windows: List[Window] = []
        boundary = self._align_start(start, delta)
        while boundary < end:
            next_boundary = boundary + delta
            actual_start = max(boundary, start)
            actual_end = min(next_boundary, end)
            if actual_start >= actual_end:
                boundary = next_boundary
                continue
            if self.mode == "calendar-day":
                window_id = boundary.strftime("%Y-%m-%d")
            elif self.mode == "calendar-hour":
                window_id = boundary.strftime("%Y-%m-%dT%H")
            else:
                window_id = boundary.strftime("%Y-%m-%d")
            windows.append(
                Window(
                    window_id=window_id,
                    start_ms=int(actual_start.timestamp() * 1000),
                    end_ms=int(actual_end.timestamp() * 1000),
                )
            )
            boundary = next_boundary
        return windows

    @staticmethod
    def _align_start(start: datetime, delta: timedelta) -> datetime:
        if delta >= timedelta(days=7):
            aligned = start - timedelta(days=start.weekday())
            return aligned.replace(hour=0, minute=0, second=0, microsecond=0)
        if delta >= timedelta(days=1):
            return start.replace(hour=0, minute=0, second=0, microsecond=0)
        if delta == timedelta(hours=1):
            return start.replace(minute=0, second=0, microsecond=0)
        return start


class StateStore:
    def __init__(self, path: Path, channel_id: str, strategy: str, overlap_ms: int):
        self.path = path
        self.channel_id = channel_id
        self.strategy = strategy
        self.overlap_ms = overlap_ms
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {
                "channel_id": self.channel_id,
                "strategy": self.strategy,
                "overlap_ms": self.overlap_ms,
                "windows": [],
                "high_water_id": None,
            }
        with self.path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload

    def save(self) -> None:
        atomic_write_json(self.path, self.data)

    def window_states(self) -> Dict[str, WindowState]:
        mapping: Dict[str, WindowState] = {}
        for raw in self.data.get("windows", []):
            mapping[raw["window_id"]] = WindowState(
                window_id=raw["window_id"],
                start_ms=raw["start_ms"],
                end_ms=raw["end_ms"],
                status=raw.get("status", "pending"),
                try_count=raw.get("try_count", 0),
                files=list(raw.get("files", [])),
                stats=raw.get("stats"),
            )
        return mapping

    def windows_ordered(self) -> List[WindowState]:
        order: List[WindowState] = []
        for raw in self.data.get("windows", []):
            order.append(
                WindowState(
                    window_id=raw["window_id"],
                    start_ms=raw["start_ms"],
                    end_ms=raw["end_ms"],
                    status=raw.get("status", "pending"),
                    try_count=raw.get("try_count", 0),
                    files=list(raw.get("files", [])),
                    stats=raw.get("stats"),
                )
            )
        return order

    def set_windows(self, windows: Iterable[Window]) -> None:
        existing = self.window_states()
        ordered: List[Dict[str, Any]] = []
        self.data["strategy"] = self.strategy
        self.data["overlap_ms"] = self.overlap_ms
        for window in windows:
            state = existing.get(window.window_id)
            if state is None:
                state = WindowState(
                    window_id=window.window_id,
                    start_ms=window.start_ms,
                    end_ms=window.end_ms,
                )
            ordered.append(dataclasses.asdict(state))
        self.data["windows"] = ordered
        self.save()

    def update_window(
        self,
        window_id: str,
        *,
        status: Optional[str] = None,
        try_count: Optional[int] = None,
        files: Optional[List[str]] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> None:
        for entry in self.data.get("windows", []):
            if entry["window_id"] == window_id:
                if status is not None:
                    entry["status"] = status
                if try_count is not None:
                    entry["try_count"] = try_count
                if files is not None:
                    entry["files"] = files
                if stats is not None:
                    entry["stats"] = stats
                self.save()
                return
        raise KeyError(f"Unknown window '{window_id}' in state store")

    def set_high_water(self, high_water_id: Optional[str]) -> None:
        self.data["high_water_id"] = high_water_id
        self.save()

    def get_high_water(self) -> Optional[str]:
        return self.data.get("high_water_id")


class ManifestStore:
    def __init__(self, path: Path, channel_id: str, overlap_ms: int):
        self.path = path
        self.channel_id = channel_id
        self.overlap_ms = overlap_ms
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {
                "channel_id": self.channel_id,
                "version": 1,
                "overlap_ms": self.overlap_ms,
                "files": [],
                "high_water_id": None,
                "ranges": {},
            }
        with self.path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def save(self) -> None:
        atomic_write_json(self.path, self.data)

    def add_or_update_entry(self, entry: ManifestEntry) -> None:
        files = self.data.setdefault("files", [])
        for idx, existing in enumerate(files):
            if existing["path"] == entry.path:
                files[idx] = dataclasses.asdict(entry)
                self._dedupe()
                self.save()
                return
        files.append(dataclasses.asdict(entry))
        self._dedupe()
        self.save()

    def _dedupe(self) -> None:
        files = self.data.get("files", [])
        files.sort(key=lambda e: (e["start_ms"], e["path"]))
        previous_max: Optional[int] = None
        for file_entry in files:
            if previous_max is None:
                file_entry["owns"] = True
            else:
                min_id = file_entry.get("min_id")
                if min_id is None:
                    file_entry["owns"] = True
                else:
                    min_id_int = int(min_id)
                    if min_id_int <= previous_max:
                        file_entry["owns"] = False
                    else:
                        file_entry["owns"] = True
            max_id = file_entry.get("max_id")
            if max_id:
                previous_max = max(previous_max or 0, int(max_id))
        observed_min = [
            entry.get("observed_min_ts")
            for entry in files
            if entry.get("observed_min_ts")
        ]
        observed_max = [
            entry.get("observed_max_ts")
            for entry in files
            if entry.get("observed_max_ts")
        ]
        if observed_min or observed_max:
            self.data["ranges"] = {
                "first_ts": min(observed_min) if observed_min else None,
                "last_ts": max(observed_max) if observed_max else None,
            }

    def set_high_water(self, high_water_id: Optional[str]) -> None:
        self.data["high_water_id"] = high_water_id
        self.save()

    def entries(self) -> List[ManifestEntry]:
        result: List[ManifestEntry] = []
        for raw in self.data.get("files", []):
            result.append(
                ManifestEntry(
                    path=raw["path"],
                    window_id=raw["window_id"],
                    start_ms=raw["start_ms"],
                    end_ms=raw["end_ms"],
                    count=raw["count"],
                    min_id=raw.get("min_id"),
                    max_id=raw.get("max_id"),
                    observed_min_ts=raw.get("observed_min_ts"),
                    observed_max_ts=raw.get("observed_max_ts"),
                    owns=raw.get("owns", True),
                )
            )
        return result


def catalog_dce_json(path: Path) -> Tuple[int, Optional[str], Optional[str]]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    messages = payload.get("messages", [])
    count = 0
    min_id: Optional[int] = None
    max_id: Optional[int] = None
    for message in messages:
        raw_id = message.get("Id") or message.get("id")
        if raw_id is None:
            continue
        snowflake = int(raw_id)
        count += 1
        if min_id is None or snowflake < min_id:
            min_id = snowflake
        if max_id is None or snowflake > max_id:
            max_id = snowflake
    min_str = str(min_id) if min_id is not None else None
    max_str = str(max_id) if max_id is not None else None
    return count, min_str, max_str


def list_tmp_files(tmp_dir: Path) -> List[Path]:
    if not tmp_dir.exists():
        return []
    return sorted([p for p in tmp_dir.iterdir() if p.is_file()])


def run_dce_export(
    *,
    archive_root: Path,
    tmp_dir: Path,
    window_id: str,
    token: str,
    channel_id: str,
    dce_image: str,
    include_threads: str,
    partition: int,
    start_id: int,
    end_id: Optional[int],
    logger: Optional[Callable[[str], None]] = None,
) -> None:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_dir / "dce.json"

    cmd: List[str] = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{archive_root.resolve()}:/out",
        "-e",
        "DCE_TOKEN",
        dce_image,
        "export",
        "--token",
        token,
        "--channel",
        channel_id,
        "--format",
        "Json",
        "--after",
        str(start_id),
    ]
    if end_id is not None:
        cmd.extend(["--before", str(end_id)])
    cmd.extend(
        [
            "--include-threads",
            include_threads,
            "--partition",
            str(partition),
            "--output",
            f"/out/{tmp_dir.relative_to(archive_root)}/dce.json",
        ]
    )
    env = os.environ.copy()
    env["DCE_TOKEN"] = token
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    if logger is not None:
        log_fn = logger
    else:
        log_fn = lambda msg: print(msg, flush=True)
    for line in process.stdout:
        cleaned = line.rstrip("\n")
        log_fn(cleaned.replace("\r", ""))
    retcode = process.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, cmd)
    if not output_path.exists():
        raise RuntimeError(f"DCE did not produce expected file at {output_path}")


def promote_tmp_files(tmp_dir: Path, final_dir: Path) -> List[Path]:
    files = list_tmp_files(tmp_dir)
    final_dir.mkdir(parents=True, exist_ok=True)
    promoted: List[Path] = []
    for idx, file_path in enumerate(files):
        suffix = file_path.suffix or ".json"
        dest = final_dir / f"dce-part-{idx:05d}{suffix}"
        if dest.exists():
            dest.unlink()
        shutil.move(str(file_path), dest)
        promoted.append(dest)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return promoted


def backfill_command(args: argparse.Namespace) -> None:
    token = args.token or ""
    if not token and args.token_file:
        token = Path(args.token_file).read_text(encoding="utf-8").strip()
    if not token:
        token = os.environ.get("DISCORD_TOKEN", "")
    if not token and not (args.dry_run or args.verify_only):
        raise SystemExit(
            "A Discord bot token is required (use --token or --token-file)."
        )

    channel_id = args.channel_id
    out_dir = Path(args.out_dir).resolve()
    channel_root = out_dir / f"channel={channel_id}"
    state_file = (
        Path(args.state_file).resolve()
        if args.state_file
        else channel_root / "_state" / "state.json"
    )
    manifest_file = state_file.parent / "manifest.json"
    tmp_root = channel_root / "tmp"
    backfill_root = channel_root / "backfill"

    start = args.start or datetime.fromtimestamp(DISCORD_EPOCH_MS / 1000, tz=UTC)
    end = args.end or datetime.now(tz=UTC)
    manifest = ManifestStore(manifest_file, channel_id, args.overlap_ms)
    if args.verify_only:
        verify_manifest(manifest, out_dir)
        return

    planner = WindowPlanner(args.window_mode, args.overlap_ms)
    planned_windows = planner.plan(start, end)

    if args.dry_run:
        for window in planned_windows:
            print(
                f"[dry-run] window {window.window_id} "
                f"{ts_ms_to_iso(window.start_ms)} → {ts_ms_to_iso(window.end_ms)}"
            )
        return

    state = StateStore(state_file, channel_id, args.window_mode, args.overlap_ms)
    state.set_windows(planned_windows)

    ordered_states = state.windows_ordered()
    total_windows = len(ordered_states)
    completed_windows = sum(
        1 for state_entry in ordered_states if state_entry.status == "done"
    )
    with tqdm(
        total=total_windows,
        desc="Backfill windows",
        unit="window",
        initial=completed_windows,
    ) as progress:
        progress.refresh()
        for state_entry in ordered_states:
            window = Window(
                window_id=state_entry.window_id,
                start_ms=state_entry.start_ms,
                end_ms=state_entry.end_ms,
            )
            if state_entry.status == "done" and args.resume:
                continue

            start_id = snowflake_floor(window.start_ms - args.overlap_ms)
            end_id = snowflake_floor(window.end_ms + args.overlap_ms)

            tmp_dir = tmp_root / window.window_id
            final_dir = (
                backfill_root
                / f"dt={ts_ms_to_iso(window.start_ms)[:10].replace('-', '/')}"
            )

            progress.set_postfix_str(f"export {window.window_id}")
            state.update_window(
                window.window_id,
                status="running",
                try_count=state_entry.try_count + 1,
            )
            try:
                run_dce_export(
                    archive_root=channel_root,
                    tmp_dir=tmp_dir,
                    window_id=window.window_id,
                    token=token,
                    channel_id=channel_id,
                    dce_image=args.dce_image,
                    include_threads=args.include_threads,
                    partition=args.partition,
                    start_id=start_id,
                    end_id=end_id,
                    logger=progress.write,
                )
                promoted_files = promote_tmp_files(tmp_dir, final_dir)
            except Exception as exc:  # noqa: BLE001 - propagate but reset state first
                progress.set_postfix_str(f"failed {window.window_id}")
                state.update_window(window.window_id, status="pending")
                shutil.rmtree(tmp_dir, ignore_errors=True)
                raise SystemExit(f"Window {window.window_id} failed: {exc}") from exc

            manifest_entries: List[str] = []
            stats: Dict[str, Any] = {}
            total_count = 0
            window_min_id: Optional[int] = None
            window_max_id: Optional[int] = None

            for file_path in promoted_files:
                count, min_id, max_id = catalog_dce_json(file_path)
                total_count += count
                if min_id is not None:
                    min_id_int = int(min_id)
                    if window_min_id is None or min_id_int < window_min_id:
                        window_min_id = min_id_int
                if max_id is not None:
                    max_id_int = int(max_id)
                    if window_max_id is None or max_id_int > window_max_id:
                        window_max_id = max_id_int
                relative_path = file_path.relative_to(out_dir).as_posix()
                entry = ManifestEntry(
                    path=relative_path,
                    window_id=window.window_id,
                    start_ms=window.start_ms,
                    end_ms=window.end_ms,
                    count=count,
                    min_id=min_id,
                    max_id=max_id,
                    observed_min_ts=ts_ms_to_iso(snowflake_to_ts_ms(min_id))
                    if min_id
                    else None,
                    observed_max_ts=ts_ms_to_iso(snowflake_to_ts_ms(max_id))
                    if max_id
                    else None,
                    owns=True,
                )
                manifest.add_or_update_entry(entry)
                manifest_entries.append(entry.path)

            if window_max_id is not None:
                manifest.set_high_water(str(window_max_id))
                state.set_high_water(str(window_max_id))

            stats.update(
                {
                    "count": total_count,
                    "min_id": str(window_min_id) if window_min_id is not None else None,
                    "max_id": str(window_max_id) if window_max_id is not None else None,
                }
            )
            state.update_window(
                window.window_id,
                status="done",
                files=manifest_entries,
                stats=stats,
            )
            progress.update(1)
            progress.set_postfix_str("")


def sync_command(args: argparse.Namespace) -> None:
    token = args.token or os.environ.get("DISCORD_TOKEN")
    if not token and args.token_file:
        token = Path(args.token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise SystemExit("A Discord bot token is required for sync.")

    channel_id = args.channel_id
    out_dir = Path(args.out_dir).resolve()
    channel_root = out_dir / f"channel={channel_id}"
    state_file = (
        Path(args.state_file).resolve()
        if args.state_file
        else channel_root / "_state" / "state.json"
    )
    manifest_file = state_file.parent / "manifest.json"
    incremental_root = channel_root / "incremental"
    tmp_root = channel_root / "tmp"

    state = StateStore(state_file, channel_id, "incremental", args.overlap_ms)
    manifest = ManifestStore(manifest_file, channel_id, args.overlap_ms)

    high_water = state.get_high_water()
    if high_water is None:
        high_water = "0"

    high_water_ts = snowflake_to_ts_ms(high_water)
    start_ms = high_water_ts - args.overlap_ms
    start_id = snowflake_floor(start_ms)

    window_id = datetime.now(tz=UTC).strftime("sync-%Y%m%d-%H%M%S")
    tmp_dir = tmp_root / window_id
    run_dir = incremental_root / f"run={window_id}"

    print(f"Running incremental sync from snowflake {start_id} (HWM {high_water})")

    try:
        run_dce_export(
            archive_root=channel_root,
            tmp_dir=tmp_dir,
            window_id=window_id,
            token=token,
            channel_id=channel_id,
            dce_image=args.dce_image,
            include_threads=args.include_threads,
            partition=args.partition,
            start_id=start_id,
            end_id=None,
        )
        promoted_files = promote_tmp_files(tmp_dir, run_dir)
    except Exception as exc:  # noqa: BLE001
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise SystemExit(f"Incremental sync failed: {exc}") from exc

    max_seen: Optional[int] = int(high_water) if high_water else None

    for file_path in promoted_files:
        count, min_id, max_id = catalog_dce_json(file_path)
        if max_id:
            snowflake_int = int(max_id)
            max_seen = max(max_seen or 0, snowflake_int)
        relative_path = file_path.relative_to(out_dir).as_posix()
        entry = ManifestEntry(
            path=relative_path,
            window_id=window_id,
            start_ms=start_ms,
            end_ms=int(time.time() * 1000),
            count=count,
            min_id=min_id,
            max_id=max_id,
            observed_min_ts=ts_ms_to_iso(snowflake_to_ts_ms(min_id))
            if min_id
            else None,
            observed_max_ts=ts_ms_to_iso(snowflake_to_ts_ms(max_id))
            if max_id
            else None,
            owns=True,
        )
        manifest.add_or_update_entry(entry)

    if max_seen is not None:
        manifest.set_high_water(str(max_seen))
        state.set_high_water(str(max_seen))


def verify_manifest(manifest: ManifestStore, out_dir: Path) -> None:
    entries = manifest.entries()
    problems: List[str] = []
    total_count = 0
    owned_count = 0
    last_max: Optional[int] = None

    for entry in entries:
        entry_path = Path(entry.path)
        if entry_path.is_absolute():
            file_path = entry_path
        elif entry_path.parts and entry_path.parts[0] == out_dir.name:
            file_path = out_dir.parent / entry_path
        else:
            file_path = out_dir / entry_path
        if not file_path.exists():
            problems.append(f"Missing file: {entry.path}")
            continue
        count, min_id, max_id = catalog_dce_json(file_path)
        total_count += count
        if entry.owns:
            owned_count += count
        if count != entry.count:
            problems.append(
                f"Count mismatch for {entry.path}: manifest={entry.count}, actual={count}"
            )
        if min_id != entry.min_id:
            problems.append(
                f"min_id mismatch for {entry.path}: manifest={entry.min_id}, actual={min_id}"
            )
        if max_id != entry.max_id:
            problems.append(
                f"max_id mismatch for {entry.path}: manifest={entry.max_id}, actual={max_id}"
            )
        min_id_int = int(min_id) if min_id else None
        max_id_int = int(max_id) if max_id else None
        if entry.owns and last_max is not None and min_id_int is not None:
            if min_id_int <= last_max:
                problems.append(f"Owned file overlap detected at {entry.path}")
        if entry.owns and max_id_int is not None:
            last_max = max_id_int

    print(
        f"Manifest checks complete. Total messages: {total_count}, owned: {owned_count}"
    )
    if problems:
        print("Issues detected:")
        for issue in problems:
            print(f" - {issue}")
        raise SystemExit(1)


def verify_command(args: argparse.Namespace) -> None:
    channel_id = args.channel_id
    out_dir = Path(args.out_dir).resolve()
    channel_root = out_dir / f"channel={channel_id}"
    manifest_file = (
        Path(args.manifest_file).resolve()
        if args.manifest_file
        else channel_root / "_state" / "manifest.json"
    )
    manifest = ManifestStore(manifest_file, channel_id, args.overlap_ms)
    verify_manifest(manifest, out_dir)


def list_windows_command(args: argparse.Namespace) -> None:
    state_path = Path(args.state_file).resolve()
    with state_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    for window in data.get("windows", []):
        start = ts_ms_to_iso(window["start_ms"])
        end = ts_ms_to_iso(window["end_ms"])
        print(
            f"{window['window_id']:>16} {window.get('status', 'pending'):>10} "
            f"{window.get('try_count', 0):>3} {start} -> {end}"
        )


def combine_command(args: argparse.Namespace) -> None:
    channel_id = args.channel_id
    out_dir = Path(args.out_dir).resolve()
    channel_root = out_dir / f"channel={channel_id}"
    manifest_file = (
        Path(args.manifest_file).resolve()
        if args.manifest_file
        else channel_root / "_state" / "manifest.json"
    )
    manifest = ManifestStore(manifest_file, channel_id, 0)

    entries = manifest.entries()
    if not entries:
        raise SystemExit("No manifest entries available for combination.")

    selected: List[ManifestEntry] = []
    for entry in entries:
        if entry.owns or args.include_unowned:
            selected.append(entry)
    if not selected:
        raise SystemExit(
            "No manifest entries matched selection. Use --include-unowned to include all files."
        )

    def entry_sort_key(item: ManifestEntry) -> Tuple[int, str]:
        if item.min_id is not None:
            try:
                return int(item.min_id), item.path
            except ValueError:
                pass
        return (item.start_ms, item.path)

    selected.sort(key=entry_sort_key)

    combined_after: Optional[Tuple[datetime, str]] = None
    combined_before: Optional[Tuple[datetime, str]] = None
    first_guild: Optional[Dict[str, Any]] = None
    first_channel: Optional[Dict[str, Any]] = None
    file_paths: List[Tuple[ManifestEntry, Path]] = []

    for entry in selected:
        file_path = out_dir / entry.path
        if not file_path.exists():
            raise SystemExit(f"Manifest references missing file: {entry.path}")
        with file_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        guild_info = data.get("guild")
        channel_info = data.get("channel")
        if guild_info is None or channel_info is None:
            raise SystemExit(f"File {entry.path} missing guild/channel metadata.")
        if first_guild is None:
            first_guild = guild_info
            first_channel = channel_info
        else:
            if guild_info.get("id") != first_guild.get("id"):
                raise SystemExit(
                    f"Guild mismatch detected in {entry.path} (expected {first_guild.get('id')})"
                )
            if channel_info.get("id") != first_channel.get("id"):
                raise SystemExit(
                    f"Channel mismatch detected in {entry.path} (expected {first_channel.get('id')})"
                )
        date_range = data.get("dateRange") or {}
        after_str = date_range.get("after")
        before_str = date_range.get("before")
        if after_str:
            after_dt = parse_dce_timestamp(after_str)
            if combined_after is None or after_dt < combined_after[0]:
                combined_after = (after_dt, after_str)
        if before_str:
            before_dt = parse_dce_timestamp(before_str)
            if combined_before is None or before_dt > combined_before[0]:
                combined_before = (before_dt, before_str)
        file_paths.append((entry, file_path))

    if first_guild is None or first_channel is None:
        raise SystemExit("Unable to determine guild/channel metadata for combination.")

    combined_range = {
        "after": combined_after[1] if combined_after else None,
        "before": combined_before[1] if combined_before else None,
    }

    output_path = (
        Path(args.output_file).resolve()
        if args.output_file
        else channel_root / "combined" / "dce-combined.json"
    )
    if output_path.exists() and not args.overwrite:
        raise SystemExit(
            f"Output file {output_path} already exists. Use --overwrite to replace it."
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    exported_at = datetime.now(tz=UTC).isoformat()
    total_written = 0
    seen_ids: Optional[set[str]] = set() if args.dedupe else None
    first_message = True

    with output_path.open("w", encoding="utf-8") as out_file:
        out_file.write("{\n")
        out_file.write('  "guild": ' + format_json_block(first_guild, 1) + ",\n")
        out_file.write('  "channel": ' + format_json_block(first_channel, 1) + ",\n")
        out_file.write('  "dateRange": ' + format_json_block(combined_range, 1) + ",\n")
        out_file.write(f'  "exportedAt": "{exported_at}",\n')
        out_file.write('  "messages": [\n')

        for entry, file_path in file_paths:
            with file_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            for message in data.get("messages", []):
                msg_id = str(message.get("id") or message.get("Id") or "")
                if seen_ids is not None:
                    if not msg_id:
                        continue
                    if msg_id in seen_ids:
                        continue
                    seen_ids.add(msg_id)
                message_json = json.dumps(
                    message, ensure_ascii=False, indent=2, sort_keys=False
                )
                message_json = message_json.replace("\n", "\n    ")
                if first_message:
                    out_file.write("    " + message_json)
                    first_message = False
                else:
                    out_file.write(",\n    " + message_json)
                total_written += 1

        out_file.write("\n  ],\n")
        out_file.write(f'  "messageCount": {total_written}\n')
        out_file.write("}\n")

    print(f"Wrote combined export to {output_path} ({total_written} messages).")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="archiver", description="Discord channel archiver orchestrator."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common_export_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--token", help="Discord bot token")
        subparser.add_argument(
            "--token-file", help="Path to file containing the bot token"
        )
        subparser.add_argument(
            "--channel-id", required=True, help="Target Discord channel ID"
        )
        subparser.add_argument(
            "--out",
            dest="out_dir",
            default="./archive",
            help="Archive output directory",
        )
        subparser.add_argument(
            "--dce-image",
            default="tyrrrz/discordchatexporter:stable",
            help="Docker image for DiscordChatExporter",
        )
        subparser.add_argument(
            "--include-threads",
            default="All",
            choices=["All", "Active", "None", "all", "active", "none"],
            help="Thread inclusion policy for DCE exports",
        )
        subparser.add_argument(
            "--partition", type=int, default=50_000, help="DCE partition size"
        )
        subparser.add_argument("--state-file", help="Override path to state.json")
        subparser.add_argument(
            "--overlap", default="5m", help="Window overlap (e.g. 5m, 30s)"
        )

    backfill = sub.add_parser("backfill", help="Run a historical backfill export")
    add_common_export_args(backfill)
    backfill.add_argument(
        "--start", type=parse_date_or_datetime, help="Backfill start datetime (UTC)"
    )
    backfill.add_argument(
        "--end", type=parse_date_or_datetime, help="Backfill end datetime (UTC)"
    )
    backfill.add_argument(
        "--window",
        dest="window_mode",
        default="calendar-day",
        choices=["calendar-day", "calendar-hour", "calendar-week"],
        help="Windowing strategy",
    )
    backfill.add_argument(
        "--resume", action="store_true", help="Resume from existing state if present"
    )
    backfill.add_argument(
        "--dry-run", action="store_true", help="Plan windows without executing exports"
    )
    backfill.add_argument(
        "--verify-only", action="store_true", help="Only run manifest verification"
    )

    sync = sub.add_parser("sync", help="Run incremental sync from the high-water mark")
    add_common_export_args(sync)

    verify = sub.add_parser("verify", help="Verify manifest and file integrity")
    verify.add_argument("--channel-id", required=True, help="Target Discord channel ID")
    verify.add_argument(
        "--out", dest="out_dir", default="./archive", help="Archive output directory"
    )
    verify.add_argument("--manifest-file", help="Override manifest path")
    verify.add_argument("--overlap", dest="overlap_ms", default="5m")

    list_windows = sub.add_parser(
        "list-windows", help="List planned windows from a state file"
    )
    list_windows.add_argument("--state-file", required=True, help="Path to state.json")

    combine = sub.add_parser(
        "combine",
        help="Combine manifest-selected partitions into a single DCE export file",
    )
    combine.add_argument(
        "--channel-id", required=True, help="Target Discord channel ID"
    )
    combine.add_argument(
        "--out", dest="out_dir", default="./archive", help="Archive output directory"
    )
    combine.add_argument("--manifest-file", help="Override manifest path")
    combine.add_argument("--output-file", help="Destination file for combined export")
    combine.add_argument(
        "--include-unowned",
        action="store_true",
        help="Include manifest entries marked owns=false",
    )
    combine.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite destination file if it already exists",
    )
    combine.add_argument(
        "--dedupe",
        action="store_true",
        help="De-duplicate messages by ID while combining (uses additional memory)",
    )

    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if hasattr(args, "overlap"):
        args.overlap_ms = parse_duration_to_ms(args.overlap)
    elif hasattr(args, "overlap_ms") and isinstance(args.overlap_ms, str):
        args.overlap_ms = parse_duration_to_ms(args.overlap_ms)

    if hasattr(args, "include_threads"):
        args.include_threads = args.include_threads.capitalize()

    command = args.command

    if command == "backfill":
        backfill_command(args)
    elif command == "sync":
        sync_command(args)
    elif command == "verify":
        verify_command(args)
    elif command == "list-windows":
        list_windows_command(args)
    elif command == "combine":
        combine_command(args)
    else:
        parser.error(f"Unknown command '{command}'")


if __name__ == "__main__":
    main()
