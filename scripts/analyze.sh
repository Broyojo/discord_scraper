set -euo pipefail

uv run main.py combine --channel-id 699326565351162036 --out output --dedupe --overwrite
bunx chat-analytics -p discord -i output/channel=699326565351162036/combined/dce-combined.json -o report.html
open report.html