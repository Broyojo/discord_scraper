set -euo pipefail

uv run main.py backfill \
    --token $BOBBYBOT_TOKEN \
    --channel-id 699326565351162036 \
    --out output \
    --dce-image tyrrrz/discordchatexporter:stable \
    --start 2020-04-13 \
    --end 2025-06-07 \
    --resume