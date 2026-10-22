#!/usr/bin/env bash
# One-command deploy on the server: pull the latest code, rebuild, restart.
#
#   ./scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "No .env yet. Create one first:  DOMAIN=yourname.duckdns.org ./scripts/init-env.sh" >&2
  exit 1
fi
if grep -q '^DOMAIN=yourname.duckdns.org' .env; then
  echo "Set DOMAIN in .env to your real domain first." >&2
  exit 1
fi

git pull --ff-only
docker compose --profile prod up -d --build --remove-orphans
# nginx resolves api-1..3 once at startup; recreated API containers can get
# new IPs, so restart nginx to pick them up.
docker compose restart nginx
docker image prune -f >/dev/null

echo "Waiting for services to become healthy..."
for _ in $(seq 1 30); do
  if docker compose ps --format '{{.Service}} {{.Health}}' | grep -E 'api|dashboard' | grep -qv healthy; then
    sleep 2
  else
    break
  fi
done
docker compose --profile prod ps
domain="$(grep '^DOMAIN=' .env | cut -d= -f2)"
echo
echo "Live at https://${domain}  (first start can take a minute while Caddy gets the certificate)"
