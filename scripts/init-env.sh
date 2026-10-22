#!/usr/bin/env bash
# Creates .env with random secrets. Run once; it refuses to overwrite.
#
#   ./scripts/init-env.sh                         # local
#   DOMAIN=yourname.duckdns.org ./scripts/init-env.sh   # production
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  echo ".env already exists; not overwriting." >&2
  exit 1
fi

rand() { openssl rand -hex "$1"; }
admin_password="$(rand 12)"

sed \
  -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(rand 24)/" \
  -e "s/^REDIS_PASSWORD=.*/REDIS_PASSWORD=$(rand 24)/" \
  -e "s/^ADMIN_PASSWORD=.*/ADMIN_PASSWORD=${admin_password}/" \
  -e "s/^CSRF_SECRET=.*/CSRF_SECRET=$(rand 32)/" \
  -e "s/^KEY_SECRET=.*/KEY_SECRET=$(rand 32)/" \
  -e "s/^DOMAIN=.*/DOMAIN=${DOMAIN:-yourname.duckdns.org}/" \
  .env.example > .env
chmod 600 .env

echo "Created .env"
echo "Dashboard admin login:  admin / ${admin_password}"
echo "(It's also in .env. Store it in your password manager.)"
