#!/bin/bash
# Deploy EdgeRunner to VPS — code only, no datasets or test files
# Usage: bash deploy/sync.sh
#
# Excludes:
#   - data/dataset/     (36GB Jon-Becker dataset)
#   - data/trevorjs/    (5.7GB TrevorJS dataset)
#   - data/sportsbook/  (sportsbook moneyline datasets)
#   - tests/            (backtest and research scripts)
#   - .venv/            (Python virtual environment)
#   - .git/             (git history)
#   - __pycache__/      (compiled Python)
#   - *.parquet         (any stray data files)
#   - data/flow_log.jsonl       (runtime log)
#   - data/discovery_prices.json (runtime cache)
#   - data/peak_prices.json     (runtime cache)

set -e

SSH_KEY="${HOME}/.ssh/digitalocean_edgerunner"
HOST="root@159.65.177.244"
REMOTE_DIR="/root/edgerunner"

echo "=== EdgeRunner Deploy ==="
echo "Target: ${HOST}:${REMOTE_DIR}"
echo ""

# Files to sync (code only)
DIRS="config signals execution data/cache.py data/espn_scores.py data/espn_standings.py data/feeds.py data/market_poller.py data/nba_poller.py data/peak_cache.py data/smart_money.py data/flow_logger.py data/discovery_cache.py data/__init__.py storage alerts deploy keys"
FILES="main.py runner.py requirements.txt .env CLAUDE.md"

echo "Syncing code directories..."
for dir in config signals execution storage alerts deploy keys; do
    scp -i "$SSH_KEY" -r "$dir" "${HOST}:${REMOTE_DIR}/" 2>/dev/null && echo "  OK: $dir/" || echo "  SKIP: $dir/"
done

echo "Syncing data module files..."
for f in data/__init__.py data/cache.py data/espn_scores.py data/espn_standings.py data/feeds.py data/market_poller.py data/nba_poller.py data/peak_cache.py data/smart_money.py data/flow_logger.py data/discovery_cache.py data/hwm_cache.py data/bayesian_cache.py; do
    scp -i "$SSH_KEY" "$f" "${HOST}:${REMOTE_DIR}/data/" 2>/dev/null && echo "  OK: $f" || echo "  SKIP: $f"
done

echo "Syncing root files..."
for f in $FILES; do
    scp -i "$SSH_KEY" "$f" "${HOST}:${REMOTE_DIR}/" 2>/dev/null && echo "  OK: $f" || echo "  SKIP: $f"
done

echo ""
echo "Verifying on server..."
ssh -i "$SSH_KEY" "$HOST" "cd ${REMOTE_DIR} && source .venv/bin/activate && python -m py_compile main.py && python -m py_compile signals/rules.py && echo 'Compile OK'"

echo ""
echo "=== Deploy Complete ==="
echo "To restart: ssh -i $SSH_KEY $HOST 'systemctl restart edgerunner'"
echo "To restart now, run: bash deploy/sync.sh && ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'systemctl restart edgerunner'"
