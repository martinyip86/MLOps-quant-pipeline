#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(dirname -- "$SCRIPT_DIR")
BUILDER="$SCRIPT_DIR/build_breakout_prop_guide.py"
BUNDLED_PYTHON="/Users/martin.yip/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3"

if [ -x "$BUNDLED_PYTHON" ]; then
    PYTHON_BIN="$BUNDLED_PYTHON"
elif python3 -c "import docx" >/dev/null 2>&1; then
    PYTHON_BIN=$(command -v python3)
else
    echo "導出失敗：目前的 Python 缺少 python-docx。" >&2
    echo "請在 Codex 工作區內執行此腳本，或先在你的 Python 環境安裝 python-docx。" >&2
    exit 1
fi

cd "$PROJECT_DIR"
"$PYTHON_BIN" "$BUILDER"
