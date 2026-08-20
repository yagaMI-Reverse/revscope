"""Прогоняет bench.run_all и записывает КАЖДУЮ строку вывода с реальной
временной меткой. Из этого лога собирается видео, где темп воспроизведения
равен темпу настоящего прогона — ничего не досочиняется.

    python -u bench/capture_run.py

Результат: bench/out/run_capture.jsonl  ({t: секунды от старта, line: текст})
"""
import json
import subprocess
import sys
import time
from pathlib import Path

OUT = Path(__file__).resolve().parent / "out" / "run_capture.jsonl"
OUT.parent.mkdir(parents=True, exist_ok=True)

started = time.monotonic()
rows = []

proc = subprocess.Popen(
    [sys.executable, "-u", "-m", "bench.run_all"],
    cwd=str(Path(__file__).resolve().parent.parent),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    encoding="utf-8",
    errors="replace",
    bufsize=1,
)

for line in proc.stdout:
    t = round(time.monotonic() - started, 3)
    line = line.rstrip("\n")
    rows.append({"t": t, "line": line})
    print(f"[{t:8.3f}s] {line}", flush=True)

code = proc.wait()
total = round(time.monotonic() - started, 3)

OUT.write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
    encoding="utf-8",
)
print(f"\ncaptured {len(rows)} lines in {total}s -> {OUT} (exit {code})")
