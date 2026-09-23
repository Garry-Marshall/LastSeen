"""Run every tests/test_*.py script and summarise.

Each test is a standalone script (exit code 0 = passed) using a temporary
database, so it never touches the bot's real data. Like the bot, the tests
read locales/ relative to the working folder, so each runs from the repo root.
Run from anywhere:

    python tests/run_all.py            # all tests
    python tests/run_all.py search     # only tests with 'search' in the name
"""
import glob
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(here)
pattern = sys.argv[1] if len(sys.argv) > 1 else ''
scripts = [p for p in sorted(glob.glob(os.path.join(here, 'test_*.py'))) if pattern in os.path.basename(p)]

env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
failed = []
for path in scripts:
    name = os.path.basename(path)
    result = subprocess.run([sys.executable, path], capture_output=True, text=True, encoding='utf-8', env=env, cwd=root)
    if result.returncode == 0:
        print(f"PASS {name}")
    else:
        failed.append(name)
        print(f"FAIL {name}\n{result.stdout}{result.stderr}")

print(f"\n{len(scripts) - len(failed)}/{len(scripts)} passed")
sys.exit(1 if failed else 0)
