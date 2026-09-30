import os

# Rich reads COLUMNS when the console is created; keep test output on one line per row.
os.environ.setdefault("COLUMNS", "200")
