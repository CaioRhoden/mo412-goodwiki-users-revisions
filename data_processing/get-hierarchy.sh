#!/bin/bash

git clone https://github.com/jamesmishra/mysqldump-to-csv
cd mysqldump-to-csv
git checkout 24301dfa739c13025844ed3ff5a8abe093ced6cc
patch <<'EOF'
diff --git a/mysqldump_to_csv.py b/mysqldump_to_csv.py
index b49cfe7..8d5bb2a 100644
--- a/mysqldump_to_csv.py
+++ b/mysqldump_to_csv.py
@@ -101,7 +101,8 @@ def main():
     # listed in sys.argv[1:]
     # or stdin if no args given.
     try:
-        for line in fileinput.input():
+        sys.stdin.reconfigure(errors='ignore')
+        for line in fileinput.input(encoding="utf-8", errors="ignore"):
             # Look for an INSERT statement and parse it.
             if is_insert(line):
                 values = get_values(line)
EOF

cd ..

wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-categorylinks.sql.gz
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page.sql.gz
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-linktarget.sql.gz

db=enwiki.sqlite
rm -f "$db"

sqlite3 "$db" 'create table "page"("page_id" integer, "page_namespace" integer, "page_title" text, "page_is_redirect" integer, "page_len" integer)'
time zcat enwiki-latest-page.sql.gz | python mysqldump-to-csv/mysqldump_to_csv.py | csvtool col 1,2,3,4,10 - | sqlite3 "$db" ".import --csv '|cat -' page"
time sqlite3 "$db" 'create unique index "page_id" on "page"("page_id")'
time sqlite3 "$db" 'create index "page_namespace_title" on "page"("page_namespace", "page_title")'

# categorylinks
sqlite3 "$db" 'create table categorylinks("cl_from" integer, "lt_id" text)'
time zcat enwiki-latest-categorylinks.sql.gz | python mysqldump-to-csv/mysqldump_to_csv.py | csvtool col 1,7 - | sqlite3 "$db" ".import --csv '|cat -' categorylinks"
time sqlite3 "$db" 'create index "categorylinks_to" on categorylinks("lt_id")'

# linktarget
sqlite3 "$db" 'create table linktarget("lt_id" integer, "namespace" integer, "cl_to" text)'
time zcat enwiki-latest-linktarget.sql.gz | python mysqldump-to-csv/mysqldump_to_csv.py | csvtool col 1,2,3 - | sqlite3 "$db" ".import --csv '|cat -' linktarget"

# create a new table joining categorylinks and linktarget to get category names and pages in one table
time sqlite3 "$db" '
create table links as
select
    page.page_namespace,
    page.page_title,
    lt.namespace,
    lt.cl_to
from categorylinks cl
join linktarget lt on cl.lt_id = lt.lt_id
join page on cl.cl_from = page.page_id
'

sqlite3 "$db" 'select page_title, cl_to from links where page_namespace = 14 and namespace = 14' > ../data/links.csv