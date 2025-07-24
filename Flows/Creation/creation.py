import os
import re
import yaml
import argparse
import snowflake.connector
import logging
from collections import Counter

# ────────────────────────────────────────────────────────────────────────────────
# Logging configuration
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Paths & Regex patterns
# ────────────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir))
DDL_PATH = os.path.join(ROOT_DIR, "Tables", "Table", "create_tables.sql")

RE_STMTS = re.compile(
    r'(?is)'  # DOTALL + IGNORECASE
    r'('      # capture group for statements
    r'\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|ALTER\s+TABLE|INSERT\s+INTO)\b.*?;'
    r')'
)
RE_CREATE = re.compile(r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE', re.IGNORECASE)
RE_NAME = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+"?(?P<name>[^"\s(]+)"?',
    re.IGNORECASE
)

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def list_clients():
    path = os.path.join(ROOT_DIR, "Clients")
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Clients folder not found at {path}")
    return sorted(d for d in os.listdir(path)
                  if os.path.isdir(os.path.join(path, d)))


def load_config(client_name):
    cfg_file = os.path.join(ROOT_DIR, "Clients", client_name, "config.yml")
    if not os.path.exists(cfg_file):
        raise FileNotFoundError(f"Config not found for {client_name}: {cfg_file}")
    with open(cfg_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def extract_statements(sql_text):
    stmts = [m.group(1).strip() for m in RE_STMTS.finditer(sql_text)]
    logger.info(f"🔍 Extracted {len(stmts)} statements from {DDL_PATH}")
    return stmts


def read_statements():
    if not os.path.exists(DDL_PATH):
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")
    with open(DDL_PATH, 'r', encoding='utf-8') as f:
        return extract_statements(f.read())


def extract_table_name(stmt):
    m = RE_NAME.match(stmt)
    return m.group('name') if m else None


def table_exists(conn, schema, table):
    query = (
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema.upper()}' "
        f"AND TABLE_NAME = '{table.upper()}'"
    )
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetchone()[0] > 0
    finally:
        cur.close()


def apply_statements(conn, schema, statements, replace_existing=False, dry_run=False):
    summary = Counter(created=0, skipped=0, altered=0, inserted=0, errors=0)

    # switch to target schema
    c = conn.cursor()
    c.execute(f"USE SCHEMA {schema}")
    c.close()
    logger.info(f"Using schema: {schema}")

    for i, stmt in enumerate(statements, start=1):
        kind = stmt.split()[0].upper()
        table = extract_table_name(stmt)
        try:
            if RE_CREATE.match(stmt):
                if table and not replace_existing and table_exists(conn, schema, table):
                    logger.info(f"[{i}] ⏩ Skipping existing table {table}")
                    summary['skipped'] += 1
                else:
                    sql = stmt
                    if not replace_existing:
                        sql = stmt.replace(
                            "CREATE OR REPLACE TABLE",
                            "CREATE TABLE IF NOT EXISTS"
                        )
                    if not dry_run:
                        conn.cursor().execute(sql)
                    logger.info(f"[{i}] ✅ Created table {table}")
                    summary['created'] += 1

            elif kind == 'ALTER':
                if not dry_run:
                    conn.cursor().execute(stmt)
                logger.info(f"[{i}] 🔧 Executed ALTER TABLE")
                summary['altered'] += 1

            elif kind == 'INSERT':
                if not dry_run:
                    conn.cursor().execute(stmt)
                logger.info(f"[{i}] ➕ Executed INSERT")
                summary['inserted'] += 1

            else:
                logger.debug(f"[{i}] 🚧 Unsupported statement, skipping: {stmt[:30]}…")
        except Exception as e:
            logger.error(f"[{i}] ❌ Error on {kind}: {e}")
            summary['errors'] += 1

    return summary

# ────────────────────────────────────────────────────────────────────────────────
# CLI + interactive fallback
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    clients = list_clients()
    parser = argparse.ArgumentParser(
        description="Create/update tables in Snowflake per-client schema"
    )
    parser.add_argument('--client', '-c', choices=clients,
                        help='Client key (folder name)')
    parser.add_argument('--schema', '-s',
                        help='Override target schema name from config')
    parser.add_argument('--replace', action='store_true',
                        help='Force DROP & REPLACE all tables')
    parser.add_argument('--dry-run', action='store_true',
                        help="Show SQL without executing")
    return parser.parse_args()


def prompt_for_client(clients):
    print("Available clients:", clients)
    raw = input("Enter client name: ").strip()
    lookup = {c.lower(): c for c in clients}
    key = raw.lower()
    if key not in lookup:
        logger.error(f"Unknown client '{raw}'. Exiting.")
        exit(1)
    return lookup[key]


def prompt_for_schema(default_schema):
    raw = input(f"Enter target schema [{default_schema}]: ").strip()
    return raw or default_schema


def main():
    args = parse_args()
    clients = list_clients()

    # determine client
    if args.client:
        client = args.client
    else:
        client = prompt_for_client(clients)

    cfg = load_config(client)['snowflake']
    default_schema = cfg.get('schema')

    # determine schema
    schema = args.schema or default_schema
    if not args.schema:
        schema = prompt_for_schema(schema)

    replace_flag = args.replace
    dry_run_flag = args.dry_run

    # execute
    statements = read_statements()
    conn = snowflake.connector.connect(
        user=cfg['user'],
        password=cfg['password'],
        account=cfg['account'],
        warehouse=cfg['warehouse'],
        database=cfg['database'],
        role=cfg.get('role', 'SYSADMIN')
    )
    try:
        summary = apply_statements(
            conn,
            schema,
            statements,
            replace_existing=replace_flag,
            dry_run=dry_run_flag
        )
    finally:
        conn.close()
        logger.info("Connection closed.")

    # final report
    logger.info("---- Summary ----")
    logger.info(f"Created : {summary['created']}")
    logger.info(f"Skipped : {summary['skipped']}")
    logger.info(f"Altered : {summary['altered']}")
    logger.info(f"Inserted: {summary['inserted']}")
    logger.info(f"Errors  : {summary['errors']}")

if __name__ == '__main__':
    main()
