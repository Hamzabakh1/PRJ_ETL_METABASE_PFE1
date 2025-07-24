#!/usr/bin/env python3
import os
from pathlib import Path
import yaml
import getpass
from datetime import datetime

# ----- Configuration Version & Metadata -----
CONFIG_VERSION = "1.0"

# ----- Default configuration values -----
DEFAULTS = {
    "client_id": "cl2",
    "client_name": "Client2",
    "source_db": {
        "driver": "{ODBC Driver 17 for SQL Server}",
        "server": "DESKTOP-AGUMSPH",
        "database": "DATABASE",
        "trusted_connection": "yes",
        "connection_timeout": 30,
    },
    "snowflake": {
        "account":   "your_snowflake_account",
        "user":      "USER",
        "password":  "your_password_account",
        # Alternatively, consider using env var: os.getenv("SF_PASSWORD", DEFAULTS["snowflake"]["password"])
        "warehouse": "COMPUTE_WH",
        "database":  "BEE_CENTER",
        "schema":    "CLIENT2",
        "role":      "ACCOUNTADMIN",
    },
    "etl": {
        "chunk_size":        100000,
        "create_or_replace": False,
        "date_format":       "%Y-%m-%d",
        "max_workers":       8,
    },
    "etl_flow":    "../../Flows/ETL/flow_prefect.py",
    "queries_path": "../../Tables/Queries/queries.py",
}

# Directory where client folders reside (this script's folder)
CLIENTS_DIR = Path(__file__).parent.resolve()


def list_clients():
    """Return existing client folder names."""
    CLIENTS_DIR.mkdir(parents=True, exist_ok=True)
    return [p.name for p in CLIENTS_DIR.iterdir() if p.is_dir()]


def choose(field_label, default, secret=False):
    """
    Let the user keep a default value or enter a new one.
    """
    print(f"\n{field_label}:")
    print(f"  1) Keep default ({default})")
    print("  2) Enter new")
    while True:
        sel = input("Choose [1/2]: ").strip()
        if sel == '1':
            return default
        if sel == '2':
            if secret:
                return getpass.getpass("Enter new value: ")
            return input("Enter new value: ").strip()
        print("Please enter 1 or 2.")


def load_config(path: Path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_config(cfg: dict, path: Path):
    """
    Prepend metadata (version, timestamp, author) and write the full config to YAML.
    """
    meta = {
        "version": CONFIG_VERSION,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "author": Path(__file__).name,
    }
    # Insert metadata before the rest of the config
    full_config = {**meta, **cfg}
    with open(path, "w") as f:
        yaml.dump(full_config, f, sort_keys=False)
    print(f"✅  Saved: {path}")


def interactive_create():
    print("\n🆕 Creating new client configuration (defaults prefilled)")
    cfg = {}
    # Client identifiers
    cfg["client_id"]   = choose("Client ID",   DEFAULTS["client_id"])
    cfg["client_name"] = choose("Client Name", DEFAULTS["client_name"])

    # Source DB settings
    sd = DEFAULTS["source_db"]
    cfg["source_db"] = {
        "driver": sd["driver"],
        "server": choose("SQL Server host",       sd["server"]),
        "database": choose("SQL Server database", sd["database"]),
        "trusted_connection": sd["trusted_connection"],
        "connection_timeout": sd["connection_timeout"],
    }

    # Snowflake settings
    sf = DEFAULTS["snowflake"]
    cfg["snowflake"] = {
        "account":   choose("Snowflake account",   sf["account"]),
        "user":      choose("Snowflake user",      sf["user"]),
        "password":  choose("Snowflake password",  sf["password"], secret=True),
        "warehouse": choose("Snowflake warehouse", sf["warehouse"]),
        "database":  choose("Snowflake database",  sf["database"]),
        "schema":    choose("Snowflake schema",    sf["schema"]),
        "role":      choose("Snowflake role",      sf["role"]),
    }

    # ETL settings (with YAML anchor support suggestion)
    et = DEFAULTS["etl"]
    cfg["etl"] = {
        "chunk_size":        int(choose("ETL chunk size",        et["chunk_size"])),
        "create_or_replace": choose("Create or replace? (y/n)", 'y' if et["create_or_replace"] else 'n').lower().startswith('y'),
        "date_format":       choose("Date format",             et["date_format"]),
        "max_workers":       int(choose("Max workers",           et["max_workers"])),
    }

    # Paths
    cfg["etl_flow"]     = choose("Path to etl_flow",     DEFAULTS["etl_flow"])
    cfg["queries_path"] = choose("Path to queries_path", DEFAULTS["queries_path"])
    return cfg


def interactive_update(old: dict):
    print("\n🔄 Updating existing configuration (choose to keep or override)")
    new = {}
    # Client identifiers
    new["client_id"]   = choose("Client ID",   old.get("client_id", DEFAULTS["client_id"]))
    new["client_name"] = choose("Client Name", old.get("client_name", DEFAULTS["client_name"]))

    # Source DB
    sd = old.get("source_db", DEFAULTS["source_db"])
    new["source_db"] = {
        "driver": sd.get("driver", DEFAULTS["source_db"]["driver"]),
        "server": choose("SQL Server host",       sd.get("server")),
        "database": choose("SQL Server database", sd.get("database")),
        "trusted_connection": sd.get("trusted_connection", DEFAULTS["source_db"]["trusted_connection"]),
        "connection_timeout": sd.get("connection_timeout", DEFAULTS["source_db"]["connection_timeout"]),
    }

    # Snowflake
    sf = old.get("snowflake", DEFAULTS["snowflake"])
    new["snowflake"] = {
        "account":   choose("Snowflake account",   sf.get("account")),
        "user":      choose("Snowflake user",      sf.get("user")),
        "password":  choose("Snowflake password",  sf.get("password"), secret=True),
        "warehouse": choose("Snowflake warehouse", sf.get("warehouse")),
        "database":  choose("Snowflake database",  sf.get("database")),
        "schema":    choose("Snowflake schema",    sf.get("schema")),
        "role":      choose("Snowflake role",      sf.get("role")),
    }

    # ETL
    et = old.get("etl", DEFAULTS["etl"])
    new["etl"] = {
        "chunk_size":        int(choose("ETL chunk size",        et.get("chunk_size", DEFAULTS["etl"]["chunk_size"]))),
        "create_or_replace": choose("Create or replace? (y/n)", 'y' if et.get("create_or_replace") else 'n').lower().startswith('y'),
        "date_format":       choose("Date format",             et.get("date_format", DEFAULTS["etl"]["date_format"])),
        "max_workers":       int(choose("Max workers",           et.get("max_workers", DEFAULTS["etl"]["max_workers"]))),
    }

    # Paths
    new["etl_flow"]     = choose("Path to etl_flow",     old.get("etl_flow",     DEFAULTS["etl_flow"]))
    new["queries_path"] = choose("Path to queries_path", old.get("queries_path", DEFAULTS["queries_path"]))
    return new


def main():
    existing = list_clients()
    print("📂 Existing clients:", ", ".join(existing) if existing else "(none)")

    # Prompt for folder name
    while True:
        name = input("\nEnter client folder name: ").strip()
        if name:
            break
        print("Folder name cannot be empty.")

    if name in existing:
        print(f"\n⚠️  Client “{name}” already exists.")
        if input("Update its config? (y/n) [default: y]: ").strip().lower() in ('', 'y', 'yes'):
            cfg_path = CLIENTS_DIR / name / "config.yml"
            old_cfg = load_config(cfg_path)
            cfg = interactive_update(old_cfg)
        else:
            print("Aborting.")
            return
    else:
        (CLIENTS_DIR / name).mkdir(parents=True, exist_ok=True)
        cfg = interactive_create()

    save_config(cfg, CLIENTS_DIR / name / "config.yml")

if __name__ == "__main__":
    main()
