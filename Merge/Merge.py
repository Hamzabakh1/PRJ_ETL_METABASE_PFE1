import os
import snowflake.connector

# 1 ✨ Connexion
conn = snowflake.connector.connect(
    account   = os.getenv("SF_ACCOUNT",   "UFVXKZD-YW58283"),
    user      = os.getenv("SF_USER",      "HB20112002"),
    password  = os.getenv("SF_PASSWORD",  "HamzaBakh2002@@2002@@2002@@"),
    warehouse = os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    role      = os.getenv("SF_ROLE",      "ACCOUNTADMIN"),
    database  = "BEE_CENTRAL",
    schema    = "PUBLIC"
)
cur = conn.cursor()

try:
    # 2 ✨ Récupérer mapping SCHEMA → ID_CLIENT
    cur.execute("""
        SELECT SCHEMA_NAME, ID_CLIENT
          FROM BEE_MASTER.PUBLIC.CLIENT_DATABASES
    """)
    raw_mapping = cur.fetchall()
    mapping = {}
    id_client_seen = set()
    for schema, id_client in raw_mapping:
        if id_client in id_client_seen:
            print(f"⚠️  Duplicate ID_CLIENT {id_client} for schema {schema}, skipping this schema.")
            continue
        mapping[schema] = id_client
        id_client_seen.add(id_client)

    # 3 ✨ Lister les schémas réels dans BEE_CENTRAL
    cur.execute("""
        SELECT SCHEMA_NAME
          FROM BEE_CENTRAL.INFORMATION_SCHEMA.SCHEMATA
         WHERE SCHEMA_NAME LIKE 'BEE_TEST%'
    """)
    actual_schemas = {r[0] for r in cur.fetchall()}
    print("Schemas trouvés :", actual_schemas)

    # 4 ✨ Filtrer mapping par schémas existants
    schemas_to_merge = [s for s in mapping if s in actual_schemas]
    if not schemas_to_merge:
        print("🚫 No schemas to merge. Exiting.")
        exit()
    print("Schemas à fusionner :", schemas_to_merge)

    # 5 ✨ Tables concernées par la fusion
    tables = [
        'BUDGET','COMPTES_ANALYTIQUES','COMPTES_BUDGETAIRES',
        'COMPTES_PL','COUTS_BEEONE','PRODUCTION_BEEONE',
        'PROFIL_DE_PRODUCTION','VERSIONS_BUDGET','DIM_CALENDAR'
    ]

    # 6 ✨ Fusion incrémentale
    for tbl in tables:
        target = f"BEE_MERGE.PUBLIC.{tbl}"

        # Création de la table cible si absente
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target}
            LIKE BEE_CENTRAL.{schemas_to_merge[0]}.{tbl}
        """)

        # S'assurer que la colonne ID_CLIENT existe
        cur.execute(f"DESC TABLE {target}")
        target_cols = [r[0] for r in cur.fetchall()]
        if "ID_CLIENT" not in target_cols:
            cur.execute(f"ALTER TABLE {target} ADD COLUMN ID_CLIENT NUMBER")

        # Récupérer les ID_CLIENT déjà fusionnés
        cur.execute(f"SELECT DISTINCT ID_CLIENT FROM {target}")
        done = {r[0] for r in cur.fetchall()}

        # Fusionner les clients non encore traités
        for schema in schemas_to_merge:
            id_client = mapping[schema]
            if id_client in done:
                print(f"→ Skipping {schema} (ID_CLIENT={id_client})")
                continue

            # Décrire colonnes source
            cur.execute(f"DESC TABLE BEE_CENTRAL.{schema}.{tbl}")
            cols = [row[0] for row in cur.fetchall()]
            quoted_cols = [f'"{col}"' for col in cols]

            # Préparer insertion
            cols_str = ", ".join(quoted_cols) + ("" if "ID_CLIENT" in cols else ', "ID_CLIENT"')
            select_cols = ", ".join(quoted_cols) + ("" if "ID_CLIENT" in cols else ", %s AS ID_CLIENT")
            params = () if "ID_CLIENT" in cols else (id_client,)

            cur.execute(f"""
                INSERT INTO {target} ({cols_str})
                SELECT {select_cols}
                  FROM BEE_CENTRAL.{schema}.{tbl}
            """, params)

            print(f"✔ Merged {schema}.{tbl} as ID_CLIENT={id_client}")

    print("\n✨ Fusion terminée. Seuls les nouveaux clients ont été ajoutés.")

finally:
    cur.close()
    conn.close()
