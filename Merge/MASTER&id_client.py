import os
import snowflake.connector

# ── 1️⃣ Charger la config depuis les variables d’environnement ───────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT",   "UFVXKZD-YW58283")
SF_USER      = os.getenv("SF_USER",      "HB20112002")
SF_PASSWORD  = os.getenv("SF_PASSWORD",  "HamzaBakh2002@@2002@@2002@@")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_ROLE      = os.getenv("SF_ROLE",      "ACCOUNTADMIN")

# On va qualifier à chaque fois DATABASE et SCHEMA,
# donc la connexion par défaut peut pointer sur BEE_CENTRAL.PUBLIC
conn = snowflake.connector.connect(
    account   = SF_ACCOUNT,
    user      = SF_USER,
    password  = SF_PASSWORD,
    warehouse = SF_WAREHOUSE,
    role      = SF_ROLE,
    database  = "BEE_CENTRAL",
    schema    = "PUBLIC"
)
cur = conn.cursor()

try:
    # ── 2️⃣ Créer la table de mapping si besoin ────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS BEE_MASTER.PUBLIC.client_databases (
            id_client      INT AUTOINCREMENT PRIMARY KEY,
            client_name    STRING,
            database_name  STRING,
            schema_name    STRING,
            status         STRING DEFAULT 'Active',
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)  # :contentReference[oaicite:1]{index=1}

    # ── 3️⃣ Lister les schémas dans BEE_CENTRAL ───────────────────────────────
    cur.execute("""
        SELECT SCHEMA_NAME
          FROM BEE_CENTRAL.INFORMATION_SCHEMA.SCHEMATA
         WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
    """)
    all_schemas = {row[0] for row in cur.fetchall()}

    # ── 4️⃣ Lister les mappings existants pour BEE_CENTRAL ──────────────────────
    cur.execute("""
        SELECT schema_name
          FROM BEE_MASTER.PUBLIC.client_databases
         WHERE database_name = 'BEE_CENTRAL'
    """)
    existing = {row[0] for row in cur.fetchall()}

    # ── 5️⃣ Insérer uniquement les nouveaux schémas ────────────────────────────
    new_schemas = all_schemas - existing
    for schema in new_schemas:
        cur.execute("""
            INSERT INTO BEE_MASTER.PUBLIC.client_databases
                (client_name, database_name, schema_name)
            VALUES (%s, %s, %s)
        """, (schema, 'BEE_CENTRAL', schema))
        print(f"➕ Nouveau client ajouté : {schema}")

    if not new_schemas:
        print("ℹ️ Aucun nouveau schéma à ajouter.")

    # ── 6️⃣ Récupérer le mapping à jour ────────────────────────────────────────
    cur.execute("""
        SELECT schema_name, id_client
          FROM BEE_MASTER.PUBLIC.client_databases
         WHERE database_name = 'BEE_CENTRAL'
    """)
    mapping = {row[0]: row[1] for row in cur.fetchall()}

    # ── 7️⃣ Tables à traiter ───────────────────────────────────────────────────
    tables = [
        'BUDGET',
        'COMPTES_ANALYTIQUES',
        'COMPTES_BUDGETAIRES',
        'COMPTES_PL',
        'COUTS_BEEONE',
        'PRODUCTION_BEEONE',
        'PROFIL_DE_PRODUCTION',
        'VERSIONS_BUDGET'
    ]

    # ── 8️⃣ Boucle principale : n’affecte que les schémas de mapping ──────────
    for schema_name, id_client in mapping.items():
        print(f"\n🔄 Traitement de {schema_name} (ID_CLIENT={id_client})")
        for tbl in tables:
            full_name = f"BEE_CENTRAL.{schema_name}.{tbl}"

            # 8.1 Vérifier si la table existe
            cur.execute("""
                SELECT COUNT(*) 
                  FROM BEE_CENTRAL.INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = %s
                   AND TABLE_NAME   = %s
            """, (schema_name, tbl))
            if cur.fetchone()[0] == 0:
                print(f" • Table introuvable : {full_name} → skip")
                continue

            # 8.2 Vérifier si colonne ID_CLIENT existe
            cur.execute("""
                SELECT COUNT(*) 
                  FROM BEE_CENTRAL.INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = %s
                   AND TABLE_NAME   = %s
                   AND COLUMN_NAME  = 'ID_CLIENT'
            """, (schema_name, tbl))
            if cur.fetchone()[0] == 0:
                # Ajouter la colonne
                cur.execute(f"ALTER TABLE {full_name} ADD COLUMN ID_CLIENT INT")
                print(f" ✅ Colonne ajoutée : {full_name}.ID_CLIENT")
            else:
                print(f" ✔️ Colonne déjà présente : {full_name}.ID_CLIENT")

            # 8.3 Mettre à jour les données
            cur.execute(f"UPDATE {full_name} SET ID_CLIENT = %s", (id_client,))
            print(f" 🔄 Mis à jour {full_name} → ID_CLIENT={id_client}")

finally:
    cur.close()
    conn.close()
