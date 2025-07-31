import os
import snowflake.connector

# ── 1️⃣ Connexion à Snowflake depuis les variables d’environnement ─────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT",   "your_snowflake_account")
SF_USER      = os.getenv("SF_USER",      "USER")
SF_PASSWORD  = os.getenv("SF_PASSWORD",  "your_password_account")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_ROLE      = os.getenv("SF_ROLE",      "ACCOUNTADMIN")

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
    # ── 2️⃣ Créer la table de mapping si elle n’existe pas ─────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS BEE_MASTER.PUBLIC.client_databases (
            id_client      INT AUTOINCREMENT PRIMARY KEY,
            client_name    STRING,
            database_name  STRING,
            schema_name    STRING,
            status         STRING DEFAULT 'Active',
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── 3️⃣ Lister les schémas disponibles dans BEE_CENTRAL ───────────────────
    cur.execute("""
        SELECT SCHEMA_NAME
        FROM BEE_CENTRAL.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
    """)
    all_schemas = {row[0] for row in cur.fetchall()}

    # ── 4️⃣ Lister les schémas déjà mappés ─────────────────────────────────────
    cur.execute("""
        SELECT schema_name
        FROM BEE_MASTER.PUBLIC.client_databases
        WHERE database_name = 'BEE_CENTRAL'
    """)
    existing = {row[0] for row in cur.fetchall()}

    # ── 5️⃣ Insérer les nouveaux schémas dans la table de mapping ─────────────
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

    # ── 6️⃣ Récupérer le mapping à jour (schema_name -> id_client) ─────────────
    cur.execute("""
        SELECT schema_name, id_client
        FROM BEE_MASTER.PUBLIC.client_databases
        WHERE database_name = 'BEE_CENTRAL'
    """)
    mapping = {row[0]: row[1] for row in cur.fetchall()}

    # ── 7️⃣ Tables à traiter avec mapping logique : source -> target ───────────
    table_mapping = {
        'COMPTES_ANALYTIQUES',
        'COMPTES_BUDGETAIRES',
        'PRODUCTION_BEEONE',
        'PROFIL_DE_PRODUCTION',
        'COUTS_BEEONE',
        'BUDGET',
        'COMPTES_PL',
        'VERSIONS_BUDGET',
        'DIM_PERSONNEL',
        'DIM_OPERATION',
        'DIM_PARCELLE',
        'DIM_FERME',
        'DIM_CAMPAGNE',
        'DIM_CENTRE',
        'FACT_POINTAGE'
    }

    # ── 8️⃣ Boucle principale sur tous les schémas clients ─────────────────────
    for schema_name, id_client in mapping.items():
        print(f"\n🔄 Traitement du schéma : {schema_name} (ID_CLIENT={id_client})")
        for original, renamed in table_mapping.items():
            full_name = f"BEE_CENTRAL.{schema_name}.{renamed}"

            # 8.1 Vérifier si la table existe
            cur.execute("""
                SELECT COUNT(*)
                FROM BEE_CENTRAL.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (schema_name, renamed))
            if cur.fetchone()[0] == 0:
                print(f" • Table introuvable : {full_name} → skip")
                continue

            # 8.2 Vérifier la présence de la colonne ID_CLIENT
            cur.execute("""
                SELECT COUNT(*)
                FROM BEE_CENTRAL.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = 'ID_CLIENT'
            """, (schema_name, renamed))
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE {full_name} ADD COLUMN ID_CLIENT INT")
                print(f" ✅ Colonne ajoutée : {full_name}.ID_CLIENT")
            else:
                print(f" ✔️ Colonne déjà présente : {full_name}.ID_CLIENT")

            # 8.3 Mise à jour des lignes existantes
            cur.execute(f"UPDATE {full_name} SET ID_CLIENT = %s", (id_client,))
            print(f" 🔄 Données mises à jour pour {full_name} → ID_CLIENT={id_client}")

finally:
    cur.close()
    conn.close()
    print("\n✅ Connexion fermée proprement.")
