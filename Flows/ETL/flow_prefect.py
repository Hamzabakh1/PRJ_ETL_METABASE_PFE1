import os
import sys

# Ensure project root is on sys.path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Optional import for questionary (for interactive mode)
try:
    import questionary
    QUESTIONARY_AVAILABLE = True
except ImportError:
    QUESTIONARY_AVAILABLE = False
    print("⚠️  questionary non disponible - mode interactif désactivé")

from prefect import flow, task
from Flows.ETL.extract import extract_data
from Flows.ETL.transform import transform_data
from Flows.ETL.load import load_data
from Tables.Queries.queries import QUERIES

# Table mapping based on the SQL schema structure
TABLE_MAPPING = {
    'COMPTES_ANALYTIQUES': 'stg_comptes_analytiques',
    'COMPTES_BUDGETAIRES': 'stg_comptes_budgetaires', 
    'PRODUCTION_BEEONE': 'stg_production_beeone',
    'PROFIL_DE_PRODUCTION': 'stg_profil_production',
    'COUTS_BEEONE': 'stg_couts_beeone',
    'BUDGET': 'stg_budget',
    'COMPTES_PL': 'stg_comptes_pl',
    'VERSIONS_BUDGET': 'stg_versions_budget',
    'DIM_PERSONNEL': 'dim_personnel',
    'DIM_OPERATION': 'dim_operation', 
    'DIM_PARCELLE': 'dim_parcelle',
    'DIM_FERME': 'dim_ferme',
    'DIM_CAMPAGNE': 'dim_campagne',
    'DIM_CENTRE': 'dim_centre',
    'FACT_POINTAGE': 'fact_pointage'
}

@task
def extract_task(client: str):
    """
    Extract raw data for a given client in full mode (dates ignored).
    """
    return extract_data(client, QUERIES)

@task
def transform_task(raw: dict):
    """
    Clean and transform raw data into DataFrames keyed by source name.
    """
    return transform_data(raw)

@task
def load_task(df, client: str):
    """
    Load a DataFrame into Snowflake (full mode only).
    """
    load_data(df, client, mode='full')

@flow(name="ETL Flow")
def etl_flow(client: str, mode: str = "full"):
    """
    ETL orchestration flow: extract, transform, and load in full mode.
    Each query automatically uses its own table name (same as query name).
    """
    raw_data = extract_task(client)
    clean_data = transform_task(raw_data)
    
    successful_loads = 0
    failed_loads = 0
    
    for source_name, df in clean_data.items():
        # Proper table mapping based on the SQL schema
        target_table = TABLE_MAPPING.get(source_name, source_name.lower())  # Default to lowercase if not found
        
        df.attrs['table'] = target_table
        print(f"🚀 Loading {source_name} -> {target_table} in full mode...")
        
        try:
            load_task(df, client)
            successful_loads += 1
        except Exception as e:
            print(f"❌ Failed to load {source_name} -> {target_table}: {str(e)}")
            failed_loads += 1
            # Continue with next table instead of stopping
            continue
    
    # Summary
    print(f"\n📈 ETL SUMMARY for {client}:")
    print(f"   ✅ Successful loads: {successful_loads}")
    print(f"   ❌ Failed loads: {failed_loads}")
    print(f"   📊 Total tables processed: {successful_loads + failed_loads}")
    print(f"   📋 Each query loaded into its own table using query name")
    
    return {"successful": successful_loads, "failed": failed_loads}

if __name__ == "__main__":
    # Set PROJECT_ROOT environment variable
    os.environ['PROJECT_ROOT'] = project_root
    
    # List clients from Clients directory
    base_dir = os.path.join(project_root, 'Clients')
    clients = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    print("Available clients:", clients)

    if QUESTIONARY_AVAILABLE:
        # Interactive mode with questionary
        sel = questionary.text("Choose client (or 'all')").ask()
        selected = clients if sel.lower() == 'all' else [sel]
    else:
        # Automatic mode - use first client or Client1
        if "Client1" in clients:
            selected = ["Client1"]
        else:
            selected = clients[:1] if clients else []
        print(f"Mode automatique - utilisation de: {selected}")

    mode = 'full'
    print("Mode set to full. Update mode is disabled.")

    # Execute for each client
    for client in selected:
        print(f"\n🚧 Running ETL for: {client} (full mode)")
        try:
            result = etl_flow(client)
            print(f"✅ Flow completed for {client}: {result}")
        except Exception as e:
            print(f"❌ Flow failed for {client}: {e}")
