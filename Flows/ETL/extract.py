import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def extract_data(client: str, queries: dict, start_date: str = None, end_date: str = None) -> dict:
    project_root = os.environ.get('PROJECT_ROOT')
    if not project_root:
        raise EnvironmentError("PROJECT_ROOT environment variable not set")
    
    config_path = os.path.join(project_root, 'Clients', client, 'config.yml')
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    db = cfg['source_db']
    driver = db['driver'].strip('{}')
    trusted = str(db.get('trusted_connection', False)).lower() in ['yes', 'true', '1']

    if trusted:
        conn_str = f"mssql+pyodbc://@{db['server']}/{db['database']}?driver={quote_plus(driver)}&trusted_connection=yes"
    else:
        conn_str = f"mssql+pyodbc://{db['username']}:{db['password']}@{db['server']}/{db['database']}?driver={quote_plus(driver)}"

    engine = create_engine(conn_str)
    data = {}
    for name, sql in queries.items():
        query = sql
        if start_date and end_date:
            query = sql.replace('{start_date}', start_date).replace('{end_date}', end_date)
        logger.info(f"📤 Extracting: {name}")
        data[name] = pd.read_sql(query, engine)
    engine.dispose()
    return data
