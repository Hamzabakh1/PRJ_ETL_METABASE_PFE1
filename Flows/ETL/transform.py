import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def transform_data(raw: dict) -> dict:
    cleaned = {}
    for name, df in raw.items():
        logger.info(f"🧹 Transforming: {name}")
        df = df.copy()
        df = df.drop_duplicates()
        df.columns = [c.strip().upper() for c in df.columns]
        cleaned[name] = df
    return cleaned
