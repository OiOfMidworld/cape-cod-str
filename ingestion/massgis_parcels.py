import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from simpledbf import Dbf5

logger = logging.getLogger(__name__)

DATA_DIR = Path('data/raw/massgis')

RESIDENTIAL_USE_CODES = {
    '101', '1010', '0101', '101V',
    '102', '1020', '102V',
    '103', '1030',
    '104', '1040',
    '105', '1050', '0105',
    '109', '1090', '0109',
    '111', '1110',
    '112', '1120', '112C',
    '013', '0130', '013V',
}
# finds asses dbf files and reads w/ Dbf5 - returning a dataframe
def load_ass(town_dir):
    assess_file = next(town_dir.glob('*Assess*.dbf'))
    dbf = Dbf5(assess_file)
    df = dbf.to_dataframe()
    return df

# finds the UC_LUT dbf file and returns a dataframe with use_code and use_desc
def load_use_lut(town_dir): 
    lut_file = next(town_dir.glob('*UC_LUT*.dbf'))
    dbf = Dbf5(lut_file)
    df = dbf.to_dataframe()
    return df[['USE_CODE', 'USE_DESC']].drop_duplicates(subset=['USE_CODE'])

def run(town_dir):
    from transform.town_normalizer import normalize_series
    from load.loader import load_dataframe, upsert_dataframe

    town_dir = Path(town_dir)
    logger.info(f"Loading parcels from {town_dir.name}...")

    # load and merge
    df = load_ass(town_dir)
    lut = load_use_lut(town_dir)
    df = df.merge(lut, on='USE_CODE', how='left')

    # lowercase all column names
    df.columns = [c.lower() for c in df.columns]

    #dedup on loc_id
    df = df.drop_duplicates(subset=['loc_id'], keep='first')
    logger.info(f"After dedup: {len(df)} parcels")
    df = df.dropna(subset=['loc_id'])
    logger.info(f"after dropping null loc_id: {len(df)} parcels")

    # add flags and metadata
    df['is_residential'] = df['use_code'].isin(RESIDENTIAL_USE_CODES)
    df['ingested_at'] = datetime.now()


    # normalize city to canonical town name
    df['town'] = normalize_series(df['city'])



    # drop columns we don't need
    drop_cols = ['ls_date', 'ls_price', 'ls_book', 'ls_page', 'reg_id', 'cama_id', 'own_co', 'location']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    logger.info(f"Loaded {len(df)} parcels, {df['is_residential'].sum()} residential")

    # load raw
    raw_df = df.drop(columns=['use_desc', 'is_residential'])
    upsert_dataframe(raw_df, 'raw.massgis_parcels', primary_keys=['loc_id'])

    # staging - residential only
    staging_df = df[df['is_residential']].copy()
    upsert_dataframe(staging_df, 'staging.stg_massgis_parcels', primary_keys=['loc_id'])

    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    for town_dir in sorted(DATA_DIR.iterdir()):
        if town_dir.is_dir():
            run(town_dir)
