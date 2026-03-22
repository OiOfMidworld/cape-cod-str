import logging
import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import re

logger = logging.getLogger(__name__)

STREET_ABBREVS = {
    r'\bRD\b': 'ROAD',
    r'\bAVE\b': 'AVENUE',
    r'\bDR\b': 'DRIVE',
    r'\bLN\b': 'LANE',
    r'\bST\b': 'STREET',
    r'\bCIR\b': 'CIRCLE',
    r'\bBLVD\b': 'BOULEVARD',
    r'\bCT\b': 'COURT',
    r'\bPL\b': 'PLACE',
    r'\bTER\b': 'TERRACE',
    r'\bHWY\b': 'HIGHWAY',
    r'\bPKWY\b': 'PARKWAY',
    r'\bEXT\b': 'EXTENSION',
    r'\bXING\b': 'CROSSING',
}



def normalize_street(name):
    if pd.isna(name) or name is None:
        return None
    
    name = str(name).upper().strip()
    name = ' '.join(name.split())  # collapse multiple spaces
    name = re.sub(r'[^\w\s]', '', name)  # remove punctuation
    
    for pattern, replacement in STREET_ABBREVS.items():
        name = re.sub(pattern, replacement, name)
    
    return name

def load_data(engine):
    str_df = pd.read_sql("""
        SELECT certificate_id, street_name, town, zip_code 
        FROM staging.stg_str_registry
    """, engine)
    
    par_df = pd.read_sql("""
        SELECT loc_id, full_str, town 
        FROM staging.stg_massgis_parcels
    """, engine)
    
    str_df['street_norm'] = str_df['street_name'].apply(normalize_street)
    par_df['street_norm'] = par_df['full_str'].apply(normalize_street)
    
    return str_df, par_df


def exact_match(str_df, par_df):
    merged = str_df.merge(
        par_df,
        on=['street_norm', 'town'],
        how='inner'
    )
    merged['match_type'] = 'exact'
    merged['match_score'] = 100
    return merged[['certificate_id', 'loc_id', 'town', 'match_type', 'match_score']]


def fuzzy_match(unmatched_str, par_df, threshold=90):
    results = []
    
    for town, town_str in unmatched_str.groupby('town'):
        town_par = par_df[par_df['town'] == town].copy()
        if town_par.empty:
            continue
        
        parcel_streets = town_par['street_norm'].dropna().unique().tolist()
        
        for _, row in town_str.iterrows():
            if pd.isna(row['street_norm']):
                continue
            
            match = process.extractOne(
                row['street_norm'],
                parcel_streets,
                scorer=fuzz.ratio,
                score_cutoff=threshold
            )
            
            if match:
                matched_street, score, _ = match
                matched_parcels = town_par[town_par['street_norm'] == matched_street]
                for _, parcel in matched_parcels.iterrows():
                    results.append({
                        'certificate_id': row['certificate_id'],
                        'loc_id': parcel['loc_id'],
                        'town': town,
                        'match_type': 'fuzzy',
                        'match_score': score
                    })
    
    return pd.DataFrame(results)


def run():
    from load.loader import get_engine
    
    load_dotenv()
    engine = get_engine()
    
    logger.info("Loading data...")
    str_df, par_df = load_data(engine)
    logger.info(f"Loaded {len(str_df)} STR certificates and {len(par_df)} parcels")
    
    # exact matching
    logger.info("Running exact match...")
    exact_results = exact_match(str_df, par_df)
    logger.info(f"Exact matches: {len(exact_results)}")
    
    # find unmatched STR certificates
    matched_certs = exact_results['certificate_id'].unique()
    unmatched_str = str_df[~str_df['certificate_id'].isin(matched_certs)]
    logger.info(f"Unmatched after exact: {len(unmatched_str)} certificates")
    
    # fuzzy matching on unmatched
    logger.info("Running fuzzy match...")
    fuzzy_results = fuzzy_match(unmatched_str, par_df)
    logger.info(f"Fuzzy matches: {len(fuzzy_results)}")
    

    
    # combine results
    all_matches = pd.concat([exact_results, fuzzy_results], ignore_index=True)
    
    # add unmatched certificates
    all_matched_certs = all_matches['certificate_id'].unique()
    still_unmatched = str_df[~str_df['certificate_id'].isin(all_matched_certs)]
    if len(still_unmatched) > 0:
        unmatched_df = pd.DataFrame({
            'certificate_id': still_unmatched['certificate_id'],
            'loc_id': None,
            'town': still_unmatched['town'],
            'match_type': 'unmatched',
            'match_score': None
        })
        unmatched_df = unmatched_df.dropna(axis=1, how='all')
        all_matches = pd.concat([all_matches, unmatched_df], ignore_index=True)
    
# only load matched records
    from load.loader import upsert_dataframe
    matched_only = all_matches[all_matches['match_type'] != 'unmatched'].copy()

    logger.info(f"Loading {len(matched_only)} matched records...")
    matched_only['snapshot_date'] = pd.Timestamp.now().date()
    matched_only = matched_only.drop_duplicates(subset=['certificate_id', 'loc_id', 'snapshot_date'])
    logger.info(f"After dedup: {len(matched_only)} match records")
    upsert_dataframe(
        matched_only,
        'staging.stg_str_parcel_match',
        primary_keys=['certificate_id', 'loc_id', 'snapshot_date']
)
    
    return all_matches


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    results = run()
    print(f"\nMatch summary:")
    print(results['match_type'].value_counts())