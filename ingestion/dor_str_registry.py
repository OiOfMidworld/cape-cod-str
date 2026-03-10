## imports
import requests
import time
import logging
from datetime import date
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

## create session
def get_session_and_token():
    session = requests.Session()
    #get page
    response = session.get('https://licensing.reg.state.ma.us/StrRegistry/')
    html = response.text
    #parse the token from response html
    # token is <input name="__RequestVerificationToken"
    soup = BeautifulSoup(html, 'lxml')
    token = soup.find('input', {'name': '__RequestVerificationToken'})['value']
    # return both
    return session, token

## post to search by town and letter
def search_by_town_and_letter(session, token, town, letter):
    params = {
        '__RequestVerificationToken' : token,
        'SearchRecord.CertificateNumber' : '',
        'SearchRecord.PropertyType' : 'STR',
        'SearchRecord.RawCity' : town,
        'SearchRecord.NormalizedStreet' : letter,
        'SearchRecord.ZipCode' : '',
        'id' : '',
        'SortField' : 'City',
        'SortOrder' : 'Asc'
    }
    url = 'https://licensing.reg.state.ma.us/StrRegistry/'
    response = session.post(url, data=params)
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'class': 'ma__table'})
    table_data = []
    headers = [th.text.strip() for th in table.find_all('th')]
    table_data.append(headers)
    if 'Search request returned no results' in response.text:
        return []
    
    rows = []
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) == 5:  # skip header row and the hidden input row at the bottom
            rows.append({
            'certificate_id': cells[0].text.strip(),
            'property_type': cells[1].text.strip(),
            'street_name': cells[2].text.strip(),
            'city_raw': cells[3].text.strip(),
            'zip_code': cells[4].text.strip(),
        })
    return rows

## run - loop 15 towns and a-z , collect results, dedupe on cert id, add snapshot date, ingested at

def run():
    from ingestion.census_api import BARNSTABLE_TOWNS
    from transform.town_normalizer import normalize_series
    from load.loader import load_dataframe, upsert_dataframe
    import string

    logger.info("Starting DOR STR Registry scrape...")
    session, token = get_session_and_token()

    all_results = []

    for town in BARNSTABLE_TOWNS.keys():
        logger.info(f"Scraping {town}...")
        for letter in string.ascii_uppercase:
            rows = search_by_town_and_letter(session, token, town, letter)
            all_results.extend(rows)
            time.sleep(0.5)

    if not all_results:
        logger.warning("No results scraped")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)

    # Deduplicate on certificate_id
    df = df.drop_duplicates(subset=['certificate_id'])

    # Add metadata
    df['snapshot_date'] = date.today()
    df['ingested_at'] = pd.Timestamp.now()

    # Normalize city_raw to canonical town name
    df['town'] = normalize_series(df['city_raw'])

    logger.info(f"Scraped {len(df)} unique STR registrations")

    # Load raw
    load_dataframe(df, 'raw.str_registry', mode='append')

    # Upsert staging
    staging_cols = [c for c in df.columns if c not in ['ingested_at', 'city_raw']]
    staging_df = df[staging_cols].copy()
    upsert_dataframe(staging_df, 'staging.stg_str_registry', primary_keys=['certificate_id', 'snapshot_date'])

    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    result = run()
    if not result.empty:
        print(f"\nLoaded {len(result)} STR registrations")
        print(result[['certificate_id', 'street_name', 'town', 'zip_code']].head(20).to_string(index=False))