import geopandas as gpd
from simpledbf import Dbf5
gdf = gpd.read_file('data/raw/massgis/barnstable/M020Assess_CY25_FY25.dbf')
print(gdf.columns.tolist())
print(gdf.dtypes)
print(gdf.head(2))

dbf = Dbf5('data/raw/massgis/Barnstable/M020UC_LUT_CY25_FY25.dbf')
lut = dbf.to_dataframe()
print(lut)

residential = lut[lut['USE_DESC'].str.contains(
    'Residential|Condo|Apartment|Multi|Single|Two|Three|Mobile|Vacation|Seasonal', 
    case=False, na=False
)]
print(residential.to_string())