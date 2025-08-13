import pandas as pd
import geopandas as gpd
import rasterio
import requests
import zipfile
import io
import os
import shutil
from shapely.geometry import Point


def load_data_from_url(url, zipped=False, filepath=None, **kwargs):
    """
    Extracts and returns raw data from a URL.

    Parameters:
    url : str
        The URL of the file to be extracted.
    zipped : bool, optional
        If True, the file is expected to be inside a zip file. Default is False.
    filepath : str, optional
        If zipped is True, this is the path to the file inside the archive.
    **kwargs :
        Additional arguments passed to the appropriate reader function 
        (e.g., pd.read_csv, pd.read_excel, gpd.read_file, rasterio.open)

    Returns:
    data : DataFrame, GeoDataFrame, or rasterio.DatasetReader
    """
    def detect_file_type(path):
        ext = os.path.splitext(path.lower())[1]
        if ext in ['.csv']: return 'csv'
        if ext in ['.txt']: return 'txt'
        elif ext in ['.xls']: return 'xls'
        elif ext in ['.xlsx']: return 'xlsx'
        elif ext in ['.shp', '.geojson', '.gpkg', '.json', '.kml']: return 'vector'
        elif ext in ['.tif', '.tiff']: return 'raster'
        else: return 'unknown'

    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    if response.status_code != 200:
        response.raise_for_status()

    if not zipped:
        file_type = detect_file_type(url)
        buffer = io.BytesIO(response.content)

        if file_type == 'csv' or file_type == 'txt':
            data = pd.read_csv(buffer, low_memory=False, **kwargs)
        elif file_type == 'xls':
            data = pd.read_excel(buffer, engine='xlrd', **kwargs)
        elif file_type == 'xlsx':
            data = pd.read_excel(buffer, engine='openpyxl', **kwargs)
        elif file_type == 'vector':
            data = gpd.read_file(buffer, **kwargs).to_crs(saws_crs)
        elif file_type == 'raster':
            data = rasterio.open(buffer, **kwargs)
        else: # fallback on csv
            try:
                data = pd.read_csv(buffer, low_memory=False, **kwargs) 
            except:
                raise ValueError(f"Could not download data from this URL. Please check URL and try again.")

    else:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            if filepath is None:
                raise ValueError("Must specify 'filepath' within ZIP archive.")
            file_type = detect_file_type(filepath)

            try:
                with zip_file.open(filepath) as file:
                    if file_type == 'csv':
                        data = pd.read_csv(file, low_memory=False, **kwargs)
                    elif file_type == 'xls':
                        data = pd.read_excel(file, engine='xlrd', **kwargs)
                    elif file_type == 'xlsx':
                        data = pd.read_excel(file, engine='openpyxl', **kwargs)
                    elif file_type in ['vector', 'raster']:
                        raise NotImplementedError("Shapefiles and rasters require extraction.")
            except:
                zip_file.extractall('extracted_data')
                full_path = os.path.abspath(os.path.join('extracted_data', filepath))
                if file_type == 'csv':
                    data = pd.read_csv(full_path, low_memory=False, **kwargs)
                elif file_type == 'xls':
                    data = pd.read_excel(full_path, engine='xlrd', **kwargs)
                elif file_type == 'xlsx':
                    data = pd.read_excel(full_path, engine='openpyxl', **kwargs)
                elif file_type == 'vector':
                    data = gpd.read_file(full_path, **kwargs).to_crs(saws_crs)
                elif file_type == 'raster':
                    data = rasterio.open(full_path, **kwargs)
                shutil.rmtree('extracted_data')

    return data


def lat_long_to_point(df, lat_col, long_col):
    
    """
    This function takes a DataFrame with lat/long columns stored as floats
    and converts it into a GeoDataFrame with a point geometry.

    Arguments:
    df : DataFrame
        The DataFrame to be converted.
    lat_col : str
        The name of the column containing latitude values.
    long_col : str
        The name of the column containing longitude values.

    Returns:
    df : GeoDataFrame
        The modified DataFrame as a GeoDataFrame with point geometry.
    """

    df_geo = [Point(lon, lat) for lon, lat in zip(df[long_col], df[lat_col])]
    df = gpd.GeoDataFrame(df, geometry=df_geo, crs = 'EPSG:4326')
    df = df.to_crs(saws_crs)
    
    return df

data_url = 'https://www.sciencebase.gov/catalog/file/get/58937228e4b0fa1e59b73361?f=__disk__5a%2Fae%2F1a%2F5aae1aa25f84b94737628e43ef82e34f6897a63b'

data=load_data_from_url(data_url, zipped=True, filepath='Major_Ions.csv')
print(data.head())

# Save first 5 rows (with headers) to a CSV file
data.head().to_csv("data_head.csv", index=False)

# Only include samples where model_TDS_mgL is greater than or equal to 1000
filtered_data = data[data['TDS_mgL'] >= 1000]

num_rows_total = len(data)  # total rows before filtering
num_rows_filtered = len(filtered_data)  # rows after filtering
num_rows_excluded = num_rows_total - num_rows_filtered

print("Number of rows in original data:", num_rows_total)
print("Number of rows in filtered data (TDS >= 1000 mg/L):", num_rows_filtered)
print("Number of rows excluded (TDS < 1000 mg/L):", num_rows_excluded)

filtered_data = filtered_data[filtered_data['charge_balance_eq'] < 0.1]

num_rows_filtered2 = len(filtered_data)  # rows after filtering
num_rows_excluded2 = num_rows_filtered - num_rows_filtered2

print("Number of rows in filtered data (charge_balance_eq < 0.1):", num_rows_filtered2)
print("Number of rows excluded (charge_balance_eq >= 0.1):", num_rows_excluded2)