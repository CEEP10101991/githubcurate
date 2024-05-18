import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def load_data(csv_file, shp_file):
    """
    Load the curated data from CSV and the shapefile.
    """
    df = pd.read_csv(csv_file)
    shp = gpd.read_file(shp_file)
    return df, shp

def validate_points_within_shapefile(df, shp):
    """
    Validate that all points in the DataFrame fall within the shapefile geometries.
    """
    # Convert the DataFrame to a GeoDataFrame
    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['decimalLongitude'], df['decimalLatitude']))
    
    # Ensure both GeoDataFrames have the same CRS
    if gdf_points.crs != shp.crs:
        gdf_points = gdf_points.set_crs(shp.crs, allow_override=True)
    
    # Check if points fall within the shapefile geometries
    gdf_points['within_shapefile'] = gdf_points.apply(lambda row: any(shp.contains(row.geometry)), axis=1)
    
    return gdf_points

def main():
    csv_file = input("Enter the path to the curated CSV file: ")  # Prompt for the CSV file path
    shp_file = input("Enter the path to the shapefile (SHP): ")  # Prompt for the shapefile path
    
    # Load the data
    df, shp = load_data(csv_file, shp_file)
    
    # Validate points within the shapefile
    validated_gdf = validate_points_within_shapefile(df, shp)
    
    # Save the results
    validated_gdf.to_csv('validated_points_within_shapefile.csv', index=False)
    
    print(f"Validation complete. Points within shapefile: {validated_gdf['within_shapefile'].sum()}. Points not within shapefile: {(~validated_gdf['within_shapefile']).sum()}.")
    print("Results saved to 'validated_points_within_shapefile.csv'.")

if __name__ == "__main__":
    main()

