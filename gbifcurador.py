import pandas as pd
import requests
from pygbif import occurrences, species
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def fetch_all_gbif_data(species_name, limit_per_request=5000):
    """
    Fetch all occurrence data from GBIF for a given species using pagination.
    """
    all_records = []
    offset = 0
    
    while True:
        response = occurrences.search(scientificName=species_name, limit=limit_per_request, offset=offset)
        records = response['results']
        
        if not records:
            break
        
        all_records.extend(records)
        offset += limit_per_request
    
    gbif_url = f"https://www.gbif.org/occurrence/search?scientificName={species_name.replace(' ', '%20')}"
    return pd.DataFrame(all_records), gbif_url

def initial_cleaning(df):
    """
    Initial data cleaning: select relevant columns and drop duplicates.
    """
    columns = ['species', 'decimalLatitude', 'decimalLongitude', 'country', 'eventDate', 'basisOfRecord', 'institutionCode', 'identificationID', 'identifiedBy']
    df = df[columns].copy()
    df.drop_duplicates(inplace=True)
    return df

def validate_data(df, min_date, max_date):
    """
    Validate data: check coordinates, dates, and species names.
    """
    initial_count = len(df)
    
    # Validate coordinates: ensure they are within valid ranges and have between 3 and 8 decimal places
    def valid_decimals(value):
        try:
            decimal_places = str(value).split('.')[1]
            return 3 <= len(decimal_places) <= 8
        except IndexError:
            return False
    
    df = df[(df['decimalLatitude'].between(-90, 90)) & 
            (df['decimalLongitude'].between(-180, 180)) & 
            df['decimalLatitude'].apply(valid_decimals) & 
            df['decimalLongitude'].apply(valid_decimals)]
    
    df.loc[:, 'decimalLatitude'] = df['decimalLatitude'].round(8)
    df.loc[:, 'decimalLongitude'] = df['decimalLongitude'].round(8)
    coord_valid_count = len(df)
    
    # Validate eventDate: convert to datetime, ensure dates are within the range, and drop invalid dates
    def is_valid_date(date):
        try:
            date = pd.to_datetime(date, errors='coerce')
            if date and min_date <= date <= max_date:
                return date
        except Exception:
            pass
        return pd.NaT
    
    df.loc[:, 'eventDate'] = df['eventDate'].apply(is_valid_date)
    df = df[df['eventDate'].notnull()]
    date_valid_count = len(df)
    
    return df, initial_count, coord_valid_count, date_valid_count

def validate_taxonomic_name(name):
    """
    Validate species name using GBIF taxonomic backbone.
    """
    match = species.name_backbone(name=name)
    if match['matchType'] != 'NONE':
        return match['usageKey']
    return None

def taxonomic_validation(df):
    """
    Validate species names using the GBIF taxonomic backbone.
    """
    valid_species = []
    for name in df['species'].unique():
        if validate_taxonomic_name(name):
            valid_species.append(name)
    
    return df[df['species'].isin(valid_species)], len(valid_species)

def validate_georeferencing(df):
    """
    Validate georeferencing using GeoPy to ensure coordinates are in plausible locations.
    """
    geolocator = Nominatim(user_agent="geo_validation")
    valid_geolocation = []

    def geocode_point(lat, lon):
        try:
            location = geolocator.reverse((lat, lon), timeout=10)
            if location:
                return True
        except GeocoderTimedOut:
            return False
        return False

    for _, row in df.iterrows():
        if geocode_point(row['decimalLatitude'], row['decimalLongitude']):
            valid_geolocation.append(row)

    return pd.DataFrame(valid_geolocation)

def enrich_data(df):
    """
    Enrich data with additional metadata if available.
    """
    df['dataSource'] = 'GBIF'
    return df

def curate_data(species_name, min_date, max_date, limit_per_request=5000):
    """
    Complete data curation process.
    """
    # Fetch raw data from GBIF using pagination
    raw_data, gbif_url = fetch_all_gbif_data(species_name, limit_per_request)
    
    # Save uncurated data
    raw_data.to_csv(f'{species_name.replace(" ", "_")}_gbif_raw_data.csv', index=False)
    
    # Initial data cleaning
    cleaned_data = initial_cleaning(raw_data)
    
    # Validate data
    validated_data, initial_count, coord_valid_count, date_valid_count = validate_data(cleaned_data, min_date, max_date)
    
    # Perform georeferencing validation
    validated_georeferencing_data = validate_georeferencing(validated_data)
    
    # Perform taxonomic validation
    taxonomically_validated_data, valid_species_count = taxonomic_validation(validated_georeferencing_data)
    
    # Enrich data with additional metadata
    enriched_data = enrich_data(taxonomically_validated_data)
    
    # Save curated data
    enriched_data.to_csv(f'{species_name.replace(" ", "_")}_gbif_curated_data.csv', index=False)
    
    # Create report
    report = (
        f"Species: {species_name}\n"
        f"GBIF Data URL: {gbif_url}\n"
        f"Total records fetched: {initial_count}\n"
        f"Records after coordinate validation: {coord_valid_count}\n"
        f"Records after date validation: {date_valid_count}\n"
        f"Records after georeferencing validation: {len(validated_georeferencing_data)}\n"
        f"Valid species names found: {valid_species_count}\n"
        f"Total curated records: {len(enriched_data)}\n"
    )
    
    with open(f'{species_name.replace(" ", "_")}_gbif_report.txt', 'w') as report_file:
        report_file.write(report)
    
    return enriched_data

def main():
    species_name = input("Enter the species name: ")  # Prompt for species name
    max_year = int(input("Enter the maximum year for event dates: "))  # Prompt for max year
    min_year = int(input("Enter the minimum year for event dates: "))  # Prompt for min year
    
    # Convert years to datetime objects
    min_date = datetime(min_year, 1, 1)
    max_date = datetime(max_year, 12, 31)
    
    # Curate data
    curated_data = curate_data(species_name, min_date, max_date)
    
    print(f"Curated data for {species_name} has been saved.")

if __name__ == "__main__":
    main()

