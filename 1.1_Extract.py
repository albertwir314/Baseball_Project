import os
import requests
import zipfile
from urllib.parse import urlparse
from io import BytesIO

def extract_csvs_from_link(link_location, output_directory):
    """
    Downloads a zip file from the given URL and extracts all CSV files to the output directory.
    
    Args:
        link_location (str): URL pointing to a zip file containing CSV files
        output_directory (str): Path to the directory where CSV files should be extracted
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        
        # Download the file from the URL
        response = requests.get(link_location)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Check if the content is a zip file
        if not zipfile.is_zipfile(BytesIO(response.content)):
            raise ValueError("The downloaded file is not a valid ZIP archive")
        
        # Extract CSV files from the zip
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                raise ValueError("No CSV files found in the ZIP archive")
                
            for csv_file in csv_files:
                # Extract to output directory, preserving the original filename
                zip_file.extract(csv_file, output_directory)
                print(f"Extracted: {csv_file}")
                
        print(f"Successfully extracted {len(csv_files)} CSV files to {output_directory}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid ZIP archive or is corrupted")
    except Exception as e:
        print(f"An error occurred: {e}")


list_years = [2024, 2023, 2022, 2021, 2020]
for i in list_years:
    extract_csvs_from_link(f"https://www.retrosheet.org/downloads/{i}/{i}csvs.zip", f"./output_data/{i}")