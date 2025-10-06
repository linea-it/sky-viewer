import logging
from astropy.io import fits
from astropy.table import Table
import pandas as pd
import os
from glob import glob
from datetime import datetime

# Input/Output paths
user = os.environ.get("USER")
user_base_path = f'/lustre/t0/scratch/users/{user}/hipsgen_cat/filter_and_concatenate'

current_date = datetime.now().strftime('%Y-%m-%d_%H-%M')
run_path = os.path.join(user_base_path, f'run_filter_and_concatenate_{current_date}')
os.makedirs(run_path, exist_ok=True)

# Configure logging
log_dir = os.path.join(run_path, f'logs')
os.makedirs(log_dir, exist_ok=True)  # Create the log directory if it doesn't exist

# Configure output
output_dir = os.path.join(run_path, f'output')
os.makedirs(output_dir, exist_ok=True)  # Create the output directory if it doesn't exist

# Log file name with current date and time
log_file = os.path.join(log_dir, f"logs-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Log the start of processing
start_time = datetime.now()
logging.info(f"Processing start time: {start_time}")

# Directory containing the .fits files
input_dir = "/lustre/t1/public/des/dr2/primary/catalogs/coadd"
output_file = os.path.join(output_dir, f'des_dr2_filtered.fits')

# List of .fits files in the directory
fits_files = glob(os.path.join(input_dir, "DES2359*.fits"))
total_files = len(fits_files)  # Total number of files

if total_files == 0:
    logging.error("No .fits files found in the specified directory.")
    exit()

logging.info(f"Total files found: {total_files}")

# List of desired columns in uppercase
columns_to_select = [
    "COADD_OBJECT_ID", "RA", "DEC", "MAG_AUTO_I_DERED"
]

# Main DataFrame for concatenation
df_concat = pd.DataFrame()

# Process each .fits file individually
for i, file in enumerate(fits_files, start=1):  # start=1 to begin the index at 1
    try:
        with fits.open(file) as hdul:
            # Read data from extension 1
            data = hdul[1].data

            # Convert data to a DataFrame
            df = pd.DataFrame(data)

            # Select the desired columns
            df_filtered = df[columns_to_select]

            # Concatenate data into the main DataFrame
            df_concat = pd.concat([df_concat, df_filtered], ignore_index=True)

        # Log progress
        if i % 50 == 0 or i == total_files:  # Update every 50 files or at the end
            logging.info(f"Progress: {i}/{total_files} files processed")

    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")
        
# Save the final DataFrame as a FITS file
try:
    table = Table.from_pandas(df_concat)
    table.write(output_file, format="fits", overwrite=True)
    logging.info(f"Processing completed! Concatenated file saved at: {output_file}")
except Exception as e:
    logging.error(f"Error saving the concatenated file: {e}")

# Log the end of processing
end_time = datetime.now()
logging.info(f"Processing end time: {end_time}")

# Calculate and log total processing duration
duration = end_time - start_time
logging.info(f"Total processing time: {duration}")

logging.info("Processing finished.")