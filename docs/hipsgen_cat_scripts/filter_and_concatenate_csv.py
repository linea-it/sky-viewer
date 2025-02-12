import logging
from astropy.io import fits
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
os.makedirs(log_dir, exist_ok=True)  # Create log directory if it doesn't exist

# Configure output
output_dir = os.path.join(run_path, f'output')
os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist

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
output_csv = os.path.join(output_dir, f'des_dr2_filtered.csv')

# List of .fits files in the directory
fits_files = glob(os.path.join(input_dir, "DES2359*.fits"))
total_files = len(fits_files)  # Total number of files

if total_files == 0:
    logging.error("No .fits files found in the specified directory.")
    exit()

logging.info(f"Total files found: {total_files}")

# List of desired columns
columns_to_select = [
    "COADD_OBJECT_ID", "RA", "DEC", "FLAGS_I", "EXTENDED_CLASS_COADD",  
    "MAG_AUTO_G_DERED", "MAG_AUTO_R_DERED", "MAG_AUTO_I_DERED", "MAG_AUTO_Z_DERED", "MAG_AUTO_Y_DERED",
    "MAGERR_AUTO_G", "MAGERR_AUTO_R", "MAGERR_AUTO_I", "MAGERR_AUTO_Z", "MAGERR_AUTO_Y"
]

# Control variable to write header to the CSV only the first time
write_header = True

# Process files and write directly to CSV
for i, file in enumerate(fits_files, start=1):
    try:
        with fits.open(file, memmap=True) as hdul:  # `memmap=True` to reduce memory usage
            # Read data from extension 1
            data = hdul[1].data

            # Convert to DataFrame and select columns
            df = pd.DataFrame(data)[columns_to_select]

            # Write to CSV without keeping data in memory
            df.to_csv(output_csv, mode='a', index=False, header=write_header)

            # Write the header only the first time
            if write_header:
                write_header = False

        # Log progress every 10 files processed
        if i % 10 == 0 or i == total_files:
            logging.info(f"Progress: {i}/{total_files} files processed")

    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")

# Log the end of processing
end_time = datetime.now()
logging.info(f"Processing end time: {end_time}")

# Calculate and log total processing duration
duration = end_time - start_time
logging.info(f"Total processing time: {duration}")

logging.info("Processing finished.")