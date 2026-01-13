import os
import subprocess
import glob
from astropy.io import fits
from datetime import datetime
import logging

# Input/Output paths
user = os.environ.get("USER")
user_base_path = f'/lustre/t0/scratch/users/{user}/hipsgen_cat/'

current_date = datetime.now().strftime('%Y-%m-%d_%H-%M')
run_path = os.path.join(user_base_path, f'run_hipsgen_{current_date}')
os.makedirs(run_path, exist_ok=True)

output_dir = os.path.join(run_path, f'output')
os.makedirs(output_dir, exist_ok=True)

logs_dir = os.path.join(run_path, f'logs')
os.makedirs(logs_dir, exist_ok=True)

files_logs_dir = os.path.join(logs_dir, f'files_logs')
os.makedirs(files_logs_dir, exist_ok=True)


# Logger configuration
log_file = os.path.join(logs_dir, "process_log.txt")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# List of .fits files
original_catalog_path = f'/lustre/t0/scratch/users/{user}/hipsgen_cat/filter_and_concatenate/run_filter_and_concatenate_<your-date-and-time>/output'
original_catalog_files = '*.fits'
total_files = [f for f in glob.glob(os.path.join(original_catalog_path, original_catalog_files))]

# Path to HiPSGen-cat
hipscat_jar = f"/lustre/t0/scratch/users/{user}/hipsgen_cat/Hipsgen-cat.jar"
java_path = f"/lustre/t0/scratch/users/{user}/java_env/bin/java"

# Total memory configuration and optimized parameters
memory_total_gb = 240  # Total memory in GB
xmx = 200  # Maximum heap (200GB)
xms = 100  # Initial heap (100GB)
max_heap_free_ratio = 5  # Reduce free heap ratio
min_heap_free_ratio = 2
parallel_gc_threads = 100  # Number of threads for GC
active_processor_count = 100  # Number of available cores

# Function to process a file
def process_csv(file):
    base_name = os.path.basename(file).replace('.fits', '')
    file_output_dir = os.path.join(output_dir, base_name)
    os.makedirs(file_output_dir, exist_ok=True)

    log_file = os.path.join(files_logs_dir, f"{base_name}_log.txt")
    error_file = os.path.join(files_logs_dir, f"{base_name}_error.txt")

    # Updated command
    command = (
        f"{java_path} "
        f"-Xms{xms}g -Xmx{xmx}g "
        f"-XX:+UseG1GC "
        f"-XX:MaxHeapFreeRatio={max_heap_free_ratio} "
        f"-XX:MinHeapFreeRatio={min_heap_free_ratio} "
        f"-XX:MaxDirectMemorySize={xmx}g "
        f"-XX:ParallelGCThreads={parallel_gc_threads} "
        f"-XX:ActiveProcessorCount={active_processor_count} "
        f"-Djava.util.concurrent.ForkJoinPool.common.parallelism={parallel_gc_threads-1} "
        f"-jar {hipscat_jar} "
        f"-cat DES_DR2_sample "
        f"-in {file} "
        f"-f FITS "
        f"-ra RA "
        f"-dec DEC "
        f"-out {file_output_dir} "
        f"-score MAG_AUTO_I_DERED "
    )

    result = subprocess.run(command, shell=True, capture_output=True, text=True, executable="/bin/bash")

    with open(log_file, 'w') as log:
        log.write(f"Command: {command}\n")
        log.write(f"Stdout:\n{result.stdout.strip()}\n")

    with open(error_file, 'w') as error:
        error.write(f"Stderr:\n{result.stderr.strip()}\n")

    if result.returncode != 0:
        message = f"Error processing {file}. See {error_file} for more details."
        logging.error(message)
    else:
        message = f"Successfully processed: {file}. See {log_file} for more details."
        logging.info(message)

# Process files sequentially
for file in total_files:
    logging.info(f"Starting processing of file: {file}")
    process_csv(file)
    logging.info(f"Finished processing of file: {file}")
    
### Acknowledgements

#This work uses the Hipsgen-cat.jar tool, developed and maintained by the Centre de Données astronomiques de Strasbourg (CDS). For more information, visit: https://aladin.cds.unistra.fr/hips/HipsCat.gml.

#This work used computational resources from the Associação Laboratório Interinstitucional de e-Astronomia (LIneA) with the financial support of INCT do e-Universo (Process no. 465376/2014-2).