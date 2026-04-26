import logging
import time
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script(script_name: str) -> bool:
    logging.info(f"Starting {script_name}")
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=True,
            text=True
        )
        
        elapsed_time = time.time() - start_time
        logging.info(f"Completed {script_name} in {elapsed_time:.2f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        elapsed_time = time.time() - start_time
        logging.error(f"Failed {script_name} after {elapsed_time:.2f} seconds")
        logging.error(f"Error: {e.stderr}")
        return False

def main():
    logging.info("Starting Medallion Pipeline")
    start_time = time.time()
    
    scripts = [
        "01_bronze.py",
        "02_silver.py",
        "03_gold.py"
    ]
    
    results = {}
    
    for script in scripts:
        success = run_script(script)
        results[script] = success
        
        if not success:
            logging.error(f"Failure in {script}, continuing to next step")
    
    total_time = time.time() - start_time
    
    logging.info("Pipeline execution summary:")
    for script, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logging.info(f"  {script}: {status}")
    
    logging.info(f"Total pipeline execution time: {total_time:.2f} seconds")
    
    all_success = all(results.values())
    if all_success:
        logging.info("Pipeline completed successfully")
    else:
        logging.error("Pipeline completed with errors")
        sys.exit(1)

if __name__ == "__main__":
    main()
