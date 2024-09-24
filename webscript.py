
import os
import time
import psycopg2
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

chrome_driver_path = "/usr/local/bin/chromedriver"

DOWNLOAD_DIR = '/Users/vanshjain/Downloads'  
XML_FILE_PATH = '/Users/vanshjain/Downloads/BRSR_1241134_10092024014211_WEB.xml'
CONVERSION_URL = "http://ec2-3-221-41-38.compute-1.amazonaws.com"
EXPECTED_DOWNLOAD_FILENAME = "brsr_1241134_10092024014211_web.xlsx"  
POSTGRESQL_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'root'
}

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")


driver = webdriver.Chrome(service=Service(chrome_driver_path), options=chrome_options)

try:

    print("Navigating to the conversion website...")
    driver.get(CONVERSION_URL)


    print("Waiting for the file upload control...")
    file_input = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, 'FileUploadControl'))
    )


    file_input.send_keys(XML_FILE_PATH)
    print("XML file uploaded successfully.")


    downloaded_file_path = os.path.join(DOWNLOAD_DIR, EXPECTED_DOWNLOAD_FILENAME)
    print(f"Waiting for the file to be downloaded to {downloaded_file_path}...")


    def is_download_complete(file_path):
        if not os.path.exists(file_path):
            return False
        initial_size = os.path.getsize(file_path)
        time.sleep(1)
        new_size = os.path.getsize(file_path)
        return initial_size == new_size


    timeout = 120  
    start_time = time.time()
    while not is_download_complete(downloaded_file_path):
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Download of {EXPECTED_DOWNLOAD_FILENAME} timed out.")
        time.sleep(1)

    print(f"File downloaded successfully: {downloaded_file_path}")


    print("Parsing the downloaded Excel file...")
    df = pd.read_excel(downloaded_file_path)
    print("Excel file parsed successfully.")


    print("Preview of parsed data:")
    print(df.head())


    print("Connecting to PostgreSQL database...")
    conn = psycopg2.connect(
        host=POSTGRESQL_CONFIG['host'],
        database=POSTGRESQL_CONFIG['database'],
        user=POSTGRESQL_CONFIG['user'],
        password=POSTGRESQL_CONFIG['password']
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully.")

# Extract CIN from the first row of the 'Fact Value' column
    cin = df['Fact Value'].iloc[0] if 'Fact Value' in df.columns else None

    if cin is None or pd.isna(cin):
        raise ValueError("CIN value is missing or not found in the first row of the 'Fact Value' column.")

    
    insert_query = """
        INSERT INTO excel_data (cin, sr_no, element_name, period, unit, decimals, fact_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in df.iterrows():
        sr_no = row.get('Sr.No.')  
        element_name = row.get('Element Name')
        period = row.get('Period')
        unit = row.get('Unit')
        decimals = row.get('Decimals')
        fact_value = row.get('Fact Value')

     
        sr_no = int(sr_no) if pd.notna(sr_no) else None
        decimals = decimals if pd.notna(decimals) else 0
        fact_value = fact_value if pd.notna(fact_value) else ''


        cursor.execute(insert_query, (
            cin,
            sr_no,
            element_name,
            period,
            unit,
            decimals,
            fact_value
        ))

    print("Uploading data to PostgreSQL...")
    # Extract CIN from the first row of the 'Fact Value' column
    cin = df['Fact Value'].iloc[0] if 'Fact Value' in df.columns else None

    if cin is None or pd.isna(cin):
        raise ValueError("CIN value is missing or not found in the first row of the 'Fact Value' column.")

    conn.commit()
    print("Data uploaded to PostgreSQL successfully.")

except Exception as e:
    print(f"An error occurred: {e}")


finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    driver.quit()
    print("WebDriver closed.")
    
