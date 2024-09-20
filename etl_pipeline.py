# import necessary libraries
import pandas as pd
import os 
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv


# Extraction of raw data files
agency_df = pd.read_csv(r'raw_files\AgencyMaster.csv')
employemnt_df = pd.read_csv(r'raw_files\EmpMaster.csv')
payroll_2020_df = pd.read_csv(r'raw_files\nycpayroll_2020.csv')
payroll_2021_df = pd.read_csv(r'raw_files\nycpayroll_2021.csv')
title_df = pd.read_csv(r'raw_files\TitleMaster.csv')

# cleaning the data
title_df.fillna({
    'TitleDescription' : 'unknown'
}, inplace=True)

# creating the tables
agency_dim = agency_df[['AgencyID', 'AgencyName']].copy().drop_duplicates().reset_index(drop=True)

# employee table
employee_dim = employemnt_df[['EmployeeID', 'LastName', 'FirstName']].copy().drop_duplicates().reset_index(drop=True)

# title table
title_dim = title_df[['TitleCode', 'TitleDescription']].copy().drop_duplicates().reset_index(drop=True)

# Concatenate the payroll data for 2020 and 2021
payroll_combined = pd.concat([payroll_2020_df, payroll_2021_df], ignore_index=True)

# filled payroll_combined with missing values
payroll_combined.fillna({
    'AgencyID' : 0,
    'AgencyCode': 0
}, inplace=True)

# payroll_facts_table
payroll_facts_table = payroll_combined.merge(employee_dim, on= ['EmployeeID', 'LastName', 'FirstName'], how = 'left') \
                                     .merge(title_dim, on = ['TitleCode', 'TitleDescription'], how = 'left') \
                                     .merge(agency_dim, on = ['AgencyID', 'AgencyName'], how = 'left') \
                                     [['PayrollNumber', 'EmployeeID', 'AgencyID','TitleCode', 'FiscalYear', 'AgencyStartDate', 'WorkLocationBorough', 'AgencyCode', \
                                        'LeaveStatusasofJune30', 'BaseSalary','PayBasis', 'RegularHours', 'RegularGrossPaid', 'OTHours', 'TotalOTPaid', 'TotalOtherPay' ]]

# loading files into csv
employee_dim.to_csv(r'dataset\employee_dim.csv', index=False)
title_dim.to_csv(r'dataset\title_dim.csv', index=False)
agency_dim.to_csv(r'dataset\agency_dim.csv', index=False)
payroll_facts_table.to_csv(r'dataset\payroll_facts_table.csv', index=False)

print('files have been loaded temporarily into local machine')

# setting the Azure blob connection and load data
load_dotenv()

connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


# loading data into Azure blob storage as a parquet file
def upload_df_to_blob_as_parquet(df, container_client,blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to Blob storage successfully')
    
upload_df_to_blob_as_parquet(agency_dim, container_client, 'rawdata/agency_dim.parquet')
upload_df_to_blob_as_parquet(employee_dim, container_client, 'rawdata/employee_dim.parquet')
upload_df_to_blob_as_parquet(payroll_facts_table, container_client, 'rawdata/payroll_facts_table.parquet')
upload_df_to_blob_as_parquet(title_dim, container_client, 'rawdata/title_dim.parquet')

