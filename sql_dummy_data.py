import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine, null
from dotenv import load_dotenv
import os
from faker import Faker 
import uuid
import random
import numpy as np

#### Set up AZURE SQL Connection ####
AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USERNAME = os.getenv("AZURE_MYSQL_USERNAME") 
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")

connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USERNAME}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

# Check 
print(db_azure.table_names())

#### Insert Fake Patient Data ####
fake = Faker()

fake_patients = [
    {
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(50)]

df_fake_patients = pd.DataFrame(fake_patients)
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])

insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)


#### Insert Medication Data ####
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    if medRowCount == 75:
        break


#### Insert Conditions ####
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    if startingRow == 100:
        break


#### Insert Procedures ####
real_cpt_data = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_data_short = real_cpt_data.sample(1000, random_state=1)
cpt_data_short = cpt_data_short.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'],keep='first')

insertQuery = "INSERT INTO procedures (code_range, cpt_sections) VALUES (%s, %s)"

startingRow = 0
for index, row in cpt_data_short.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row db_azure: ", index)
    if startingRow == 100:
        break

#### Insert Social Determinants ####

#### Insert Patient-Conditions ####
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])

for index, row in df_patients.iterrows():
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    df_conditions_sample['mrn'] = row['mrn']
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions.head(20))

insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

#### Insert Patient-Medications ####
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_azure) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])

for index, row in df_patients.iterrows():
    numMedications = random.randint(1, 5)
    df_medications_sample = df_medications.sample(n=numMedications)
    df_medications_sample['mrn'] = row['mrn']
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)

