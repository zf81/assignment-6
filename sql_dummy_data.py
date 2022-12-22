# Import packages
import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker
import uuid
import random

load_dotenv()
AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")

connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

### show dbs
db_azure.table_names() 
print(db_azure.table_names())

# Insert Fake Patient Data 
fake = Faker ()
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
    } for x in range(15)]

df_fake_patients = pd.DataFrame(fake_patients)
# Drop the duplicate MRN
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])

# Real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

# Real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

# Real cpt codes 
cpt_codes = pd.read_csv("https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv")
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(
    subset = ['com.medigy.persist.reference.type.clincial.CPT.code', 'label']
)

# From pandas to sql
df_fake_patients.to_sql('production_patients', con=db_azure, if_exists='append', index=False)
# query db_azure 
df_azure = pd.read_sql_query("SELECT * FROM production_patients", db_azure)

# Insert fake medications 
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 15 rows
    if medRowCount == 15:
        break
# query db_azure 
df_azure = pd.read_sql_query("SELECT * FROM production_medications", db_azure)

# Inserting fake treatment_procedures 
insertQuery = "INSERT INTO treatment_procedures (cpt_code, cpt_description) VALUES (%s, %s)"

starting = 0
for index, row in cpt_codes_1k.iterrows():
    starting += 1
    db_azure.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    ## stop once we have 15 rows
    if starting == 15:
        break
# query db_azure 
df_azure = pd.read_sql_query("SELECT * FROM treatment_procedures", db_azure)

### insert fake conditions
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    ## stop once we have 15 rows
    if startingRow == 15:
        break
# query dbs 
df_azure = pd.read_sql_query("SELECT * FROM conditions", db_azure)


# Creating fake patient medication as patient_meds

# query dbs for id
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_azure) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_azure)

# create stacked df and assign patients random number of meds between 1-5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])

# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)
print(df_patient_medications.head(15))

# Add a random medication to each patient
insertQuery = "INSERT INTO patient_meds (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)


### create fake patient_treatment_procedures

# query dbs for id
df_treatment_procedures = pd.read_sql_query("SELECT cpt_description FROM treatment_procedures", db_azure) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_azure)

# create stacked df and assign patients random number of meds between 1-5
df_patient_treatment_procedures = pd.DataFrame(columns=['mrn', 'cpt_description'])

# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numTreatment = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_treatment_sample = df_treatment_procedures.sample(n=numTreatment)
    # add the mrn to the df_medications_sample
    df_treatment_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_treatment_procedures = df_patient_treatment_procedures.append(df_treatment_sample)
print(df_patient_treatment_procedures.head(15))

# now lets add a random procedure to each patient
insertQuery = "INSERT INTO patient_treatment_procedures (mrn, cpt_description) VALUES (%s, %s)"

for index, row in df_patient_treatment_procedures.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['cpt_description']))
    print("inserted row: ", index)


### create fake patient_conditions
# quesry dbs for id
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])

# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions.head(15))

# now lets add a random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)