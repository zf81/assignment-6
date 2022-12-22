#import packages
import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random
import cryptography

#login credentials
load_dotenv()

AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD= os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")

#connecting
connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)


### show databases
print(db_azure.table_names())

#### fake stuff 
fake = Faker()

fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'zip_code':fake.zipcode(),
        'city':fake.city(), 
        'state':fake.state(),
        'phone_number':fake.phone_number()

    } for x in range(50)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])

#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')


#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

#### real cpt codes
cpt_codes = pd.read_csv('ENTER CSV')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
cpt_codes_1k = cpt_codes.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')

####real loinc codes
loinc_codes = pd.read_csv('enter LOINC')


########## INSERTING IN FAKE PATIENTS ##########
insertQuery = "INSERT INTO patients (mrn, first_name, last_name, dob, gender, zip_code, city, state, phone_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"


for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'],row['dob'], row['gender'], row['zip_code'], row['city'], row['state'], row['phone_number']))
    print("inserted row: ", index)

# # query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)



########## INSERTING IN CONDITIONS ##########
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM conditions", db_azure)


########## INSERTING IN FAKE MEDICATIONS ##########
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 75:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_medications", db_azure)

######### INSERTING IN TREATMENTS #########
insertQuery = "INSERT INTO treatments_procedures (cpt_code, cpt_code_description) VALUES (%s, %s)"

TreatmentRowCount = 0
for index, row in cpt_codes_1k.iterrows():
    TreatmentRowCount += 1
    db_azure.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if TreatmentRowCount == 75:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM treatments_procedures", db_azure)

####### INSERTING IN SOCIAL DETERMINANTS ########
insertQuery = "INSERT INTO social_determinants (loinc_code, loinc_code_description) VALUES (%s, %s)"

RowCount = 0
for index, row in loinc_codes.iterrows():
    RowCount += 1
    db_azure.execute(insertQuery, (row['loinc_code'], row['loinc_code_description']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if RowCount == 75:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM social_determinants", db_azure)


######## patient_summary table ######

# first, lets query conditions, medications, and patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_azure)

# create a dataframe that is stacked and give each patient two conditions and two medications
df_patient_summary = pd.DataFrame(columns=['mrn'])
for index, row in df_patients.iterrows():
   
    df_conditions_sample = df_conditions.sample(n=2)
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
 
    df_medications_sample = df_medications.sample(n=2)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    
    df_patient_summary = df_patient_summary.append(df_conditions_sample)
    df_patient_summary = df_patient_summary.append(df_medications_sample)

print(df_patient_summary.head(20))

#get rid of Nan values
df_patient_summary['icd10_code'] = df_patient_summary['icd10_code'].fillna(method='ffill').fillna(method='bfill')
df_patient_summary['med_ndc'] = df_patient_summary['med_ndc'].fillna(method='ffill').fillna(method='bfill')

print(df_patient_summary.head(20))

insertQuery = "INSERT INTO patient_summary (mrn, conditions, medication) VALUES (%s, %s, %s)"

for index, row in df_patient_summary.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code'], row['med_ndc']))
    print("inserted row: ", index)

####FOR THE PATIENT CONDITIONS TABLE
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

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

print(df_patient_conditions.head(20))

# now lets add a random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_azure) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)


####FOR THE PATIENT MEDICATIONS TABLE
# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
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

print(df_patient_medications.head(10))

# now lets add a random medication to each patient
insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)
