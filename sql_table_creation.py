#import packages
import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker 

### drop the old tables
def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table} succesfully!')
    else:
        print(f'kept table {table}')


#get login credentials from env
load_dotenv()
AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USER")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")

#connecting to mysql 
connection_string = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string)

#### note to self, need to ensure server_paremters => require_secure_transport is OFF in Azure 
### show tables from databases

tableNames_azure = db_azure.table_names()

# reoder tables
tableNames_azure = ['medications','conditions', 'social_determinants','treatments_procedures','patients', 'patient_summary','patient_conditions','patient_medications']

# ### delete everything 
droppingFunction_all(tableNames_azure, db_azure)


#### first step below is just creating a basic version of each of the tables,
#### along with the primary keys and default values 

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
    
    
); 
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
    
); 
"""


table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc_code varchar(255) default null unique,
    loinc_code_description varchar(255) default null,
    PRIMARY KEY (id)

); 
"""


table_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    cpt_code varchar(255) default null unique,
    cpt_code_description varchar(255) default null,
    PRIMARY KEY (id)
    

); 
"""

table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_patient_summary = """
create table if not exists patient_summary (
    id int auto_increment,
    mrn varchar(255) default null,
    conditions varchar(255) default null,
    medication varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (conditions) REFERENCES conditions(icd10_code) ON DELETE CASCADE,
    FOREIGN KEY (medication) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

table_prod_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""

table_prod_patients_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""


#execute tables
db_azure.execute(table_patients)
db_azure.execute(table_medications)
db_azure.execute(table_conditions)
db_azure.execute(table_treatments_procedures)
db_azure.execute(table_social_determinants)
db_azure.execute(table_patient_summary)
db_azure.execute(table_prod_patient_conditions)
db_azure.execute(table_prod_patients_medications)

# get tables from db_azure
azure_tables = db_azure.table_names()