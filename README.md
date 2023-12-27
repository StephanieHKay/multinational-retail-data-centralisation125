#
**Multinational Retail Data Centralisation**

##
Project Description:
1. Extracting and cleaning data from various data sources.
2. Creation of a database Schema.
3. SQL data querying.


##
**Installation instructions:**
 - Install the relevant packages by running the below terminal command:

```
pip install -r requirements.txt
```

##
**Usage instructions:**

The following technologies were used:
- Pandas and NumPy
- PostgreSQL
- SQLAlchemy
- PyYAML
- psycopg2
- boto3
- tabula-py

Use the command below in the database_utils.py this file:
```
python database_utils.py
```

##
**File structure:**
- database_utils.py: 
    iniiate a connection to the database and upload data.
- data_extraction.py:
    retrieve data from various sources (S3 Bucket, RDS, PDF, APIs etc).
- data_cleaning.py:
    clean the data.

