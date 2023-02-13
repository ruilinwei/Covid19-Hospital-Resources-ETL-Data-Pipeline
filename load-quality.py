"""Implement a script to load quality data in the database"""

import psycopg
import pandas as pd
import numpy as np
import sys

# Connect to the sculptor
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="ruilinw",
    user="ruilinw", password="Eip8oosei"
)
cur = conn.cursor()
n_start = pd.read_sql_query("select count(*) from hospital_info;",
                            conn).iloc[0, 0]
# Acquire file name from the command line (3rd element)
file_name = sys.argv[2]
# Acquire date of the file from the command line (2nd element)
date = sys.argv[1]

# Identify -999999 and Nan as missing values
missing_values = [-999999, np.nan]
# Make dataframe from the imported .csv file
# affirm that pre-identified missing values (-999999 and Nan)
# as na_values within the dataframe
df = pd.read_csv(file_name, na_values=missing_values)
# If the value is "Not Available" in the column "Hospital overall rating"
# convert into Nan value
df.loc[df["Hospital overall rating"] ==
       "Not Available", "Hospital overall rating"] = np.nan

# Enable auto commit
# conn.autocommit = True


# Create a function that transforms string value to float
# to quantify hospital overall rating
# return None otherwise
def if_float_for_str(n):
    if (type(n) == str):
        return float(n)
    else:
        return None


# Create an empty dataframe that will store the rows
# for which values fail to be inserted into the database
df2 = pd.DataFrame(columns=[
    "Facility ID", "State", "Facility Name", "Address",
    "City", "ZIP Code", "Hospital Type",
    "Hospital Ownership", "Emergency Services", "Hospital overall rating"])
# Instantiate a varible that counts the number of rows
# for which values fail to be inserted into the database
nrow = 0
# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_quality)
num_rows_inserted_quality = 0
# Insert each row of the dataset to SQL table
# Connect to SQL and make transaction
with conn.transaction():
    # Perform for-loop execution for every row in the dataframe
    for row in range(df.shape[0]):
        # Identify each column in the dataframe
        # (as pre-defined from SQL schema)
        hospital_pk = str(df.loc[row, "Facility ID"])
        time = date
        state = df.loc[row, "State"]
        hospital_name = df.loc[row, "Facility Name"]
        address = df.loc[row, "Address"]
        city = df.loc[row, "City"]
        zip = str(df.loc[row, "ZIP Code"])
        type_of_hospital = df.loc[row, "Hospital Type"]
        ownership = df.loc[row, "Hospital Ownership"]
        emergency = df.loc[row, "Emergency Services"]
        overall_quality_rating = df.loc[row, "Hospital overall rating"]
        # Call try-except so that we can store
        # fail-to-upload values into df2
        try:
            with conn.transaction():
                # If ON CONFLICT (if hospital data already exists),
                # DO NOTHING (we do not add any further information
                # in this case)
                cur.execute(
                    "insert into hospital_info (hospital_pk, state,"
                    "hospital_name,"
                    "address, city, zip)"
                    "values (%s, %s, %s, %s, %s, %s)"
                    "ON CONFLICT (hospital_pk) DO UPDATE SET state = "
                    "EXCLUDED.state,"
                    "hospital_name = EXCLUDED.hospital_name,"
                    "address = EXCLUDED.address,"
                    "city = EXCLUDED.city,"
                    "zip = EXCLUDED.zip",
                    (hospital_pk,
                        state,
                        hospital_name,
                        address, city,
                        zip))
                # Insert pre-identified values for each variable into SQL
                # for each row in the dataframe
                # Execute SQL query
                cur.execute(
                    "insert into hospital_quality"
                    "(hospital_pk, update_time, type_of_hospital,"
                    "ownership, emergency, overall_quality_rating)"
                    "values ( %s, %s, %s, %s, %s, %s)",
                    (hospital_pk, time,
                        type_of_hospital,
                        ownership,
                        emergency,
                        if_float_for_str(overall_quality_rating)))
        except Exception as e:
            # If the execution fails, store the fail-to-insert row (entire row)
            # into the dataframe df2
            print("Insert failed!", e)
            df2.loc[nrow] = list(df.loc[row, [
                            "Facility ID", "State", "Facility Name", "Address",
                            "City", "ZIP Code", "Hospital Type",
                            "Hospital Ownership", "Emergency Services",
                            "Hospital overall rating"]])
            # Add a count to the number of rows
            # for which values fail to be inserted into the database
            nrow += 1
        else:
            # If properly added to the SQL table,
            # add a count to the number of rows updated
            # for hospital_quality
            num_rows_inserted_quality += 1

# Print number of rows where updates failed
print("The number of failed updates is ", nrow)
# Print the number of rows where updates succeeded
print("The number of successful updates is ", num_rows_inserted_quality)
# Export the collection fail-to-insert data in CMS_data
# (dataframe, df2) as a .csv file
df2.to_csv("failed_rows_cms.csv")
n_end = pd.read_sql_query("select count(*) from hospital_info;",
                          conn).iloc[0, 0]
print("The number of newly added hospital is ", n_end-n_start)
conn.commit()
conn.close()
