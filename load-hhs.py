"""Implement a script to load HHS data in the database"""

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
# Acquire file name from the command line (2nd element)
file_name = sys.argv[1]

# Make dataframe from the imported .csv file
df = pd.read_csv(file_name)
# Replace Nan values to None
# (this allows Python to recognize None = null values)

# Enable auto commit
# conn.autocommit = True


# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_info)
num_rows_inserted_info = 0

# Identify -999999 and Nan as missing values
missing_values = [-999999, np.nan]
# Make dataframe from the imported .csv file
# affirm that pre-identified missing values (-999999 and Nan)
# as na_values within the dataframe
df = pd.read_csv(file_name, na_values=missing_values)

df = df.replace({np.nan: None})

# Create a function that transforms Nan value to None
# (this allows Python to recognize None = null values)
# Cast the value "n" as float otherwise


def if_float(n):
    if (n is None):
        return None
    else:
        return float(n)


# Instantiate a varible that counts the number of rows
# for which values fail to be inserted into the database
nrow = 0

# Create an empty dataframe that will store the rows
# for which values fail to be inserted into the database
df2 = pd.DataFrame(
    columns=[
        "hospital_pk",
        "collection_week",
        "state",
        "hospital_name",
        "address",
        "city",
        "zip",
        "fips_code",
        "geocoded_hospital_address",
        "all_adult_hospital_beds_7_day_avg",
        "all_pediatric_inpatient_beds_7_day_avg",
        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
        "all_pediatric_inpatient_bed_occupied_7_day_avg",
        "total_icu_beds_7_day_avg",
        "icu_beds_used_7_day_avg",
        "inpatient_beds_used_covid_7_day_avg",
        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"])

# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_weekly)
num_rows_inserted_weekly = 0
# Insert each row of the dataset to SQL table
# Connect to SQL and make transaction
with conn.transaction():
    # Perform for-loop execution for every row in the dataframe
    for row in range(df.shape[0]):
        # Identify each column in the dataframe
        # (as pre-defined from SQL schema)
        hospital_pk = str(df.loc[row, "hospital_pk"])
        state = df.loc[row, "state"]
        hospital_name = df.loc[row, "hospital_name"]
        address = df.loc[row, "address"]
        city = df.loc[row, "city"]
        zip = str(df.loc[row, "zip"])
        fips_code = str(df.loc[row, "fips_code"])
        geocoded = (df.loc[row, "geocoded_hospital_address"])
        # Since geocoded constitues of 2 variables, logitude and latitude,
        # split the values into 2 different components (logitude and latitude)
        # if value is not None
        if (((geocoded is None))):
            longitude = None
            latitude = None
        else:
            longitude = if_float(geocoded[6:].strip("()").split(" ")[0])
            latitude = if_float(geocoded[6:].strip("()").split(" ")[1])
        collection_week = df.loc[row, "collection_week"]
        all_adult_hospital_beds_7_day_avg = df.loc[
                    row,
                    "all_adult_hospital_beds_7_day_avg"]
        all_pediatric_inpatient_beds_7_day_avg = df.loc[
                    row,
                    "all_pediatric_inpatient_beds_7_day_avg"]
        adult_inpatient_bed_occupied_coverage = df.loc[
                    row,
                    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"]
        pediatric_inpatient_occupied = df.loc[
                    row,
                    "all_pediatric_inpatient_bed_occupied_7_day_avg"]
        total_icu_beds_7_day_avg = df.loc[
                    row,
                    "total_icu_beds_7_day_avg"]
        icu_beds_used_7_day_avg = df.loc[
                    row,
                    "icu_beds_used_7_day_avg"]
        inpatient_beds_used_covid_7_day_avg = df.loc[
                    row,
                    "inpatient_beds_used_covid_7_day_avg"]
        staffed_icu_adult_confirmed_covid_ = df.loc[
                    row,
                    "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
        # Call try-except so that we can store
        # fail-to-upload values into df2
        try:
            with conn.transaction():
                # Execute SQL query
                # If ON CONFLICT (if hospital data already exists),
                # DO NOTHING (only update new information in this case)
                cur.execute(
                    # Insert pre-identified values for each variable into SQL
                    # for each row in the dataframe
                    "insert into hospital_info (hospital_pk, state,"
                    "hospital_name, address, city, zip, fips_code, longitude,"
                    "latitude) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    "ON CONFLICT (hospital_pk) DO UPDATE SET "
                    "state = EXCLUDED.state,"
                    "hospital_name = EXCLUDED.hospital_name,"
                    "address = EXCLUDED.address,"
                    "city = EXCLUDED.city,"
                    "zip = EXCLUDED.zip",
                    (hospital_pk,
                        state,
                        hospital_name,
                        address, city,
                        zip,
                        fips_code,
                        longitude,
                        latitude))
                cur.execute(
                    # Insert pre-identified values for each variable into SQL
                    # for each row in the dataframe
                    "insert into hospital_weekly(hospital_pk,"
                    "collection_week, all_adult_hospital_beds_7_day_avg,"
                    "all_pediatric_inpatient_beds_7_day_avg,"
                    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage,"
                    "all_pediatric_inpatient_bed_occupied_7_day_avg,"
                    "total_icu_beds_7_day_avg, icu_beds_used_7_day_avg,"
                    "inpatient_beds_used_covid_7_day_avg,"
                    "staffed_icu_adult_patients_confirmed_covid_7_day_avg)"
                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (hospital_pk,
                        collection_week,
                        if_float(all_adult_hospital_beds_7_day_avg),
                        if_float(all_pediatric_inpatient_beds_7_day_avg),
                        if_float(adult_inpatient_bed_occupied_coverage),
                        if_float(pediatric_inpatient_occupied),
                        if_float(total_icu_beds_7_day_avg),
                        if_float(icu_beds_used_7_day_avg),
                        if_float(inpatient_beds_used_covid_7_day_avg),
                        if_float(staffed_icu_adult_confirmed_covid_)))
        except Exception as e:
            # If the execution fails, store the fail-to-insert row (entire row)
            # into the dataframe df2
            print("Insert failed!", e)
            df2.loc[nrow] = list(df.loc[
                row, ["hospital_pk",
                      "collection_week",
                      "state",
                      "hospital_name",
                      "address",
                      "city",
                      "zip",
                      "fips_code",
                      "geocoded_hospital_address",
                      "all_adult_hospital_beds_7_day_avg",
                      "all_pediatric_inpatient_beds_7_day_avg",
                      "all_adult_hospital_inpatient"
                      "_bed_occupied_7_day_coverage",
                      "all_pediatric_inpatient_bed_occupied_7_day_avg",
                      "total_icu_beds_7_day_avg",
                      "icu_beds_used_7_day_avg",
                      "inpatient_beds_used_covid_7_day_avg",
                      "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]])
            # Add a count to the number of rows
            # for which values fail to be inserted into the database
            nrow += 1
        else:
            # If properly added to the SQL table,
            # add a count to the number of rows updated
            # for hospital_weekly
            num_rows_inserted_weekly += 1

# Print number of rows where updates failed
print("The number of failed updates is ", nrow)
# Print the number of rows where updates succeeded
print("The number of successful updates is ", num_rows_inserted_weekly)
# Export the collection fail-to-insert data
# (dataframe, df2) as a .csv file
df2.to_csv("failed_rows_hhs.csv")
n_end = pd.read_sql_query("select count(*) from hospital_info;",
                          conn).iloc[0, 0]
print("The number of newly added hospital is ", n_end-n_start)
conn.commit()
conn.close()
