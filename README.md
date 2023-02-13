# Data-Engineering
We will be developing a data pipeline for the unstructured and messy data that has been provided to produce a structured SQL database, and automate the data updates and generate efficient and meaningful automated reports. The data provided will be from the US Department of Health and Human Services (HHS) about hospitals functioning in the country. The data includes over a hundred variables, we will be focusing at roughly thirty variables for our database and analysis through automated reports.

Specifically, in this Github there are three main components: schema.sql, load-hhs.py, and load-quality.py. 

### Steps to generate weekly reports
1. Run load-hhs.py file with argument of the name of the csv file in terminal to load HHS data for each week:
   "python load-hhs.py 2022-01-04-hhs-data.csv"
2. Run load-quality.py file with arguments of date of the records and name of csv file in terminal to load CMS data for each month:
   "python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv"
3. Run analytics.py file with argument of date for weekly report in terminal to generate the report in streamlit interactive platform:
   "python analytics.py 2022-09-30"

The detailed information for the files in this Github is as follows: 

### schema.sql
In this file, we identified three schemas - hospital_info, hospital_weekly, and hospital_quality. Each schema works as follows:

Table 1: Hospital_info: a table that stores permanent information about the hospitals 

Table 2: Hospital_weekly: a table that stores weekly updates regarding the hospital operation (these features will be updated weekly)

Table 3: Hospital_quality: a table that stores overall quality rating (these features will be updated several times a year)

### load-hhs.py
In this file, we create two execution functions. 

For table1: Update the table hospital_info, which only stores permanent information about the hospitals. Since these are permanent information they should stay the same when the weekly updates take place. Hence, when there is any conflict (i.e. when duplicates exist), we just ignore the row and proceed to the next row to store the information (by using ON CONFLICT DO NOTHING).

For table2:  Upate the table hospital_weekly, which stores weekly updated information about the hospitals. Since these are changing information, instead of ON CONFLICT DO NOTHING, we perform try-except clause, for which allows us to take an action (e.g. store the fail-to-insert data into a different dataframe) in the incident of any failure.

(Please check individual files for specific comments.)

We begin with data cleaning: we replace Nan values to None, which allows Python to recognize None = null values for our qunatitative analysis later. Then, we insert each row of the dataset to SQL table using SQL INSERT VALUE INTO query. In order to perform this, we need to perform for-loop execution for every row in the dataframe, identify each column in the dataframe (as pre-defined from SQL schema), and insert those values into the appropriate variable location within the schema. 


### load-quality.py

In this file, we create one execution function. 

For table3: Update the table hospital_quality, which stores irregular updates about hospital quality information. Since these are also changing information, instead of ON CONFLICT DO NOTHING, we perform try-except clause, for which allows us to take an action (e.g. store the fail-to-insert data into a different dataframe) in the incident of any failure.

(Please check individual files for specific comments.)

Once again, we begin with data cleaning: we replace Nan values to None, which allows Python to recognize None = null values for our qunatitative analysis later. Then, we insert each row of the dataset to SQL table using SQL INSERT VALUE INTO query. In order to perform this, we need to perform for-loop execution for every row in the dataframe, identify each column in the dataframe (as pre-defined from SQL schema), and insert those values into the appropriate variable location within the schema. 


### analytics.py

In this file, we create SQL queries to generate analytic graphs and tables for 7 analytic highlights with regards to the uploaded data (HHS and CMS). The points addressed in this analytic report are as follows:

* A summary of how many hospital records were loaded in the most recent week, and how that compares to previous weeks.
* A table summarizing the number of adult and pediatric beds available this week, the number used, and the number used by patients with COVID, compared to the 4 most recent weeks
* A graph or table summarizing the fraction of beds currently in use by hospital quality rating, so we can compare high-quality and low-quality hospitals
* A plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases
* A map showing the number of COVID cases by state (the first two digits of a hospital ZIP code is its state)
* A table of the states in which the number of cases has increased by the most since last week
* A table of the hospitals (including names and locations) with the largest changes in COVID cases in the last week

In this file, first we import streamlit package to generate our analytic report in HTML format. Then, we generate a grpah or a table that corresponds to each of our analytic topic. Since we are utilizing streamlit (import streamlit as st), the commands such as st.table or st.markdown are used.
