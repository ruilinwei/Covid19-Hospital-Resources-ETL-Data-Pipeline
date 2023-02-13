"""Implementation fro SQL tables to analyze the data sets"""


import psycopg
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import plotly.express as px
import streamlit as st
import sys


warnings.filterwarnings("ignore")
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="ruilinw",
    user="ruilinw", password="Eip8oosei"
)
cur = conn.cursor()

week = sys.argv[1]

# Q1 A summary of how many hospital records were loaded in the most recent
# week, and how that compares to previous weeks.

records = pd.read_sql_query("SELECT collection_week AS Date, "
                            "count(*) AS Hospitals "
                            "FROM hospital_weekly "
                            "WHERE collection_week <= %s"
                            "GROUP BY collection_week "
                            "ORDER BY collection_week desc ",
                            conn, params=(week,))

# Q2 A table summarizing the number of adult and pediatric beds available
# this week, the number used, and the number used by patients with COVID,
# compared to the 4 most recent weeks

beds = pd.read_sql_query("SELECT collection_week, "
                         "ROUND(sum(all_adult_hospital_beds_7_day_avg), 1) "
                         "AS adult_beds_available, "
                         "ROUND(sum(all_pediatric_inpatient_beds_7_day_avg)) "
                         "AS pediatric_beds_available, "
                         "ROUND(sum(all_adult_hospital_inpatient_bed_"
                         "occupied_7_day_coverage)) "
                         "AS adult_beds_occupied, "
                         "ROUND(sum(all_pediatric_inpatient_bed_"
                         "occupied_7_day_avg)) "
                         "AS pediatric_beds_occupied, "
                         "ROUND(sum(inpatient_beds_used_covid_7_day_avg)) "
                         "AS inpatient_beds_used_covid "
                         "FROM hospital_weekly "
                         "WHERE collection_week <= %s"
                         "GROUP BY collection_week "
                         "ORDER BY collection_week Desc "
                         "LIMIT 5; ",
                         conn, params=(week,))


# Q3 A graph or table summarizing the fraction of beds currently in use by
# hospital quality rating, so we can compare high-quality
# and low-quality hospitals

quality = pd.read_sql_query("SELECT overall_quality_rating, beds, "
                            "ROUND(beds/sum(beds) OVER (),2) AS fraction "
                            "FROM (SELECT overall_quality_rating, "
                            "sum(bed_in_use) AS beds "
                            "FROM (SELECT hospital_pk, "
                            "sum(all_adult_hospital_inpatient_"
                            "bed_occupied_7_day_coverage) + "
                            "sum(all_pediatric_inpatient_"
                            "bed_occupied_7_day_avg) "
                            "AS bed_in_use FROM hospital_weekly "
                            "WHERE collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week <= %s) "
                            "GROUP BY hospital_pk) A "
                            "JOIN (SELECT hospital_pk, overall_quality_rating "
                            "FROM hospital_quality) B "
                            "ON A.hospital_pk = B.hospital_pk "
                            "GROUP BY overall_quality_rating "
                            "ORDER BY overall_quality_rating) AS C;",
                            conn, params=(week,))

# Q4 A plot of the total number of hospital beds used per week, over all time,
# split into all cases and COVID cases

covid = pd.read_sql_query("SELECT collection_week, "
                          "ROUND(sum(all_adult_hospital_inpatient_bed_"
                          "occupied_7_day_coverage) + "
                          "sum(all_pediatric_inpatient_bed_occupied_7_"
                          "day_avg)) AS bed_in_use_all_cases,"
                          "sum(inpatient_beds_used_covid_7_day_avg) AS "
                          "bed_in_use_covid "
                          "FROM hospital_weekly "
                          "WHERE collection_week <= %s"
                          "GROUP BY collection_week "
                          "ORDER BY collection_week ASC;",
                          conn, params=(week,))

# Q5 A map showing the number of COVID cases by state (the first two digits of
# a hospital ZIP code is its state)

map_1 = pd.read_sql_query("SELECT state, sum(covid_cases) "
                          "AS total_covid_cases "
                          "FROM (SELECT hospital_pk, state "
                          "FROM hospital_info) A "
                          "JOIN (SELECT hospital_pk, "
                          "sum(inpatient_beds_used_covid_7_day_avg) "
                          "AS covid_cases "
                          "FROM hospital_weekly "
                          "WHERE collection_week = (SELECT "
                          "max(collection_week) "
                          "FROM hospital_weekly "
                          "WHERE collection_week <= %s) "
                          "GROUP BY hospital_pk) B "
                          "ON A.hospital_pk = B.hospital_pk "
                          "GROUP BY state; ",
                          conn, params=(week,))

# Q6 A table of the states in which the number of cases has increased
# by the most since last week

covid_2 = pd.read_sql_query("SELECT state, covid_cases_ct, covid_cases_lw, "
                            "(covid_cases_ct - covid_cases_lw) AS diff "
                            "FROM (SELECT state, sum("
                            "CASE WHEN collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week <= %s) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_ct, sum("
                            "CASE WHEN collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week < ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly  "
                            "WHERE collection_week <= %s)) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_lw "
                            "FROM (SELECT hospital_pk, state "
                            "FROM hospital_info) A "
                            "JOIN (SELECT hospital_pk,collection_week, "
                            "inpatient_beds_used_covid_7_day_avg "
                            "FROM hospital_weekly) B "
                            "ON A.hospital_pk = B.hospital_pk "
                            "GROUP BY state) AS C "
                            "WHERE covid_cases_ct > covid_cases_lw "
                            "ORDER BY diff DESC;",
                            conn, params=(week, week))

# A table of the hospitals (including names and locations) with the
# largest changes in COVID cases in the last week

covid_3 = pd.read_sql_query("SELECT hospital_name, address, "
                            "abs(covid_cases_ct - covid_cases_lw) AS diff "
                            "FROM (SELECT A.hospital_pk, "
                            "address, hospital_name, sum("
                            "CASE WHEN collection_week = ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week <= %s) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_ct, sum("
                            "CASE WHEN collection_week = ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week < "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week <= %s)) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_lw "
                            "FROM (SELECT hospital_pk, hospital_name, address "
                            "FROM hospital_info) A "
                            "JOIN (SELECT hospital_pk,collection_week, "
                            "inpatient_beds_used_covid_7_day_avg "
                            "FROM hospital_weekly) B "
                            "ON A.hospital_pk = B.hospital_pk "
                            "GROUP BY A.hospital_pk, A.hospital_name, "
                            "A.address) AS C "
                            "WHERE covid_cases_ct IS NOT NULL "
                            "AND covid_cases_lw IS NOT NULL "
                            "ORDER BY diff DESC "
                            "LIMIT 10;",
                            conn, params=(week, week))

# Setting the index from 1
records = records.set_index(records.index + 1)
beds = beds.set_index(beds.index + 1)
quality = quality.set_index(quality.index + 1)
covid = covid.set_index(covid.index + 1)
map_1 = map_1.set_index(map_1.index + 1)
covid_2 = covid_2.set_index(covid_2.index + 1)
covid_3 = covid_3.set_index(covid_3.index + 1)

# Implement streamlit
st.title("Hospital Operation Analysis Weekly Report %s" % (sys.argv[1]))

# Layout for Q1
st.subheader("Summary of Hospitals Added")

st.markdown("We analyze here how many hospitals have been added in the "
            "respective weeks. The system allows the readers to view the "
            "number of weekly data loaded. In the first column, the "
            "table consists of the updated week. In the second column, "
            "the table displays the number of new uploads that have "
            "been made to the database.")


records.columns = ['Collection Week', 'Number of Hospitals']

# Table for question 1
st.caption("Table 1: Count for Hospitals by week")
st.table(records)

st.write("##")

# Layout for Q2
st.subheader("Summary of Bed Operation")

st.markdown("The table allows the readers to view the number of "
            "beds available each week. Specifically, it illustrates "
            "the total capacity of each hospital in terms of adult beds, "
            "pediatric beds, and their occupancy (total number of beds "
            "occupied by adults or pediatric patients respectively, "
            "as well as the COVID patients, regardless of the age group)")

st.write("##")

# Table for question 2
st.caption("Table 2: Availability for Hospitals by week")

beds.columns = ['Collection Week',
                'Adult Beds Available',
                'Pediatric Beds Available',
                'Adult Beds Occupied',
                'Pediatric Beds Occupied',
                'COVID Beds Used']

st.table(beds.style.format({
    "Adult Beds Available": "{:.0f}",
    "Pediatric Beds Available": "{:.0f}",
    "Adult Beds Occupied": "{:.0f}",
    "Pediatric Beds Occupied": "{:.0f}",
    "COVID Beds Used": "{:.0f}"
    }))

st.write("##")

st.markdown("From Table 2, we can interpret the different types of beds "
            "available and occupied for the four different weeks just "
            "before the week we generate the report for.")

st.write("##")

# Layout for Q3
st.subheader("Plot for Hospitals on the Basis of Quality")

st.markdown("The plot provides a visual representation of what is "
            "the quality rating for the occupied beds, providing a "
            "perspective on high-quality and low-quality beds where "
            "quality ranges from 1 to 5 and the empty row value for "
            "rating stands for no rating.")

st.write("##")

st.caption("Figure 1: Classification of Hospitals by Ratings")

fig3, ax3 = plt.subplots()

# Plot for question 3
plot_3 = quality.plot(ax=ax3,
                      kind="bar",
                      x="overall_quality_rating",
                      y="fraction",
                      xlabel="Ratings",
                      ylabel="Fraction",
                      title="Classification of Hospitals by Ratings")

# Annotation for plot of question 3
for bar in plot_3.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),
           ncol=3, fancybox=True, shadow=True)

ax3.get_legend().remove()

st.pyplot(fig3)

st.write("##")

# Layout for Q4
st.subheader("Plot for Hospital Beds Occupied on the Basis of Type of Case")

st.markdown("The plot provides a visual representation of what is the "
            "bifurcation for the occupied beds on the basis of what type of "
            "case it is - COVID or otherwise, informing and indicating the "
            "count for other cases compared to COVID.")

st.caption("Figure 2: Classification of Hospitals by Type of Case")


fig4, ax4 = plt.subplots()


# Plot for question 4
plot_4 = covid.plot(ax=ax4,
                    kind="bar",
                    x="collection_week",
                    xlabel="Week",
                    ylabel="Beds in Use",
                    title="Classification of Hospitals by Type of Case")

# Annotation for plot of question 4
for bar in plot_4.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='best')

ax4.get_legend().remove()

st.pyplot(fig4)

st.write("##")

# Layout for Q5
st.subheader("Map Visualization of COVID Cases by State")

st.markdown("This is a map visualization of the number of COVID "
            "patients registered to hospitals, aggregated by the States "
            "where the hospitals are located. The number of total COVID "
            "cases are color coded - the States with lower number of "
            "patients will be colored with yellow (lighter color, 0~500), "
            "and the States with higher number of patients will be colored "
            "with indigo (darker color, 3000+ patients per State).")

st.caption("Figure 3: Interactive Map for COVID Cases by State")


# Plot for question 5
plot_5 = px.choropleth(map_1,
                       locations='state',
                       locationmode="USA-states",
                       scope="usa",
                       color='total_covid_cases',
                       color_continuous_scale="Viridis_r",
                       )
# plot_5.show()
st.plotly_chart(plot_5)

st.markdown("The map is an interactive map that shows the states with the "
            "covid cases. Ideally, it would be a better visual in an html "
            "form compared to the pdf form as you can see the numbers for "
            "each state when you put the cursor on the state. This map "
            "fails to incorporate the results for the different zip codes "
            "or county.")

st.write("##")

# Layout for Q6
st.subheader("States with the Highest Increase in the Number of COVID Cases")

st.markdown("This table displays the number of increased patient occupancy "
            "for COVID cases per each State. Specifically, the number of "
            "patients would be ordered in descending order, showing the State "
            "with the highest number of increase in the COVID patients "
            "compared to the last week’s data.")

# Table for question 6
st.caption("Table 3: Highest Number of COVID Cases in States")

covid_2.columns = ["State", "COVID Cases of Current Week",
                   "COVID cases of Last Week", "Difference"]

covid_3.columns = ["Hospital Name", "Address", "Difference"]

st.write("##")


st.table(covid_2.style.format({
    "COVID Cases of Current Week": "{:.0f}",
    "COVID cases of Last Week": "{:.0f}",
    "Difference": "{:.0f}"
    }))

st.write("##")

st.markdown("The first row in the table is showing the State with the "
            "highest new incidence of COVID patients. Each row in the list "
            "are the States in descending order with respect to the number "
            "of newly added cases of COVID patients.")

st.write("##")

# Layout for Q7
st.subheader("Hospital with the Highest Increase in the Number of COVID Cases")

st.markdown("This table displays the number of increased patient occupancy "
            "for COVID cases per each hospital. Specifically, the number of "
            "patients would be ordered in descending order, showing the "
            "hospital with the highest number of increase in the COVID "
            "patients compared to the last week’s data.")

# Table for question 7
st.caption("Table 4: Highest Number of COVID Cases in Hospitals")

st.table(covid_3.style.format({
    "Difference": "{:.0f}"
    }))
