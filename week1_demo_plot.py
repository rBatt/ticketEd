import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

username = 'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

con = None
con = psycopg2.connect(database = dbname, user = username)
sql_query = """
  SELECT "index", "SummonsNumber", "IssueDate", "ViolationCode", "ViolationLocation", "ViolationPrecinct", "ViolationTime", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision", "ViolationLegalCode", "ViolationPostCode"
  FROM "parkVio2017"
  WHERE "ViolationCounty" = 'NY';
"""
pv17_from_sql = pd.read_sql_query(sql_query,con, parse_dates={"IssueDate": "%m/%d/%Y"})

#%% Plotting for Week 1 Demo
pv17_nByDate = pv17_from_sql.groupby(by=["IssueDate"]).size().reset_index(name="counts")
start_date = "2016-06-01" #pd.to_datetime(["2016-06-01"])
end_date = "2017-06-27" #pd.to_datetime(["2017-12-31"])
mask = (pv17_nByDate["IssueDate"] > start_date) & (pv17_nByDate["IssueDate"] < end_date)
pv17_nByDate = pv17_nByDate[mask]

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
fig = plt.figure(facecolor='white') # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.plot(pv17_nByDate[['IssueDate']], pv17_nByDate[['counts']], color='black', linewidth=3, marker='o', label='Number of Tickets')
plt.ylabel("Tickets Issued per Day")
plt.xlabel("Date")
plt.title("Manhattan 2017")
# ax.set_axis_bgcolor("white")
plt.legend()
myFmt = DateFormatter("%b-%d \n %Y")
ax.xaxis.set_major_formatter(myFmt)
plt.show()