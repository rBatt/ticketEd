#!/Users/Battrd/anaconda3/bin/python
# #!/usr/local/bin/python3

import pandas as pd
import numpy as np
#%%
print('hi all')

print("test string 2")

#%%

# Not using this b/c it seems too slow to be worth the saved disk space, and a bit annoying to use their query system
# Probably only worthwhile if I wanted to continuously update the data set
#query = ("https://data.cityofnewyork.us/resource/aagd-wyjz.json?"
#         "$limit=10000"
#         )
#raw_data = pd.read_json(query)

# Parking Violations:
#   2019 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2019/pvqr-7yc4
#   2018 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2018/a5td-mswe
#   2017 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2017/2bnn-yakx
#   2016 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2016/kiv2-tbus
#   2015 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2015/c284-tqph
#   2014 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2014-August-/jt7v-77mi
#   **Note that 2014 is a partial year\n

# Open Parking and Camera Violations:
#   data https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89
#   eda vis https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/i4p3-pe6a


# Parking Violation Codes:
#   https://data.cityofnewyork.us/Transportation/DOF-Parking-Violation-Codes/ncbg-6agr

