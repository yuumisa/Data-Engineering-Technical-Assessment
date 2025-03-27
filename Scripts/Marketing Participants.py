from datetime import datetime
import numpy as np
import pandas as pd
import re

# Marketing Participants Table Creation

# Ingest all necessary data
events_lpd = pd.read_excel('../Data/Events.xlsx',dtype=str,sheet_name='Leaders and Partners Dinner')

events_19mrc = pd.read_excel('../Data/Events.xlsx',dtype=str,sheet_name='2019 Market Re-Cap')

# Merge both tabs of Events table into one dataframe but mark each event seperately in a new column
events_lpd['Event'] = 'Leaders and Partners Dinner'
events_19mrc['Event'] = '2019 Market Re-Cap'
events_df = pd.concat([events_lpd,events_19mrc])

# Use contacts load file as lookup for events_df
combined_contacts = pd.read_csv('../Load Files/CSV/Contacts.csv',dtype=str,sep=',')

# Grab contact IDs alongside company
events_df = events_df.merge(combined_contacts[['Name','Contact ID','Company Code','Company Name','Title','E-mail']],how='left',on='E-mail')

# There are certain names that are not consistent with the actual contacts table
# Using the name from the contacts table
events_df = events_df.rename({'Name_y':'Name'},axis=1)[['Contact ID','Name','Title','E-mail','Event','Attendee Status','Company Code','Company Name']]

# Export as xlsx for viewing and csv (for this example) for loading
events_df.to_excel('../Load Files/XLSX/Marketing Participants.xlsx',index=False)
events_df.to_csv('../Load Files/CSV/Marketing Participants.csv',index=False)

print('Export Successful')
