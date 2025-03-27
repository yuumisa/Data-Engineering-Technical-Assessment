from datetime import datetime
import numpy as np
import pandas as pd
import re

# Deal Table Creation

# Ingest all necessary data
bsp = pd.read_excel('../Data/Business Services Pipeline.xlsx',skiprows=5,dtype=str)

crhp = pd.read_excel('../Data/Consumer Retail and Healthcare Pipeline.xlsx',skiprows=8,dtype=str)
crhp.drop(crhp.tail(2).index,inplace=True)

# ### BSP Cleanup

# Business Service Cleanup
# Removing $, (LTM) from all Revenue/EBITDA columns. Replacing CAD and C$ with CAD in the new currency column
bsp['Currency'] = 'USD'

# Set the canadian dollar exceptions
bsp.loc[(bsp['2014A EBITDA'].astype(str).str.startswith("C$") | bsp['2014A EBITDA'].astype(str).str.startswith("CAD")), ['Currency']] = 'CAD'
bsp.loc[(bsp['2015A EBITDA'].astype(str).str.startswith("C$") | bsp['2015A EBITDA'].astype(str).str.startswith("CAD")), ['Currency']] = 'CAD'
bsp.loc[(bsp['2016A EBITDA'].astype(str).str.startswith("C$") | bsp['2016A EBITDA'].astype(str).str.startswith("CAD")), ['Currency']] = 'CAD'
bsp.loc[(bsp['2017A/E EBITDA'].astype(str).str.startswith("C$") | bsp['2017A/E EBITDA'].astype(str).str.startswith("CAD")), ['Currency']] = 'CAD'
bsp.loc[(bsp['2018E EBITDA'].astype(str).str.startswith("C$") | bsp['2018E EBITDA'].astype(str).str.startswith("CAD")), ['Currency']] = 'CAD'

# Remove text from revenue fields
bsp['2014A EBITDA'] = bsp['2014A EBITDA'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')
bsp['2015A EBITDA'] = bsp['2015A EBITDA'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')
bsp['2016A EBITDA'] = bsp['2016A EBITDA'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')
bsp['2017A/E EBITDA'] = bsp['2017A/E EBITDA'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')
bsp['2018E EBITDA'] = bsp['2018E EBITDA'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')

# Remove text from Enterprise Value and Equity Investment Est. columns
bsp['Enterprise Value'] = bsp['Enterprise Value'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')
bsp['Equity Investment Est.'] = bsp['Equity Investment Est.'].apply(lambda x: re.sub("[^0-9\.]", "", str(x)) if x != np.nan else '')

# Convert all "Dead" values in Status to be "Passed/Dead" to be in line with crhp table
bsp.loc[bsp['Status'] == 'Dead', ['Status']] = 'Passed/Dead'

# Datetime is inconstant with some fields missing the year
# As a temp solution, we will clean/leave the data as is until an agreement can be made 
# Except with blanks, they will be replaced with a placeholder of 12/31/2099 (arbitrary until client resolution)
#bsp.loc[bsp['Date Added'].isna(), ['Data Added']] = '2099-12-31 '
bsp['Date Added'] = bsp['Date Added'].apply(lambda x: str(x).split(' ')[0])
bsp.loc[bsp['Date Added'] == 'nan',['Date Added']] = '2099-12-31'

# ### CRHP Cleanup

# Remove extraneous rows
crhp = crhp.loc[~crhp['Company Name'].isna()].copy()

# Same fix to dates, but years are intact so no problems there
crhp['Date Added'] = crhp['Date Added'].apply(lambda x: str(x).split(' ')[0])

# Numbers do not need to be cleaned
# Assumed all deals are in USD
crhp['Currency'] = 'USD'

# Rename Est. Equity investment column to be in-line with bsp table
crhp = crhp.rename({'Est. Equity Investment':'Equity Investment Est.'}, axis=1)

# ### Create Deals Table

# Concat two tables
deals_df = pd.concat([bsp,crhp]).reset_index(drop=True)

# Adding in "Last Contact" column
deals_df['Last Contact'] = ''

# Assigned Company Codes
company_code_map = pd.read_excel('../Lookups/Company Code Lookup.xlsx',dtype=str).set_index('Company Name').to_dict()['Company Code']
deals_df['Company Code'] = deals_df['Company Name'].map(company_code_map)

# Create a unique deal ID for better tracking with various systems (Set at 10000 and up to avoid overlap with company codes and contact IDs)
deals_df = deals_df.reset_index()
deals_df['Deal ID'] = deals_df['index'].apply(lambda x: str((int(x) + 10000)).zfill(5))

# Reorganizing Columns
deals_df = deals_df[['Deal ID','Company Name','Company Code', 'Project Name', 'Date Added','Last Contact', 'Invest. Bank', 'Banker','Banker Email',
       'Banker Phone Number','Sourcing', 'Transaction Type','Currency', 'LTM Revenue', 'LTM EBITDA',
       '2014A EBITDA', '2015A EBITDA', '2016A EBITDA', '2017A/E EBITDA',
       '2018E EBITDA', 'Vertical', 'Sub Vertical', 'Enterprise Value',
       'Equity Investment Est.', 'Status','Portfolio Company Status', 'Active Stage',
       'Passed Rationale', 'Current Owner','Business Description', 'Lead MD',
       ]]

# Export as xlsx for viewing and csv (for this example) for loading
deals_df.to_excel('../Load Files/XLSX/Deals.xlsx',index=False)
deals_df.to_csv('../Load Files/CSV/Deals.csv',index=False)

print('Export Successful')
