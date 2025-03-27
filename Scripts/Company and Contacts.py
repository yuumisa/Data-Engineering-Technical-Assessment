from datetime import datetime
import numpy as np
import pandas as pd
import re

# Ingest all necessary data
bsp = pd.read_excel('../Data/Business Services Pipeline.xlsx',skiprows=5,dtype=str)

crhp = pd.read_excel('../Data/Consumer Retail and Healthcare Pipeline.xlsx',skiprows=8,dtype=str)
crhp.drop(crhp.tail(2).index,inplace=True)

contact_t1 = pd.read_excel('../Data/Contacts.xlsx',dtype=str,sheet_name="Tier 1's")

contact_t2 = pd.read_excel('../Data/Contacts.xlsx',dtype=str,sheet_name="Tier 2's")

pe_comp = pd.read_excel('../Data/PE Comps.xlsx',dtype=str,skiprows=2)

# Company Table Creation

# Keep it simple at first, grab only explicit company names (usually first column)
company_list = pd.DataFrame(columns=['Company Name'])
company_list = pd.concat([company_list,
                          bsp.loc[~bsp['Company Name'].isna()]['Company Name'].drop_duplicates(),
                          crhp.loc[~crhp['Company Name'].isna()]['Company Name'].drop_duplicates(),
                          contact_t1.rename({'Firm':'Company Name'},axis=1)[['Company Name']].drop_duplicates(),
                          contact_t2.rename({'Firm':'Company Name'},axis=1)[['Company Name']].drop_duplicates(),
                          pe_comp.loc[~pe_comp['Company Name'].isna()]['Company Name'].drop_duplicates()]).reset_index(drop=True)
company_list = company_list.drop_duplicates().reset_index().copy()

# Start from 1 and count up
# This is assuming that there are currently no duplicate company names and thus we can do this method
# If there were duplicate companies present, a different approach would need to be employed
company_list['Company Code'] = company_list['index'].apply(lambda x: str((int(x) + 1)).zfill(5))
company_list = company_list.drop('index',axis=1)
# Exporting to a lookup file
company_list.to_excel('../Lookups/Company Code Lookup.xlsx',index=False)
company_code_map = company_list.set_index('Company Name').to_dict()['Company Code']

# Begin consolidating company columns
# Slight manual cleanup on Contact info (psuedo-csv format)
pe_comp = pe_comp.drop(0)
pe_comp['Private Equity Indiciator'] = 'X'

# Cleaning pe_comp table to prepare for append
pe_comp['Sectors'] = pe_comp['Sectors'].apply(lambda x: ', '.join(str(x).lstrip('- ').split('\n- ')))
pe_comp = pe_comp.rename({'Sectors':'Vertical'},axis=1)
pe_comp['Sample Portfolio Companies'] = pe_comp['Sample Portfolio Companies'].apply(lambda x: ', '.join(str(x).lstrip('- ').split('\n- ')))
pe_comp['Comments'] = pe_comp['Comments'].apply(lambda x: str(x).lstrip('-'))
pe_comp.loc[pe_comp['Comments'] == 'nan', ['Comments']] = np.nan

# Creating company table
company_df = pd.concat([pe_comp,
                        bsp.loc[~bsp['Company Name'].isna()].rename({'Business Description':'Comments'},axis=1)[['Company Name','Vertical','Sub Vertical','Comments']],
                        crhp.loc[~crhp['Company Name'].isna()].rename({'Business Description':'Comments'},axis=1)[['Company Name','Vertical','Sub Vertical','Comments']],
                        contact_t1['Group'].groupby([contact_t1['Firm']]).apply(set).apply(', '.join).reset_index().rename({'Firm':'Company Name','Group':'Vertical'},axis=1),
                        contact_t2['Group'].groupby([contact_t2['Firm']]).apply(set).apply(', '.join).reset_index().rename({'Firm':'Company Name','Group':'Vertical'},axis=1)])
company_df['Company Code'] = company_df['Company Name'].map(company_code_map)
company_df = company_df[['Company Code','Company Name','Website','Vertical','Private Equity Indiciator','AUM\n(Bns)','Sample Portfolio Companies','Comments']].sort_values('Company Code').reset_index(drop=True)
company_df.columns = ['Company Code','Company Name','Website','Sector','Private Equity Indiciator','AUM','Sample Portfolio Companies','Comments']

# Export as xlsx for viewing and csv (for this example) for loading
company_df.to_excel('../Load Files/XLSX/Company.xlsx',index=False)
company_df.to_csv('../Load Files/CSV/Company.csv',index=False)

# Contact Table Creation

# The current contact table format will be maintained
combined_contacts = pd.concat([contact_t1,contact_t2])

# Grab contact info from Private Equity Company Table from crhp table
# Grab from contact 1 column
pe_split_1 = pe_comp.loc[~pe_comp['Contact Name 1'].isna()].copy()
pe_split_1['Name'] = pe_split_1['Contact Name 1'].apply(lambda x: x.split(',')[0])
pe_split_1['Title'] = pe_split_1['Contact Name 1'].apply(lambda x: x.split(',')[1])
pe_split_1['Phone'] = pe_split_1['Contact Name 1'].apply(lambda x: x.split(',')[2])
pe_split_1['E-mail'] = pe_split_1['Contact Name 1'].apply(lambda x: x.split(',')[3])

# Grab from contact 2 column
pe_split_2 = pe_comp.loc[~pe_comp['Contact 2'].isna()].copy()
pe_split_2['Name'] = pe_split_2['Contact 2'].apply(lambda x: x.split(',')[0])
pe_split_2['Title'] = pe_split_2['Contact 2'].apply(lambda x: x.split(',')[1])
pe_split_2['Phone'] = pe_split_2['Contact 2'].apply(lambda x: x.split(',')[2])
pe_split_2['E-mail'] = pe_split_2['Contact 2'].apply(lambda x: x.split(',')[3])

# Cannot fill in City, Birthday, Coverage Person,  Preferred Contact Method, Sub-Vertical, Secondary Phone due to lack of info
pe_combined = pd.concat([pe_split_1,pe_split_2])[['Name','Title','Phone','E-mail','Company Name','Vertical']].rename({'Company Name':'Firm','Vertical':'Group'},axis=1)

# Append to contact table
combined_contacts = pd.concat([combined_contacts,pe_combined]).reset_index(drop=True)

# Assign new contact numbers (3000 and above to not overlap with companies)
combined_contacts = combined_contacts.reset_index()
combined_contacts['Contact ID'] = combined_contacts['index'].apply(lambda x: str((int(x) + 3000)).zfill(5))

# Grab assigned company ids from earlier
combined_contacts['Company Code'] = combined_contacts['Firm'].map(company_code_map)

# Cleanup
combined_contacts = combined_contacts[['Contact ID','Name','Title','Company Code','Firm','Group','Sub-Vertical','E-mail',
                                       'Phone','Secondary Phone','City','Birthday','Coverage Person','Preferred Contact Method']].rename({'Firm':'Company Name'},axis=1)

# Export as xlsx for viewing and csv (for this example) for loading
combined_contacts.to_excel('../Load Files/XLSX/Contacts.xlsx',index=False)
combined_contacts.to_csv('../Load Files/CSV/Contacts.csv',index=False)

print('Export Successful')
