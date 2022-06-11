# dependencies
import pandas as pd
from sqlalchemy import create_engine


# ### Store Medals CSV into DataFrame

csv_file = "Resources/medals.csv"
medals_df = pd.read_csv(csv_file)
medals_df.head()


# ### Create new data with select columns

# create new df with only the columns we need
new_medals_df = medals_df[['Year', 'Country_Name', 'Gold', 'Silver', 'Bronze']].copy()

# preview
new_medals_df.head()


# ### Rename Country_Name to Country and add column for Total Medals

# add column for total medals
new_medals_df['Total'] = new_medals_df['Gold'] + new_medals_df['Silver'] + new_medals_df['Bronze']

# rename 'Country_Name' column to 'Country'
cols = {'Country_Name': 'Country'}
new_medals_df.rename(columns = cols, inplace=True)

# preview
new_medals_df.head()


# ### Remove all data before 1980 and after 2010

# remove data before 1980
new_medals_df = new_medals_df[new_medals_df['Year'] >= 1980]

# remove data after 2010
new_medals_df = new_medals_df[new_medals_df['Year'] <= 2010]

# preview
new_medals_df.head()


# ### Store Population CSV into DataFrame

csv_file = "Resources/population.csv"
pop_df = pd.read_csv(csv_file)
pop_df.head()


# ### Rename column to Country

# rename 'Unnamed: 0' column to 'Country'
cols = {'Unnamed: 0': 'Country'}
new_pop_df = pop_df.rename(columns = cols)
new_pop_df.set_index('Country', inplace=True)

# preview
new_pop_df.head()


# ### Transpose column to have years as index

# transpose
new_pop_df = new_pop_df.T
new_pop_df.head()


# ### Reorganize dataframe to have redundant years for every country

# reorganize table using stack and reset_index
new_pop_df = new_pop_df.stack().reset_index()
new_pop_df.head()


# ### Rename columns

# rename columns as needed
cols = {'level_0': 'Year', 0: 'Population'}
new_pop_df = new_pop_df.rename(columns = cols)

# preview
new_pop_df.head()

# create list of countries in olympics data
m_countries = list(new_medals_df['Country'].unique())
m_countries

# create list of countries in population data
p_countries = list(new_pop_df['Country'].unique())
p_countries


# ### Data Cleaning: Check for potential mismatched countries

# All countries in the Olympics data should be in the population data
# Loop to print countries that are in Olympics but not population data

for country in m_countries:
    if country not in p_countries:
        print(country)

# Loop to print countries that are in population data but not Olympics
# This will help potentially identify the matching country from the Olympics data set above
for country in p_countries:
    if country not in m_countries:
        print(country)

# Countries to rename in Population data to match with Olympics data
# 'United Kingdom' to 'Great Britain'
# 'Germany, East' to 'East Germany'
# 'Former U.S.S.R.' to 'Soviet Union'
# 'Korea, North' to 'North Korea'
# 'Former Czechoslovakia' to 'Czechoslovakia'
# 'Former Yugoslavia' to 'Yugoslavia'
# 'Germany, West' to 'West Germany'
# 'Cote dIvoire (IvoryCoast)' to 'Ivory Coast'
# 'Taiwan' to 'Chinese Taipei'
# 'Korea, South' to 'South Korea'
# 'Virgin Islands,  U.S.' to 'Virgin Islands'
# '' to 'Unified Team' ***This is related to dissolved Soviet Union territories in 1992
# 'Bahamas, The' to 'Bahamas'
# '' to 'Independent Olympic Participants' ***This is related to dissolved Soviet Union territories in 1992
# 'Former Serbia and Montenegro' to 'Serbia and Montenegro'

# dictionary with countries to rename
rename_dict = {
            'United Kingdom' : 'Great Britain',
            'Germany, East' : 'East Germany',
            'Former U.S.S.R.' : 'Soviet Union',
            'Korea, North' : 'North Korea',
            'Former Czechoslovakia' : 'Czechoslovakia',
            'Former Yugoslavia' : 'Yugoslavia',
            'Germany, West' : 'West Germany',
            'Cote dIvoire (IvoryCoast)' : 'Ivory Coast',
            'Taiwan' : 'Chinese Taipei',
            'Korea, South' : 'South Korea',
            'Virgin Islands,  U.S.' : 'Virgin Islands',
            'Bahamas, The' : 'Bahamas',
            'Former Serbia and Montenegro' : 'Serbia and Montenegro'
            }

# rename countries in population dataframe
new_pop_df = new_pop_df.replace({"Country": rename_dict})

# preview
new_pop_df.head()

# drop Unified Team and Independent Olympic Participants from Olympics dataframe
# these are groups that represent multiple countries that are not useful for our dataset

# remove Unified Team
new_medals_df = new_medals_df[new_medals_df['Country'] != 'Unified Team']

# remove Independent Olympic Participants
new_medals_df = new_medals_df[new_medals_df['Country'] != 'Independent Olympic Participants']

# drop Yugoslavia data after 1991 from combined dataframe. Further research showed athletes were allowed to compete
# under their flag, but there was no documented population as Yugoslavia was 'absorbed' by Serbia & Montenegro

# remove Yugoslavia after 1991. 
new_medals_df = new_medals_df.drop(new_medals_df[(new_medals_df['Country'] == 'Yugoslavia') & (new_medals_df['Year'] > 1991)].index)
#df_new = df.drop(df[(df['col_1'] == 1.0) & (df['col_2'] == 0.0)].index)
# preview
new_medals_df.head()

# confirm there are no longer countries in Olympics dataframe that aren't in population dataframe

# create list of countries in olympics data
m_countries = list(new_medals_df['Country'].unique())

# recreate list of countries in population data
p_countries = list(new_pop_df['Country'].unique())

# Loop to print countries that are in Olympics but not population data

for country in m_countries:
    if country not in p_countries:
        print(country)


# ### Successfully cleaned data. All countries in Olympic data have a match in population data

# ### Merge the two dataframe on Year and Country

# check data types in olympics dataframe
new_medals_df.dtypes

# check data types in population dataframe
new_pop_df.dtypes

# change 'Year' datatype in population dataframe to integer to allow merge to occur
new_pop_df['Year'] = new_pop_df['Year'].astype(str).astype(int)

# change 'Population' datatype in population dataframe to float
new_pop_df['Population'] = pd.to_numeric(new_pop_df['Population'],errors = 'coerce')

# confirm change was successful
new_pop_df.dtypes

# merge two dataframes
combined_df = pd.merge(new_medals_df, new_pop_df,  how='left', left_on=['Year','Country'], right_on = ['Year','Country'])

# preview
combined_df.head()

# check for null values
combined_df.isnull().values.sum()

# rename all columns to be all lowercase
# rename 'Country_Name' column to 'Country'
combined_df = combined_df.rename(columns=str.lower)

# preview
combined_df.head()


# ### Connect to local database

protocol = 'postgresql'
username = 'postgres'
password = 'admin'
host = 'localhost'
database_name = 'olympics_db'
rds_connection_string = f'{protocol}://{username}:{password}@{host}/{database_name}'
engine = create_engine(rds_connection_string)


# ### Check for tables

engine.table_names()


# ### Use pandas to load merged DataFrame into database

combined_df.to_sql(name='olympic_medals', con=engine, if_exists='replace', index=False)


# ### Confirm data has been added by querying the table
# * NOTE: can also check using pgAdmin

pd.read_sql_query('select * from olympic_medals', con=engine).head()