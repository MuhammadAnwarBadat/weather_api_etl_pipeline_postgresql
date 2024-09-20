#!/usr/bin/env python
# coding: utf-8

# ### Pipeline 1: Data from Web APIs to a Data Warehouse

# Use Case: Extract data from a public API, transform it, and load it into a data warehouse for analysis (e.g., weather data, financial data).
# 

# #### Importing necessary libraries

# In[1]:


import json
import requests
import pandas as pd
from datetime import datetime
from matplotlib import pyplot as plt
import seaborn as sns


# In[15]:


import os

def load_api_key(filepath):
    with open(filepath, 'r') as file:
        for line in file:
            if line.startswith("OPENWEATHER_API_KEY"):
                key = line.strip().split('=')[1]
                return key
    return None

# Load the API key from the config file
api_key = load_api_key("config.txt")

# Use the API key in your request
city = "Little Rock"
url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"

response = requests.get(url)
weather_data = response.json()

# print(weather_data)


# In[ ]:


# # import json

# # Pretty print the JSON output
# print(json.dumps(weather_data, indent=4))


# In[19]:


# Create a function to convert temperature from Kelvin to Celsius

def kelvin_to_celsius(temp_k):
    return temp_k - 273.15


# In[25]:


# To check how many forecast records should be returned

print(len(data['list']))  


# In[21]:


if response.status_code == 200:
    data = response.json()
    weather_data = []

    # Iterate over the 'list' of forecast records
    for forecast in data['list']:
        date_time = datetime.utcfromtimestamp(forecast['dt']).strftime('%Y-%m-%d %H:%M:%S')
        temp = kelvin_to_celsius(forecast['main']['temp'])
        feels_like = kelvin_to_celsius(forecast['main']['feels_like'])
        pressure = forecast['main']['pressure']
        humidity = forecast['main']['humidity']
        weather_main = forecast['weather'][0]['main']
        weather_description = forecast['weather'][0]['description']
        wind_speed = forecast['wind']['speed']
        wind_direction = forecast['wind']['deg']
        cloudiness = forecast['clouds']['all']
        rain_volume = forecast.get('rain', {}).get('3h', 0)
        snow_volume = forecast.get('snow', {}).get('3h', 0)

        # Append the weather data for each forecast
        weather_data.append({
            "DateTime": date_time,
            "Temperature": temp,
            "Feels Like_temp": feels_like,
            "Pressure(hPa)": pressure,
            "Humidity_percent": humidity,
            "Weather": weather_main,
            "Weather Description": weather_description,
            "Wind Speed": wind_speed,
            "Wind Direction": wind_direction,
            "Cloudiness": cloudiness,
            "Rain Volume(mm)": rain_volume,
            "Snow Volume(mm)": snow_volume
        })

    # Create DataFrame with the weather data
    df = pd.DataFrame(weather_data)
    # Display the DataFrame (this will now show all rows)
    
else:
    print(f"Failed to get data: {response.status_code}")

df.head(len(df))



# In[ ]:


## Convert ".ipynb" to ".py"

get_ipython().system('jupyter nbconvert --to script weather_api_etl_pipeline_postgresql.ipynb')


# In[29]:


df.info()


# #### Trend Plot (Tempearature Trend Over Time)

# In[65]:


df['DateTime'] = pd.to_datetime(df['DateTime'])

# Calculate the average temperature
average_temp = df['Temperature'].mean()

# Create the plot
plt.figure(figsize=(14, 6))
plt.plot(df['DateTime'], df['Temperature'], marker='o', linestyle='-', color='b', label='Temperature')

# Add the red line for the average temperature
plt.axhline(y=average_temp, color='r', linestyle='--', label=f'Avg Temp: {average_temp:.2f}°C')

# Add labels and title
plt.title('Temperature Trend Over Time with Average', fontsize=16)
plt.xlabel('DateTime', fontsize=10)
plt.ylabel('Temperature (°C)', fontsize=12)
plt.xticks(rotation=45)

# Add a legend
plt.legend()

# Display the plot
plt.grid(True)
plt.show()


# In[45]:


## For an interactive plot we can use Plotly


# In[57]:


import plotly.express as px
import plotly.graph_objects as go

# Calculate the average temperature
average_temp = df['Temperature'].mean()

# Create an interactive line plot with Plotly
fig = px.line(df, x='DateTime', y='Temperature', title='Temperature Trend Over Time')

# Add a red line for the average temperature
fig.add_shape(
    type='line',
    x0=df['DateTime'].min(),
    x1=df['DateTime'].max(),
    y0=average_temp,
    y1=average_temp,
    line=dict(color='red', dash='dash'),
)

# Add annotation for the average temperature
fig.add_annotation(
    x=df['DateTime'].max(),  # Position the annotation on the right
    y=average_temp,
    text=f"Avg Temp: {average_temp:.2f}°C",
    showarrow=False,
    font=dict(color='red')
)

# Customize the layout for better visibility
fig.update_layout(xaxis_title='DateTime', yaxis_title='Temperature (°C)', xaxis_tickangle=-45)

# Show the plot
fig.show()


# In[87]:


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Assume df already has the 'DateTime' and 'Temperature' columns
df['DateTime'] = pd.to_datetime(df['DateTime'])

# Create new columns for the day and hour from 'DateTime'
df['Day'] = df['DateTime'].dt.date  # Extract date
df['Hour'] = df['DateTime'].dt.hour  # Extract hour

# Pivot the DataFrame to create a matrix of days and hours
temp_pivot = df.pivot(index='Day', columns='Hour', values='Temperature')

# Set the size of the heatmap
plt.figure(figsize=(12, 3))

# Create the heatmap
sns.heatmap(temp_pivot, cmap='YlGnBu', annot=True, fmt='.1f', linewidths=.5, cbar_kws={'label': 'Temperature (°C)'})

# Add labels and a title
plt.title('Temperature Heatmap (3-Hour Intervals)', fontsize=16)
plt.xlabel('Hour of Day', fontsize=10)
plt.ylabel('Date', fontsize=10)

# Show the heatmap
plt.show()


# #### Loading data to PostgreSQL

# In[92]:


get_ipython().system('pip install psycopg2')


# In[172]:


# Importing necessary libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# In[180]:


# Function to load the API key and DB credentials from config.txt
def load_config(filepath):
    config = {}
    with open(filepath, 'r') as file:
        for line in file:
            if line.strip():  # Ignore empty lines
                key, value = line.strip().split('=')
                config[key] = value
    return config

# Load the configuration (API key and DB credentials)
config = load_config("config.txt")

# Extracting the configuration
db_user = config['DB_USER']
db_password = config['DB_PASSWORD']
db_host = config['DB_HOST']
db_port = config['DB_PORT']
db_name = config['DB_NAME']

# Create a connection string
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')


# In[182]:


# Create a table name
table_name = 'weather_data'

# Load the data into the database
df.to_sql(table_name, engine, if_exists='replace', index=False)

# # Close the connection
# engine.dispose()

print("Data loaded into PostgreSQL successfully!")


# In[ ]:




