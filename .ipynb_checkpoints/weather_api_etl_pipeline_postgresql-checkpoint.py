#!/usr/bin/env python
# coding: utf-8

# ### Pipeline 1: Data from Web APIs to a Data Warehouse

# Use Case: Extract data from a public API, transform it, and load it into a data warehouse for analysis (e.g., weather data, financial data).
# 

# #### Importing necessary libraries

# In[50]:


import json
import requests
import pandas as pd
from datetime import datetime
from matplotlib import pyplot as plt
import seaborn as sns


# In[122]:


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

print(weather_data)


# In[ ]:


# import json

# # Pretty print the JSON output
# print(json.dumps(weather_data, indent=4))


# In[123]:


# Create a function to convert temperature from Kelvin to Celsius

def kelvin_to_celsius(temp_k):
    return temp_k - 273.15


# In[128]:


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
    # print(df.head())

else:
    print(f"Failed to get data: {response.status_code}")

df.head()


# In[130]:


print(len(data['list']))  # Check how many forecast records are being returned


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




