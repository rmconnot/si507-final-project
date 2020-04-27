#################################
##### Name: Robert Connot
##### Uniqname: rmconnot
#################################

from bs4 import BeautifulSoup
import requests
import json

params = {'t' : 'breaking bad', 'type' : 'series'}

response = requests.get('http://www.omdbapi.com/?apikey=a497baba&', params = params)

result = response.json()

print(result)