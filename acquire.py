import pandas as pd
import requests
from env import get_connection
import os

def get_people():
    
    url = 'https://swapi.dev/api/people/'
    
    response = requests.get(url)
    
    data = response.json()
    
    people_df = pd.DataFrame(data['results'])
    
    while data['next'] != None:
        response = requests.get(data['next'])
        data = response.json()
        people_df = pd.concat([people_df, pd.DataFrame(data['results'])], ignore_index = True)

    return people_df


def get_planets():
    
    url = 'https://swapi.dev/api/planets/'
    
    response = requests.get(url)

    data = response.json()
    
    planets_df = pd.DataFrame(data['results'])

    while data['next'] != None:
        response = requests.get(data['next'])
        data = response.json()
        planets_df = pd.concat([planets_df, pd.DataFrame(data['results'])], ignore_index = True)
        
    return planets_df


def get_starships():
    
    url = 'https://swapi.dev/api/starships/'
    
    response = requests.get(url)

    data = response.json()
    
    starships_df = pd.DataFrame(data['results'])

    while data['next'] != None:
        response = requests.get(data['next'])
        data = response.json()
        starships_df = pd.concat([starships_df, pd.DataFrame(data['results'])], ignore_index = True)

    return starships_df


def make_csvs(a, b, c):
    
    a.to_csv('people.csv')

    b.to_csv('planets.csv')

    c.to_csv('starships.csv')


def combine_dfs(people_df, planets_df, starships_df):

    star_wars = pd.concat([people_df, planets_df, starships_df], ignore_index = True)
    
    return star_wars



def get_germany():

    germany = pd.read_csv('opsd_germany_daily.csv')
    
    return germany
