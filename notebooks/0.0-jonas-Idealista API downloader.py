import requests
import base64
import pandas as pd
import numpy as np
import json
import os
from dotenv import load_dotenv, find_dotenv
from datetime import datetime

DESTINATION_FOLDER = "./temporal_landing"

def load_credentials():
    load_dotenv()

    # OR, the same with increased verbosity
    load_dotenv(find_dotenv())

    # First we need the client secret and api key

    apikey = os.environ.get("APIKEY")
    client_secret = os.environ.get("CLIENTSECRET")
    print(apikey, client_secret, "AAA")

    # Concatenating the credentials into one variable(string)
    credentials = f"{apikey}:{client_secret}"

    # After we need to encode the api key and client secret
    return base64.b64encode(credentials.encode()).decode()


def get_api_token(encoded_credentials):
    # url where we will request the token
    token_url = "https://api.idealista.com/oauth/token"

    data = {"grant_type": "client_credentials",
            "scope": "read"}

    headers = {"Authorization": f"Basic {encoded_credentials}", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    # getting response
    response = requests.post(token_url, data=data, headers=headers)

    # checking status
    if response.status_code == 200:
        # Parse the JSON response
        token_data = response.json()
        c=token_data["access_token"]
        # Print access token and other details
        print("Access Token:", token_data)
        print("Token Type:", token_data["token_type"])
        print("Expires In (seconds):", token_data["expires_in"])
        print("Scope:", token_data["scope"])
    else:
        print("Error:", response.status_code, response.text)

    return token_data["access_token"]

# Detailing the parameters
base_url = 'https://api.idealista.com/3.5/'  # Base search url
country = 'es'     # Search country (es, it, pt)
language = 'es'     # Search language (es, it, pt, en, ca)
max_items = '50'     # Max items per call, the maximum set by Idealista is 50
operation = 'rent'     # Kind of operation (sale, rent)
property_type = 'homes'     # Type of property (homes, offices, premises, garages, bedrooms)
order = 'priceDown'     # Order of the listings, consult documentation for all the available orders
center = '41.3851,2.1734'     # Coordinates of the search center
distance = '90000'     # Max distance from the center
sort = 'desc'     # How to sort the found items
maxprice = '100000'     # Max price of the listings

# Creating the URL with the parameteres I want 

def define_search_url():
    url = (base_url +
           country +
           '/search?operation=' + operation +
           '&maxItems=' + max_items +
           '&order=' + order +
           '&center=' + center +
           '&distance=' + distance +
           '&propertyType=' + property_type +
           '&sort=' + sort +
           '&numPage=%s' +
           '&maxPrice=' + maxprice +
           '&language=' + language)
    return url


def search_api(url, access_token):
    url = define_search_url()

    headers = {'Content-Type': "application/json",   # Define the search headers
               'Authorization' : 'Bearer ' + access_token}

    content = requests.post(url, headers = headers)   # Return the content from the request

    result = json.loads(content.text)   # Transform the result as a json file

    return result


def results_to_df(results):
    df = pd.DataFrame.from_dict(results['elementList'])
    return df

def concat_df(df, df_tot):
    df_tot = pd.concat([df_tot, df])
    return df_tot

def df_to_json(df, file_path):
    df = df.reset_index()  # Reset the index in order to organize the records
    df.to_json(file_path, orient='records', indent=4)  # Save it as a JSON file

def store_api_df(pages, url, access_token):
    df_tot = pd.DataFrame()
    for i in range(1, pages + 1):
        url_copy = url  # Make a copy of the original URL
        url_copy = url_copy % (i)  # Add the pagination to the copied URL
        results = search_api(url_copy, access_token)  # Get the search results
        df = results_to_df(results)  # Save the results as a dataframe
        df_tot = concat_df(df, df_tot)  # Concatenate the results to the main dataframe

    # Get the current date in 'YYYY_MM_DD' format
    current_date = datetime.now().strftime("%Y_%m_%d")

    # Generate the JSON file name with the date
    json_file_path = f"{DESTINATION_FOLDER}/{current_date}_idealista.json"
    df_to_json(df_tot, json_file_path)


if __name__ == "__main__":
    credentials = load_credentials()
    access_token = get_api_token(credentials)
    
    url = define_search_url()

    pagination = 1
    first_search_url = url %(pagination)
    store_api_df(1, url, access_token)