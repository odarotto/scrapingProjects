import requests
import dateparser
import argparse
import json
import urllib
import time
import pickle
import os
import pandas as pd
import re
import string
import boto3
import ssl
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
from bs4 import BeautifulSoup
from selenium import webdriver
from tendo import singleton
from http.client import RemoteDisconnected
from OpenSSL.SSL import WantReadError
# To add logging to the code
import urllib3
import logging
from splunk_hec_handler import SplunkHecHandler
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
# from exceptions import *
# -----------------------------------------------------------------------------
domain = 'https://www.rubmaps.com'
client = boto3.client('s3')
bucket = 'ti-merida'
prefix = 'sites/rm/' # Change for the new page folder
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger('SplunkHECHandler')
logger.setLevel(logging.DEBUG)
MAX_RETRIES = 300
current_state = ''
current_page = 1
splunk_host = "logging.threatinformantlabs.com"
splunk_port = 8088
splunk_token = "cd672cae-5f9c-4d07-854e-d9d650f7b147"
splunk_handler = SplunkHecHandler(splunk_host,
                    splunk_token,
                    port=8088, proto='https', ssl_verify=False,
                    source="merida")
logger.addHandler(splunk_handler)

global scraped_places
# -----------------------------------------------------------------------------

def get_html_from_request(url):
    '''Returs the html from the url. It uses proxies for every new url'''
    proxyDict = {
        'http':'http://merida:snOQUFB0eQYLtIb4X@69.197.144.122:52027',
        'https':'http://merida:snOQUFB0eQYLtIb4X@69.197.144.122:52027'
    }
    headers = {
        'user-agent':
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)\
        Chrome/60.0.3112.90 Safari/537.36'
    }

    session = requests_retry_session(retries=MAX_RETRIES)
    value_connection = False
    while not value_connection:
        try:
            r = session.get(url, headers=headers, proxies=proxyDict,
                verify=False)
            # r = session.get(url, headers=headers, verify=False)
            # r_ip = session.get('https://api.ipify.org/?format=json', 
            r_ip = session.get('https://api.ipify.org/?format=json', 
                proxies=proxyDict,
                verify=False)
            value_connection = True
        except RemoteDisconnected:
            print('Retrying {}'.format(url))
            pass
    ip = json.loads(r_ip.text)
    ip['type'] = 'public_ip'
    # logger.info(ip)
    print(ip)
    message = '[!] Getting {}'.format(url)
    # logger.info(message)
    print(message)
    time.sleep(1)
    return r.content

def requests_retry_session(retries=3,
    backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    '''Creates a session and configures it for retry requests.'''
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_states(url):
    '''Scrapes the main page from domain and returns all the states\' links.
    It returns the states in a dictionary of the form state:{city:url}'''
    states = dict()
    main_bs = BeautifulSoup(get_html_from_request(url), 'lxml')
    states_list = main_bs.find('ul', {'id':'states'}).find_all('li',
        {'class':'item-mainstate-li'})

    for item_mainstate in states_list:
        main_state = item_mainstate.find('a', {'class':'item-mainstate'})\
            .get_text()
        # find_all() function returns a list (ResultSet)
        dn_list = item_mainstate.find_all('ul', {'class':'dn'})
        if len(dn_list) > 1:
            li_list = [li for ul in dn_list for li in ul.find_all('li')]
            dn_list = li_list
        else:
            dn_list = dn_list[0].find_all('li')
        cities = {li.find('a').get_text(): \
            li.find('a').get('href') for li in dn_list}
        states[main_state] = cities
    return states

def crawl_states(states):
    '''Crawls the states dictionary and each city\'s link.'''
    for state in states.keys():
        for relative_url in states[state].values():
            city_url = domain + relative_url
            city_bs = BeautifulSoup(get_html_from_request(city_url), 'lxml')
            places_links = get_places_links(city_bs, city_url)
            scrape_city_places(places_links)

def scrape_city_places(places_list):
    '''Scrape the data from the list of places in the city.'''
    for place in places_list:
        if place.get('href') in scraped_places:
            continue
        scraped = False
        while not scraped:
            try:
                # print(scrape_place_data(place))
                logger.info(scrape_place_data(place))
                scraped_places.add(place.get('href'))
                scraped = True
            except Exception as ex:
                scraped = False
                print(ex)

def scrape_place_data(place):
    '''Scrapes the needed data from the current place.'''
    fields = dict() # Will contain all the data from current place
    place_url = domain + place.get('href')
    place_bs = BeautifulSoup(get_html_from_request(place_url), 'lxml')

    # Get place name, state and n_reviews
    name = place_bs.find('h1', {'class':'titlu'}).get_text()
    fields['place_name'] = name.split('in')[0]
    fields['state'] = name.split('in')[-1].split(',')[0]
    fields['city'] = name.split('in')[-1].split(',')[-1]
    fields['n_reviews'] = place_bs.find('a', {'class':'l_nr_reviews'})\
        .get_text()
    
    # Get all the data in loc-right-tabl div
    table = place_bs.find('div', {'id':'loc-right-tabl'}).find('table')
    for td in table.find_all('td'): 
        td_fields = [children for children in td.children 
            if children not in td.find_all('span')
            and children != '\n' and isinstance(children, str)]
        td_values = [span.get_text() for span in td.find_all('span')
            if span.get('id') != 'goTo']
        for k, v in zip(td_fields, td_values):
            k, v = clean_fields(k, v)
            if k == 'email':
                continue
            fields[k] = v

    # Get the address in loc-left-left
    address = place_bs.find('div', {'class':'loc-left-left'}).get_text()\
        .split('\n')
    address = [a for a in address if bool(re.search(r'\d', a))]
    fields['address'] = ' '.join(address)

    # Get the phone number in loc-left-right
    phone = place_bs.find('div', {'class':'loc-left-right'}).find('span',
        {'id':'qe_phone_number'}).find('span', {'class':'m_hide'}).get_text()
    fields['phone'] = phone

    # Get the hours the place is open, located in loc-hours-operation-item
    try:
        opened = place_bs.find('div', {'class':'loc-hours-operation-item'}).find(
            'div', {'class':'lhop-name'}).get_text()
        fields['opened'] = opened
    except AttributeError:
        fields['opened'] = 'unknown'

    try:
        hours = place_bs.find('div', {'class':'loc-hours-operation-item'}).find(
            'div', {'class':'lhop-interval'}).get_text()
        fields['hours'] = hours
    except AttributeError:
        fields['hours'] = 'unknown'
    
    # Get the email --------------NEEDS REVIEW --------------------------------
    has_email = bool(place_bs.find('span', {'id':'qe_email'}))
    if has_email:
        email_span = place_bs.find('div', {'id':'loc-right-tabl'})\
            .find('span', {'id':'qe_email'})
        fields['email'] = email_span.get_text()

    # Get the thumbnail image
    has_image = bool(place_bs.find('a', {'id':'photo_premium_click'}))
    if has_image:
        thumbnail = place_bs.find('a', {'id':'photo_premium_click'}).find(
            'img').get('src')
        fields['image'] = thumbnail

    fields = clean_data(fields)
    save_place_data(fields, place_url)
    return fields

def write_place_to_s3(place_phone, body):
    '''Writes the profile.json file to S3 folder.'''
    value_connection = False
    while not value_connection:
        try:
            s3key = prefix + 'places/'+place_phone+'/place.json'
            client.put_object(
                Bucket=bucket,
                Key=s3key,
                Body=json.dumps(body))
            value_connection = True
        except RemoteDisconnected:
            pass

def write_HTML_to_s3(place_phone, body):
    '''Writes the html profile to S3.'''
    value_connection = False
    while not value_connection:
        try:
            s3key = prefix + 'places/'+place_phone+'/'+'place.html'
            client.put_object(
                Bucket=bucket,
                Key=s3key,
                Body=body)
            value_connection = True
        except RemoteDisconnected:
            pass

def configure_scraped_places_list(filename):
    '''Creates a profile file that tracks the scraped profiles.
    If the file exist, it will initialize, if not, it will create it
    and initialize it.'''
    scraped_places = None
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            logger.info('[!] Places file created.')
            # print('[!] Places file created.')
        scraped_places = set()
    else:
        with open(filename, 'r') as f:
            logger.info('[!] Places file loaded.')
            # print('[!] Places file loaded.')
            scraped_places = json.JSONDecoder().decode(f.read())
    return scraped_places

def save_context(places=None):
    '''Saves the scraped profiles in a .json file.'''
    if places is not None:
        try:
            if len(places) == 0:
                return
            places = json.dumps(list(places), indent=4)
            with open('scraped_places.json', 'w') as f:
                f.write(places)
        except TypeError:
            print('Profiles not saved.')
        except Exception as ex:
            print(ex)

def save_place_data(place_fields, place_url):
    '''Creates a json file with the place dictionary and saves it to S3.'''
    write_place_to_s3(place_fields['phone'], place_fields)
    write_HTML_to_s3(place_fields['phone'], requests.get(place_url).content)

def clean_fields(key, value):
    '''Returns a key:value pair formatted.'''
    key = key.lower().strip(' \n\t'+string.punctuation).replace(' ', '_')

    if key == 'address':
        return key, value

    if not isinstance(value, list):
        value = value.strip(string.whitespace+'-\n\t\r').lower()

    if ',' in value:
        value_list= list()
        for v in value.split(','):
            value_list.append(v.lower().strip(' '))
        value = value_list

    return key, value

def clean_data(fields):
    '''Returns a place record formatted.'''
    cleaned_fields = dict()
    for field in fields.keys():
        k, v = clean_fields(field, fields[field])
        cleaned_fields[k] = v
    return cleaned_fields

def get_places_links(city_bs, city_url):
    '''Returns a list with the links for each profile in the city.'''
    more_than_one_page = bool(city_bs.find('div', {'class':'pagination'}))
    places_links = []

    if more_than_one_page:
        pagination = city_bs.find('div', {'class':'pagination'})
        pages = [a.get_text() for a in pagination.find_all('a') 
            if a.get_text().isnumeric()]
        for page in range(1, int(pages[-1])+1):
            if page == 1:
                container = city_bs.find('div', {'id':'container'})
                places_links += extract_places_links_from_container(container)
            else:
                page_url = city_url + '-' + str(page)
                page_bs = BeautifulSoup(get_html_from_request(page_url), 'lxml')
                container = page_bs.find('div', {'id':'container'})
                places_links += extract_places_links_from_container(container)
    else:
        container = city_bs.find('div', {'id':'container'})
        count = int(container.find('h1').get_text().split('-')[-1])
        places_links = extract_places_links_from_container(container, count)
    
    return places_links

def extract_places_links_from_container(container, count=None):
    '''Extracts all the places links in the div container'''
    links = []
    i = 1
    if count:
        next_link = container.find_next('a', {'class':'th-a'})
        while next_link and i<=count:
            links.append(next_link)
            i += 1
            next_link = next_link.find_next('a', {'class':'th-a'})
    else:
        links = container.find_all('a', {'class':'th-a'})
    return links

if __name__ == '__main__':

    scraped_places = set()
    scraped_places = set(configure_scraped_places_list('scraped_places.json'))

    scraped = False
    while not scraped:
        try:
            states_links = get_states(domain)
            crawl_states(states_links)
        except KeyboardInterrupt:
            scraped = True
            save_context(scraped_places)
        except Exception:
            scraped = True
            save_context(scraped_places)