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
from exceptions import *
# -------------------- Remove the 'FOR TESTING' lines--------------------------
# -----------------------------------------------------------------------------
domain = 'https://www.eroticmonkey.ch'
client = boto3.client('s3')
bucket = 'ti-merida'
prefix = 'sites/em/'
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
# -----------------------------------------------------------------------------

def configure_scraped_profiles_list(filename):
    '''Creates a profile file that tracks the scraped profiles.
    If the file exist, it will initialize, if not, it will create it
    and initialize it.'''
    scraped_profiles = None
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            logger.info('[!] Profiles file created.')
        scraped_profiles = list()
    else:
        with open(filename, 'r') as f:
            logger.info('[!] Profiles file loaded.')
            scraped_profiles = json.JSONDecoder().decode(f.read())
    return scraped_profiles

def configure_context(filename):
    '''Gets the saved context from file.'''
    context = None
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            logger.info('[!] Scraped states file created.')
        context = dict()
    else:
        with open(filename, 'r') as f:
            logger.info('[!] Scraped states file loaded.')
            context = json.JSONDecoder().decode(f.read())
    return context

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
            r_ip = session.get('https://api.ipify.org/?format=json', 
                proxies=proxyDict, verify=False)
            value_connection = True
        except RemoteDisconnected:
            print('Retrying {}'.format(url))
            pass
    ip = json.loads(r_ip.text)
    ip['type'] = 'public_ip'
    logger.info(ip)
    message = '[!] Getting {}'.format(url)
    logger.info(message)
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

def get_cities(url, args):
    '''Scrape the cities links from the main page'''
    cities_limit = 0
    if args['states'] == [-1]:
        cities_limit = []
    else:
        cities_limit = args['states']

    city_links = dict()
    r = get_html_from_request(url)
    main_page = BeautifulSoup(r, 'lxml')
    left_menu = main_page.find('div', {'id':'content_left_menu'})\
        .find('ul').find('li').find('ul')
    
    if left_menu != None:
        for city in left_menu.find_all('li'):
            if isinstance(cities_limit, int) and len(city_links) == cities_limit:
                break
            link = city.find('a')
            name = re.sub('[^a-zA-z]', '', link.get_text())
            city_links[name] = link
    else:
        print('Not found')
    
    logger.info('[!] Cities links scraped')
    return city_links

def get_profiles_in_cities(cities_links, args,
                            profiles,
                            state=None,
                            page=None,
                            update=False):
    '''Scrape the profile links from each city.
    If --update=True it sorts the profiles by newest '''
    profiles_links = dict()
    global current_state
    global current_page
    if state == '' and page == 1:
        saved_state = sorted(list(cities_links.keys()))[0]
        saved_page = 1
    else:
        saved_state = state
        saved_page = page
    # Iterate over all the  cities
    sorted_cities = list(cities_links.keys())
    sorted_cities = sorted(sorted_cities)
    use_saved_state = True
    use_saved_page = True
    for city_name in sorted_cities:
        if saved_state != city_name and use_saved_state:
            continue
        current_state = city_name
        pages = 0
        pages_limit = 0
        if args['pages'] == [-1]:
            pages_limit = []
        else:
            pages_limit = args['pages']
        # All the profile links from the current city
        if update:
            profiles_links[city_name] = set()
            city_url = domain + cities_links[current_state].get('href')\
                + '#sortby=new_esc'
        else:
            city_url = domain + cities_links[current_state].get('href')
        r = get_html_from_request(city_url)
        bs = BeautifulSoup(r, 'lxml')
        pagination = bs.find('div', {'class':'reviews_pagination'})\
            .find('ul')
        
        # Find the last index to know how many pages
        # the city have
        last_pagination = pagination.find_all('li')[-2]
        pages = int(last_pagination.get_text())
        links = list()
        logger.info('Scraping {}. {} pages'.format(city_name, pages))
        if use_saved_page:
            start_page = saved_page
        else:
            start_page = 1
        for page in range(start_page, pages+1):
            current_page = page
            time.sleep(1)
            page_url = ''
            # Limit
            if isinstance(pages_limit, int) and page == pages_limit:
                break
            # Limit
            if page == 1:
                page_url = city_url 
            else:
                if update:
                    page_url = '{0}-{page}#{1}'.format(city_url.split('#')[0],
                        city_url.split('#')[-1], page=page)
                else:
                    page_url = city_url + '-' + str(page)
            r = get_html_from_request(page_url)
            bs = BeautifulSoup(r, 'lxml')
            profiles_list = bs.find('div', {'id':'reviews_list'})
            links = scrape_links_from_list(profiles_list, page_url)
            # try:
            links_set = remove_scraped_profiles(links, update, profiles)
            scrape_list_profile_data(links_set, cities_links[city_name],
                profiles)
        logger.info('[!] {} state completely scraped.'.format(city_name))
        use_saved_state = False
        use_saved_page = False
    return profiles_links

def remove_scraped_profiles(links, update, profiles):
    '''Returns a list of links that hasn't been scraped.'''
    new_links = links.copy()
    links.clear()
    for l in new_links:
        if l in profiles and update:
            continue
        elif l in profiles:
            continue
        else:
            links.append(l)
    return links

def upgrade_scraped_profiles(profiles):
    '''Iters over profiles list and upgrades all the profiles that
    shows changes on its number of reviews.'''
    # global profiles
    for profile in profiles:
        url = profile
        profile_fields = scrape_profile_data(url)
        writed_fields = get_profile_from_s3(profile_fields['emid'])
        if same_profile(profile_fields, writed_fields):
            continue
        else:
            upgrade_profile(profile_fields)
            update_message = '[!] Profile {} updated.'\
                .format(profile_fields['emid'])
            logger.info(update_message)

def upgrade_profile(fields):
    '''Upgrade all the data in the profile if the number of reviews differs.'''
    write_profile_to_s3(fields['emid'], fields)

def same_profile(new_profile, old_profile):
    '''Returns True if all the data in profile is the same in 
    scraped profile.'''
    for k in new_profile.keys():
        if new_profile[k] == old_profile[k]:
            continue
        else:
            return False
    return True

def scrape_profile_data(url, return_images=False):
    '''Scrapes a single profile data.'''
    # Slows down the process between profile scraping
    # time.sleep(1)
    fields = {}
    images_links = []
    n_images = 0
    image_counter_text = ''
    r = get_html_from_request(domain+'/'+url) 
    bs = BeautifulSoup(r, 'lxml')

    # We count the number of images in the thumbnail
    try:
        image_counter_text = bs.find('div', \
            {'class':'model-box-photo-counter'}).get_text()
        n_images = int(re.sub('[^0-9]', '', image_counter_text))
    except AttributeError:
        pass
    profile_bs = BeautifulSoup(r, 'lxml')
    fields['page_url'] = domain+'/'+url

    if n_images > 1:
        escort_images_links = profile_bs.find('div', \
            {'class':'page_col_container'}).find('div', \
            {'class':'escort_with_images'}).find('div', \
            {'class':'escort_photos'}).find_all('a')
        
        for image in escort_images_links:
            main_link = image.get('href')
            main_link = str(main_link).split('main')
            small_link = main_link[0] + 'small' + main_link[-1]
            images_links.append(small_link)
    else:
        # Just one image, save it has a list with one element
        try:
            main_link = str(profile_bs.find('a', \
                {'class':'escort_photos_view'}).get('href')).split('main')
            small_link = main_link[0] + 'small' + main_link[-1]
            images_links = [small_link]
        except AttributeError:
            images_links = []
    
    # Last seen online
    escort_thumbnail = profile_bs.find('div', 
        {'class':'width_25 escort_with_images'})
    fields['last_seen'] = get_last_seen(escort_thumbnail)

    # Escort title
    try:
        fields['emid'] = profile_bs.find('p', {'class':'emid'})\
            .get_text()
        fields['emid'] = re.sub('[^0-9]', '' , fields['emid'])
        review_string = profile_bs.find('div', {'class':'er_header'}).\
        find('div', {'class':'width_66'}).find('h3').get_text()
        fields['n_reviews'] = re.sub('[^0-9]', '', review_string)
    except AttributeError:
        fields['n_reviews'] = None
    fields['escort_name'] = profile_bs.find('span', \
        {'id':'qe_name'}).get_text()

    # Escort info
    for div in profile_bs.find('div', \
        {'class':'escort_info'}).find_all('div', {'class':'width_50'}):
        for li in div.find('ul').find_all('li'):
            key = li.find('b').get_text().strip(string.punctuation+
                string.whitespace).lower()
            value = li.get_text()
            fields[key] = value
    
    # Check if profiles contains incall or outcall rates
    has_incall = False
    has_outcall = False
    try:
        has_incall = bool(profile_bs.find('h3',
            string=re.compile('Incall Rates:')))
        has_outcall = bool(profile_bs.find('h3',
            string=re.compile('Outcall Rates:')))
    except Exception:
        pass
    
    # Escort details
    for div in profile_bs.find_all('div', \
        {'class':'escort_details'}):
        for width in div.find_all('div', {'class':'width_50'}):
            if has_incall and bool(width.find('h3')):
                fields['incall_rates'], has_incall = \
                    get_incall_rates(width, has_incall)
            elif has_outcall and bool(width.find('h3')):
                fields['outcall_rates'], has_outcall = \
                    get_outcall_rates(width, has_outcall)
            else:
                for li in width.find('ul').find_all('li'):
                    key = li.find('b').get_text().strip(string.punctuation+ 
                        string.whitespace).lower()
                    value = li.get_text()
                    fields[key] = value
    
    # Quick notes
    fields['quick_notes'] = get_quick_notes(profile_bs)

    if return_images:
        return clean_data(fields), images_links, r
    else:
        return clean_data(fields)

def get_last_seen(thumbnail):
    '''Return the value of the last seen if it exists.'''
    last_seen = ''
    try:
        last_seen_text = thumbnail.find('p', {'class':'lastseenonline'})\
            .get_text()
        last_seen = last_seen_text.split(':')[-1].strip(' ')
    except Exception:
        return 'unknown'
    return last_seen

def get_escort_details(profile_bs, fields):
    '''Returns the profile\'s details section.''' 
    for div in profile_bs.find_all('div', \
        {'class':'escort_details'}):
        for width in div.find_all('div', {'class':'width_50'}):
            for li in width.find('ul').find_all('li'):
                key = li.find('b').get_text().strip(string.punctuation+ 
                    string.whitespace).lower()
                value = li.get_text()
                fields[key] = value

def get_quick_notes(profile_bs):
    '''Return the profile\'s quick notes, none if empty.'''
    try:
        notes_container = profile_bs.find('h3', string=re.compile(
            '([A-Z])\w+ Notes:'))
        notes = notes_container.find_next_sibling('div', {'class':'width_100'})\
            .find('p', {'class':'form-control'})
        return notes.get_text()
    except AttributeError:
        return None
    except Exception:
        return None

def get_outcall_rates(outcall_list, has_outcall):
    '''Returns the outcall rates as a list.'''
    has_outcall = False
    out_rates = dict()
    for li in outcall_list.find('ul').find_all('li'):
        key = li.find('b').get_text().strip(string.punctuation+ 
            string.whitespace).lower()
        value = li.get_text().split(':')[-1].strip(' ')
        new_k, new_v = clean_field(key, value)
        out_rates[new_k] = new_v
    return out_rates, has_outcall

def get_incall_rates(incall_list, has_incall):
    '''Returns the incall rates as a list.'''
    has_incall = False
    in_rates = dict()
    for li in incall_list.find('ul').find_all('li'):
        key = li.find('b').get_text().strip(string.punctuation+ 
            string.whitespace).lower()
        value = li.get_text().split(':')[-1].strip(' ')
        new_k, new_v = clean_field(key, value)
        in_rates[new_k] = new_v
    return in_rates, has_incall

def scrape_links_from_list(reviews_list, url):
    '''Scrape the lins from the review_list bs object'''
    links = set()
    list_founded = False
    if reviews_list == None:
        list_founded = False
        while not list_founded:
            time.sleep(2)
            r = get_html_from_request(url)
            reviews_list = BeautifulSoup(r, 'lxml')\
                .find('div', {'id':'reviews_list'})
            list_founded = True
    else:
        list_founded = True
        for item in reviews_list.find_all('div', {'class':'reviews_list_item'}):
            links.add(item.find('a').get('href'))
    return list(links)

def scrape_list_profile_data(url_lists, city_name, profiles):
    '''Scrape the data from each profile in the url_list parameter.'''
    for url in url_lists:
        if url in profiles:
            continue
        fields, images_links, r = scrape_profile_data(url, return_images=True)
        # clean_fields = clean_data(fields)
        save_profile(url, profiles)
        prepare_data(fields, images_links, r)

def clean_data(fields):
    '''Cleans each profile\'s dataset.'''
    fields = {k: v for k, v in fields.items() if k != ''}
    clean_fields = dict()
    for k, v in fields.items():
        k_new, v_new = clean_field(k, v)
        clean_fields[k_new] = v_new
    return clean_fields

def clean_field(key, value):
    '''Clean the key and value from current field'''

    if key is 'incall_rates' or key is 'outcall_rates':
        return key, value

    if key is 'last_seen':
        if value is not 'unknown':
            date = dateparser.parse(value)
            value =  '{}-{}'.format(date.year, date.month)
        else:
            return key, value

    if value == None:
        key = key.replace(' ', '_').replace('.', '_')
        return key, ''
    key = key.replace(' ', '_').replace('.', '_')
    
    if key is not 'page_url':
        value = value.split(':')[-1].lstrip(string.whitespace).\
            strip(string.whitespace)
    value = value.replace('\t', '').lower()

    # Arrange the services into a list if there's more than one
    if key == 'services':
        value = value.replace(string.whitespace, '')
        value = value.split(',')
        if isinstance(value, list):
            for i in range(0, len(value)):
                value[i] = value[i].replace(' ', '')
        if isinstance(value, list) and len(value) == 1:
            value = ''.join(value)
    
    # Converts the height to inches and formats it
    if key == 'height':
        f_i = value.replace('\'', '|').replace('"', '').split('-')
        heights = []
        for measure in f_i:
            measure = measure.split('|')
            try:
                feets = int(measure[0]) * 12
            except ValueError:
                value = ''
                break
            if measure[-1] == '':
                inches = 0
            else:
                inches = int(measure[-1])
            heights.append(str(feets+inches))
        value = '-'.join(heights)
    if isinstance(value, str) and 'other' in value.lower():
        value = value.strip(' ').lower()

    return key, value

def remove_empty_items(fields):
    '''Removes the : characters (empty key:value)'''
    if '' in fields:
        del fields[""]

def save_profile(url, profiles):
    '''Saves the profile url into the profiles file'''
    profiles.append(url)

def save_context(profiles=None, context=None):
    '''Saves the scraped profiles in a .txt file.
    Also saves the context(current state and page) in another .txt file.'''
    # Save profiles
    if profiles is not None:
        try:
            if len(profiles) == 0:
                return
            profiles = json.dumps(profiles, indent=4)
            with open('scraped_profiles.json', 'w') as f:
                f.write(profiles)
        except TypeError:
            print('Profile not saved.')
    
    # Save context
    if context is not None:
        try:
            context = json.dumps(context, indent=4)
            with open('context.json', 'w') as f:
                f.write(context)
        except TypeError:
            print('Context not saved.')

def save_entry(filename, dataset, mode='w'):
    '''Saves the current entry in the data.json file'''
    with open(filename, mode) as outfile:
        json.dump(dataset, outfile)
        outfile.write(',\n')

def save_html(request, profile_name, emid, path):
    '''Saves the raw html'''
    filename = path + '/' + 'profile.html'
    logger.info('[!] Saving HTML file')
    write_HTML_to_s3(emid, request, profile_name)

def prepare_data(fields, images_links, r):
    '''Create the directories in S3 for each profile.
    It also writes the profile, data and images to S3.'''
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fields['images_urls'] = images_links
    logger.info(fields)

    write_profile_to_s3(fields['emid'], fields)

    record_path = dir_path+'/'+fields['escort_name']
    # if isinstance(dataset['images_links'], list):
    if len(images_links) > 1:
        logger.info('[!] Saving profile images')
        for image_link in images_links:
            try:
                image_filename = image_link.split('small')[-1]
                image_path = record_path+'/'+image_filename
                write_image_to_s3(image_link, image_path, fields['emid'])
                print('{} saved'.format(image_filename))
            except ValueError:
                print('Could not download image')
        save_html(r, fields['escort_name'], fields['emid'], record_path)
    else:
        try:
            if len(images_links) != 0:
                logger.info('[!] Saving profile images')
                image_link = images_links[0]
                image_filename = list(image_link.split('small'))[-1]
                image_path = record_path+'/'+image_filename
                write_image_to_s3(image_link, image_path, fields['emid'])
                print('{} saved'.format(image_filename))
            else :
                logger.info('[!] Profile without an image.')
        except ValueError:
            print('Could not download image')
        save_html(r, fields['escort_name'], fields['emid'], record_path)

def write_profile_to_s3(em_id,body):
    '''Writes the profile.json file to S3 folder.'''
    value_connection = False
    while not value_connection:
        try:
            s3key = prefix + 'profiles/'+em_id+'/profile.json'
            client.put_object(
                Bucket=bucket,
                Key=s3key,
                Body=json.dumps(body))
            value_connection = True
        except RemoteDisconnected:
            pass

def get_profile_from_s3(em_id):
    '''Extracts the profile.json from profile with em_id.'''
    global client
    value_connection = False
    while not value_connection:
        try:
            s3key = prefix + 'profiles/'+em_id+'/profile.json'
            profile_json = client.get_object(
                Bucket=bucket,
                Key=s3key)
            value_connection = True
            file_content = profile_json['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
        except RemoteDisconnected:
            pass
    return json_content

def write_HTML_to_s3(em_id, body, escort_name):
    '''Writes the html profile to S3.'''
    value_connection = False
    while not value_connection:
        try:
            s3key = prefix + 'profiles/'+em_id+'/'+'profile.html'
            client.put_object(
                Bucket=bucket,
                Key=s3key,
                Body=body)
            value_connection = True
        except RemoteDisconnected:
            pass

def write_image_to_s3(url,image, em_id):
    '''Writes all the profile images to its S3 folder.'''
    value_connection = False
    while not value_connection:
        try:
            fname = url.split('/')[-1]
            s3key = prefix + 'profiles/'+em_id+'/'+fname
            with requests.get(url, stream=True) as req:
                client.put_object(
                    Bucket=bucket,
                    Key=s3key,
                    Body=req.content)
            value_connection = True
        except RemoteDisconnected:
            pass

if __name__ == '__main__':
    # Adding an argparser to specify how many states and pages
    # the script should scrape
    parser = argparse.ArgumentParser(description=
        'Scrapes profiles links from the domain')
    parser.add_argument('-s', '--states', metavar='S', type=int,
        nargs='+', default=2,
        help='Number of states to extract links from, -1 for all')
    parser.add_argument('-p', '--pages', metavar='P', type=int,
        nargs='+', default=2,
        help='Number of pages per state to extract links from, -1 for all')
    parser.add_argument('-u', '--update', metavar='U', type=bool,
        nargs='+', default=False,
        help='If True, it looks only for the newest profiles per state.')
    parser.add_argument('-up', '--updateprofiles', metavar='UP', type=bool,
        nargs='+', default=False,
        help='If True, it sorts the profiles per state by newest review.\
        and compares them with the already scraped profiles.')
    
    eromonkey = singleton.SingleInstance()
    args = vars(parser.parse_args())

    # Adding logging to the script

    scraped = False
    updated = False
    update = args['update']
    update_profiles = args['updateprofiles']
    profiles = configure_scraped_profiles_list('scraped_profiles.json')
    context = configure_context('context.json')
    # Configure the global variables
    if len(context) != 0 and not update:
        current_state = context[0] # context[0] contains current_state
        current_page = context[-1] # context[-1] contains current_page
    restarted = False
    while not scraped or updated:
        try:
            if update_profiles:
                upgrade_scraped_profiles(profiles)
                updated = True
            # If scraping or updating process still not completed
            if not scraped or not updated:
                city_links = get_cities(domain, args)
                saved_state = current_state
                saved_page = current_page
                profile_links_by_city = get_profiles_in_cities(city_links, 
                    args, profiles, saved_state, saved_page, update)
                scraped = True
        except RemoteDisconnected:
            message = 'Connection Restarted. Resuming scraping in 10 seconds.'
            logger.debug(message)
            save_context(profiles, (current_state, current_page))
            time.sleep(10)
            restarted = True
        except ConnectionError as ex:
            message = 'Connection Restarted. Resuming scraping in 10 seconds.'
            logger.debug(message)
            save_context(profiles, (current_state, current_page))
            time.sleep(10)
            restarted = True
        except ProtocolError as ex:
            message = 'Connection Restarted. Resuming scraping in 10 seconds.'
            logger.debug(message)
            save_context(profiles, (current_state, current_page))
            time.sleep(10)
            restarted = True
        except KeyboardInterrupt as ex:
            message = 'Scraping stopped. Saving current state.'
            logger.debug(message)
            save_context(profiles, (current_state, current_page))
            scraped = True
        except Exception as ex:
            print(ex)
        finally:
            if scraped:
                print('Finished')
            else:
                pass