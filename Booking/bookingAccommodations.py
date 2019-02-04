import selenium
import json
import time
import re
import string
import requests
import bs4
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

domain =  'https://www.booking.com'

def prepare_driver(url):
    '''Returns a Firefox Webdriver.'''
    options = Options()
    # options.add_argument('-headless')
    driver = Firefox(executable_path='geckodriver', options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        (By.ID, 'ss')))
    return driver

def fill_form(driver, search_argument):
    '''Finds all the input tags in form and makes a POST requests.'''
    search_field = driver.find_element_by_id('ss')
    search_field.send_keys(search_argument)
    # We look for the search button and click it
    driver.find_element_by_class_name('sb-searchbox__button')\
        .click()
    wait = WebDriverWait(driver, timeout=10).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, 'sr-hotel__title')))

def scrape_results(driver, n_results):
    '''Returns the data from n_results amount of results.'''

    accommodations_urls = list()
    accommodations_data = list()

    for accomodation_title in driver.find_elements_by_class_name('sr-hotel__title'):
        accommodations_urls.append(accomodation_title.find_element_by_class_name(
            'hotel_name_link').get_attribute('href'))

    for url in range(0, n_results):
        if url == n_results:
            break
        url_data = scrape_accommodation_data(driver, accommodations_urls[url])
        accommodations_data.append(url_data)
    
    return accommodations_data

def scrape_accommodation_data(driver, accommodation_url):
    '''Visits an accommodation page and extracts the data.'''

    if driver == None:
        driver = prepare_driver(accommodation_url)

    driver.get(accommodation_url)
    time.sleep(12)

    accommodation_fields = dict()

    # Get the accommodation name
    accommodation_fields['name'] = driver.find_element_by_id('hp_hotel_name')\
        .text.strip('Hotel')

    # Get the accommodation score
    accommodation_fields['score'] = driver.find_element_by_class_name(
        'bui-review-score--end').find_element_by_class_name(
        'bui-review-score__badge').text
    
    # Get the accommodation location
    accommodation_fields['location'] = driver.find_element_by_id('showMap2')\
        .find_element_by_class_name('hp_address_subtitle').text

    # Get the most popular facilities
    accommodation_fields['popular_facilities'] = list()
    facilities = driver.find_element_by_class_name('hp_desc_important_facilities')

    for facility in facilities.find_elements_by_class_name('important_facility'):
        accommodation_fields['popular_facilities'].append(facility.text)
    
    return accommodation_fields

if __name__ == '__main__':

    try:
        driver = prepare_driver(domain)
        fill_form(driver, 'Barcelona')
        accommodations_data = scrape_results(driver, 10)
        accommodations_data = json.dumps(accommodations_data, indent=4)
        with open('booking_data.json', 'w') as f:
            f.write(accommodations_data)
    finally:
        driver.quit()

