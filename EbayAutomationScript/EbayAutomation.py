import os
import selenium
import json
import time
import re
import string
import bs4
import argparse
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select


# TODO

# README ----------------------------------------------------------------------
# The input for this script is:
#   - A list of orders like:
#       order1:[product_url1, product_url2, ...]
#       order2:[product_url1, product_url2, ...]
#       order3:[product_url1, product_url2, ...]
#   - A list of cards like:
#       order1:'6518485734083301|01|2023|216'
#       order2:'6518480556248884|09|2024|852'
#       order3:'6518482544551064|01|2022|685'
# The orders and cards need to have the same keys!

domain = 'https://www.ebay.com'

def prepare_chromedriver(url):
    '''Returns a Firefox Webdriver with a custom user-agent.'''
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36\
        (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'
    options = Options()
    options.add_argument('-headless')
    options.add_argument('user-agent={}'.format(user_agent))
    driver = Chrome(executable_path='chromedriver', options=options)
    driver.get(url)

    # Set language to English
    english_language(driver)
    
    print('[!] Chrome WebDriver is ready.')
    return driver

def restart_driver(driver):
    '''Deletes all cookies and cache, returns a new WebDriver.'''
    driver.delete_all_cookies()
    driver.quit()
    driver = prepare_chromedriver(domain)
    return driver

def english_language(driver):
    '''Sets the Ebay language to English.'''
    # Checks if page is in Spanish
    button_text = driver.find_element_by_id('gh-btn').get_attribute('value')
    hovering = ActionChains(driver)

    if button_text == 'Buscar':
        lang_selector = driver.find_element_by_id('gh-eb-Geo-a-default')
        hovering.move_to_element(lang_selector).perform()
        english_option = driver.find_element_by_id('gh-eb-Geo-a-en')
        hovering.move_to_element(english_option).perform()
        english_option.click()
        driver.implicitly_wait(5)
        print('[!] Language set.')
    else:
        print('[!] Language already in English.')

def visit_product_url(driver, url):
    '''Makes the WebDriver to get a product URL and wait until loaded.'''
    print('[!] Getting {}'.format(url))
    driver.get(url)
    driver.implicitly_wait(4)
    print('[!] Page loaded.')

def read_file(filename):
    '''Reads all of the orders from a JSON file.'''
    if not os.path.isfile(filename):
        print('[!] File not found')
        raise FileNotFoundError
    else:
        with open(filename, 'r') as f:
            orders = json.JSONDecoder().decode(f.read())
    return orders

def extract_from_filename(data):
    '''Ask the user for a file name for data.'''
    valid_filename = False
    while not valid_filename:
        try:
            json_data = read_file(
                input('Please enter the name of the file that contains'\
                 ' the {}: '.format(data)))
            valid_filename = True
        except FileNotFoundError:
            print('Invalid Filename.')
        except Exception as ex:
            print(ex)
    return json_data

def place_order(driver, products, card, billing_address):
    '''Places an order, one order can have one or several items.'''
    for product in products:
        add_to_cart(driver, product)
    
    go_to_checkout(driver, card, billing_address)

def go_to_checkout(driver, card, billing_address):
    '''Checkouts the order.'''
    driver.get('https://cart.payments.ebay.com/sc/view')
    # click the gotocheckout button
    driver.implicitly_wait(5)
    driver.find_element_by_class_name('call-to-action').click()
    driver.implicitly_wait(5)

    # click the guest checkout button
    driver.find_element_by_id('gtChk').click()

    # Enter the billing address and phone
    enter_billing_address(driver, billing_address)

    # Selects payment method and enter card info
    enter_card_data(driver, card)

    # Find the CONFIRM AND PAY button and clicks it
    driver.find_element_by_class_name('btn--large').click()

def enter_billing_address(driver, billing_address):
    '''Enters the billing address for the order.'''

    # Used to scroll to element
    actions = ActionChains(driver)


    # Enter address1
    element = driver.find_element_by_id('addressLine1')
    actions.move_to_element(element).perform()
    element.send_keys(billing_address['address1'])
    # Enter address2
    driver.find_element_by_id('addressLine2')\
        .send_keys(billing_address['address2'])
    # Enter name and lastname
    driver.find_element_by_id('firstName').send_keys(billing_address['name'])
    driver.find_element_by_id('lastName').send_keys(billing_address['lastname'])
    # Enter city
    driver.find_element_by_id('city').send_keys(billing_address['city'])
    # Enter state
    select = Select(driver.find_element_by_id('stateOrProvince'))
    # driver.find_element_by_xpath('//select[@id="stateOrProvince"]'\
    #     '/option[contains(text(), '+billing_address['state'].capitalize()+')]')\
    #     .click()
    state = ' '.join([word.capitalize() for word 
        in billing_address['state'].split(' ')])
    select.select_by_visible_text(state)
    # Enter ZIP code
    driver.find_element_by_id('postalCode').send_keys(billing_address['zip'])
    # Enter email and confirm
    driver.find_element_by_id('email').send_keys(billing_address['email'])
    driver.find_element_by_id('emailConfirm').send_keys(billing_address['email'])
    # Enter phone number (required)
    driver.find_element_by_id('phoneNumber').send_keys(billing_address['phone'])
    time.sleep(6)

    # Finds the DONE button and clicks it
    driver.find_element_by_class_name('btn--medium').click()
    time.sleep(6)
    print('[!] Billing address filled.')

def confirm_address(driver):
    '''Just clicks the DONE button again.'''
    driver.find_element_by_class_name('ADD_ADDRESS_SUBMIT')\
        .find_element_by_tag_name('button').click()
    time.sleep(6)

def enter_card_data(driver, card):
    '''Enters the card data. Serial, date and back code.'''
    # Selects the payment method (credit card)

    payment_selected = False

    while not payment_selected:
        try:
            driver.find_element_by_xpath('//input[@value="CC"]').click()
            time.sleep(5)
            payment_selected = True
        except Exception:
            confirm_address(driver)

    card = arrange_card_data(card)

    # Enter card number
    driver.find_element_by_id('cardNumber').send_keys(card['card_number'])
    # Enter expiration date
    date = card['month']+card['year']
    driver.find_element_by_id('cardExpiryDate').send_keys(date)
    # Enter security code
    driver.find_element_by_id('securityCode').send_keys(card['security_code'])
    time.sleep(6)

    # Clicks the DONE button for add the card
    driver.find_element_by_class_name('ADD_CARD')\
        .find_element_by_tag_name('button').click()
    time.sleep(6)

    print('[!] Credit Card filled.')

def arrange_card_data(card):
    '''Creates a dictionary that contains card number, expiration date and
    security code.'''
    card_dict = dict()
    data = card.split('|')
    card_dict['card_number'] = data[0]
    card_dict['month'] = data[1]
    card_dict['year'] = data[2][2:]
    card_dict['security_code'] = data[-1]
    return card_dict

def add_to_cart(driver, product_url):
    '''Visit a product URL and add it to cart.'''
    visit_product_url(driver, product_url)
    product_title = driver.find_element_by_id('itemTitle').text

    add_link = driver.find_element_by_id('isCartBtn_btn').get_attribute('href')
    # add_button.click()
    driver.get(add_link)
    driver.implicitly_wait(5)
    print('[!] Product {} added to cart.'.format(product_title))

def automate_orders_placement(driver, orders, cards, billing_address):
    '''Reads the orders and place them one by one.'''
    for order, products in orders.items():
        place_order(driver, products, cards[order], billing_address)
        driver = restart_driver(driver)
    

if __name__ == "__main__":
    valid_filename = False
    add_received = False
    address = {
        'name':'',
        'lastname':'',
        'address1':'',
        'address2':'',
        'city':'',
        'state':'',
        'zip':'',
        'country':'',
        'phone':'',
        'email':''
    }

    # Ask the user to enter a filename to read the orders
    orders = extract_from_filename('orders')
    cards = extract_from_filename('cards')
    
    # Ask the user to enter the billing address
    while not add_received:
        print('[!] Please enter the billing address.')
        try:
            for key in address.keys():
                address[key] = input('{}:'.format(key.capitalize()))
            add_received = True
        except Exception as ex:
            print(ex)
    
    try:
        driver  = prepare_chromedriver('https://www.ebay.com')
        # Start placing the orders
        automate_orders_placement(driver, orders, cards, address)
    except Exception as ex:
        print(ex)
    finally:
        driver.quit()
    