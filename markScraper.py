import requests
import string
import re
import json
from selenium.webdriver import Firefox
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as firefoxOptions
from selenium.webdriver.chrome.options import Options as chromeOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains


# TODO 
#   Scrape http://docs.splunk.com/Documentation/Splunk/7.2.1/RESTREF/RESTprolog
#   Crawl the resource groups
#   For each resource group extract the endpoints
#   For each endpoint, extract the methods and the items inside these

# -----------------------------------------------------------------------------
domain = 'http://docs.splunk.com/Documentation/Splunk/7.2.1/RESTREF/RESTprolog'
# -----------------------------------------------------------------------------

def prepare_webdriver():
    '''Returns a WebDriver for the script execution'''
    options = firefoxOptions()
    options.add_argument('-headless')
    options.binary_location='/usr/bin/firefox'
    driver = Firefox(executable_path='geckodriver', options=options)
    # options = chromeOptions()
    # options.add_argument('--headless')
    # options.binary_location = '/usr/bin/chromium'
    # driver = Chrome(executable_path='chromedriver', options=options)
    print('[!] Initializing WebDriver...')
    wait = WebDriverWait(driver, timeout=4)
    print('[!] WebDriver ready!')
    return driver

def get_url_with_driver(url, driver, wait_time=5):
    '''Gets a url with the driver, and an specified wait time'''
    print('[!] Getting {}'.format(url))
    driver.get(url)
    wait = WebDriverWait(driver, timeout=wait_time)

def close_driver(driver):
    '''Closes the driver when finish the program or KeyboardInterrupt Error'''
    driver.quit()

def extract_groups(driver):
    '''Extract the groups links from the domain'''
    resources = dict()
    ff_driver = driver
    links = ff_driver.find_elements_by_tag_name('a')

    for r_link in links:
        if r_link.find_element_by_xpath('..').tag_name == 'td':
            name = r_link.text
            value = r_link.get_attribute('href')
            resources[name] = value
        if len(resources) == 14:
            break
    groups = dict()
    for group, group_link in resources.items():
        get_url_with_driver(group_link, driver, 5)
        print('[!] Getting {}. -------- GROUP'.format(group))
        right_nav = driver.find_element_by_id('right-nav')
        groups[group] = extract_endpoints(driver, right_nav)
    return groups

def extract_endpoints(driver, right_nav):
    '''Extracts the endpoints in the page and calls the extract_methods() 
    function.'''
    endpoints_links = driver.find_elements_by_xpath(
        '//*[@id="right-nav"]/ul/li/a')
    endpoints_text = [a.text for a in endpoints_links if ' ' not in a.text]
    endpoints = dict()

    expansible_anchors = driver.find_elements_by_xpath(
        '//*[@id="mw-content-text"]/div/span/a[contains(text(), "Expand")]')
    
    for a in expansible_anchors:
        a.click()
    
    for ep_text in endpoints_text:
        print('[!] Getting {}. ---- ENDPOINT'.format(ep_text))
        driver.execute_script('window.scrollTo(0, 1000)')
        endpoint = driver.find_element_by_xpath(
            '//span[contains(text(), "' +ep_text+ '")]')
        # endpoint_span = endpoint.find_element_by_xpath('//span')
        # method_titles = endpoint.find_elements_by_xpath('following-sibling::h4')
        endpoints[ep_text] = extract_methods(driver, endpoint)
    return endpoints

def extract_methods(driver, endpoint):
    '''Extracts methods from each endpoints.

    It calls the extract_method_parameters() function. '''
    methods = dict()
    methods_order = ['GET', 'POST', 'DELETE']
    next_endpoint = endpoint.find_element_by_xpath('following::h2')
    endpoint_id = endpoint.get_attribute('id')
    try:
        next_endpoint_id = endpoint.find_element_by_xpath('../following::h2/span')\
            .get_attribute('id')
    except NoSuchElementException:
        parent = endpoint.find_element_by_xpath('..')
        endpoint_methods = parent.find_elements_by_xpath(
            'following::h4')
        for method in endpoint_methods:
            methods[method.text] = method
        for method_title in methods.keys():
            print('[!] Getting {} method.'.format(method_title))
            method = methods[method_title]
            methods[method_title] = None
            wait = WebDriverWait(driver, 5)
            click = method.find_element_by_xpath('following-sibling::div/span/a'\
                '[contains(text(), "Collapse")]')
            if methods[method.text] != None:
                continue
            wait = WebDriverWait(driver, 2)
            parameters_div = method.find_element_by_xpath('following-sibling::div/div')
            method_value = extract_method_parameters(driver, parameters_div)
            methods[method_title] = method_value
            # click.click()
        return methods
    else:
        parent = endpoint.find_element_by_xpath('..')
        endpoint_methods = parent.find_elements_by_xpath(
            'following::h4[following::span[@id="{}"]]'\
                .format(next_endpoint_id))
        for method in endpoint_methods:
            methods[method.text] = method
        for method_title in methods.keys():
            print('[!] Getting {} method.'.format(method_title))
            method = methods[method_title]
            methods[method_title] = None
            wait = WebDriverWait(driver, 5)
            if methods[method.text] != None:
                continue
            wait = WebDriverWait(driver, 2)
            parameters_div = method.find_element_by_xpath('following-sibling::*')
            if parameters_div.tag_name == 'div':
                click = parameters_div.find_element_by_xpath('.//a[contains(text(), '\
                    '"Collapse")]')
                method_value = extract_method_parameters(driver, parameters_div)
                methods[method_title] = method_value
            else:
                methods[method_title] = parameters_div.text
        return methods

def extract_method_parameters(driver, parameters_div, last=False):
    '''Extracts the method\'s parameters from:
            -GET
            -POST
            -DELETE
    It will scrape the div and create a dict object'''
    params = ['Request parameters', 
        'Returned values',
        'Response keys',
        'Example request and response']
    # b_tags = []
    # b_tags = parameters_div.find_elements_by_xpath('.//p[contains(text(), "'+p+'")]')
    methods_parameters = dict()
    b_tags = [b for b in parameters_div.find_elements_by_xpath('.//b')
        if b.text in params]
    for b in b_tags:
        key = b.text
        p = b.find_element_by_xpath('ancestor::p')
        value = None
        if 'None' in p.text:
            print('[!] Getting {}.'.format(key))
            value = None
        elif 'example' in p.text.lower():
            try:
                print('[!] Getting {}.'.format(key))
                samples = p.find_elements_by_xpath('following-sibling::div'\
                    '[@class = "samplecode"]')
                examples = {'requests':None, 'response':None}
                for e in zip(examples, samples):
                    examples[e[0]] = extract_inner_xml(driver, e[-1])
                value = examples
            except NoSuchElementException:
                continue
        elif '\n' in p.text:
            print('[!] Getting {}.'.format(key))
            value = p.text.split('\n')[-1]
        elif 'details' in b.text:
            break
        else:
            key = key.strip(string.punctuation+string.whitespace)
            if 'XML' in p.text:
                continue
            else:
                paragraph = p.find_element_by_xpath('following-sibling::p').text
                if paragraph not in params:
                    print('[!] Getting {}.'.format(key))
                    value = paragraph
                else:
                    print('[!] Getting {}.'.format(key))
                    try:
                        tbody = p.find_element_by_xpath('following-sibling::table/tbody')
                        value = extract_inner_values(driver, tbody)
                    except NoSuchElementException:
                        value = None
        methods_parameters[key] = value
    
    return methods_parameters

def extract_inner_xml(driver, samplecode_div):
    '''extracts the inner xml examples from the methods'''
    # samplecode_div = p.find_element_by_xpath(
    #     'following-sibling::div[@class = "samplecode"]')
    value = samplecode_div.text
    return value

def extract_inner_values(driver, parameters_tbody):
    '''Extract the values from:
            -request parameters
            -returned values
            -XML requests
            -XML response
    It will scrape the table and create a list object for the storage'''

    items = list()
    # The first row contains the table info
    rows = parameters_tbody.find_elements_by_tag_name('tr')
    keys = [k.text for k in rows[0].find_elements_by_tag_name('th')]
    row_counter = 0
    for row in rows:
        if row_counter == 0:
            row_counter += 1
            continue
        row_data = {}
        for key, data in zip(keys, row.find_elements_by_tag_name('td')):
            row_data[key] = data.text
        items.append(row_data)
    
    return items

def data_to_json(data):
    json_data = json.dumps(data, indent=4)
    return json_data

if __name__ == '__main__':
    driver = prepare_webdriver()
    data_json = None
    try:
        get_url_with_driver(domain, driver, 5)
        groups = extract_groups(driver)
        
        data_json = data_to_json(groups)

        with open('restDoc.json', 'w') as f:
            f.write(data_json)
    except KeyboardInterrupt as ex:
        print(ex)
        close_driver(driver)
    finally:
        close_driver(driver)