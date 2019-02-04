import time
import selenium
import argparse
import getpass
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


def prepare_driver():
    '''Returns a Firefox Webdriver.'''
    options = Options()
    options.add_argument('-headless')
    driver = Chrome(executable_path='chromedriver', options=options)
    return driver

def login(driver, domain):
    '''Ask the user to enter ID and Password and goes to Pixabay 
    to makes the login.'''
    user_id = input('ID: ')
    passwd = getpass.getpass()

    driver.get(domain)
    time.sleep(2)
    login_button = WebDriverWait(driver, timeout=10).until(
        EC.visibility_of_element_located((By.XPATH, 
            '//a[contains(text(), "Log in")]')))

    login_button.click() # Clicks the button
    
    username_field = WebDriverWait(driver, timeout=5).until(
        EC.presence_of_element_located((By.XPATH, 
        '//input[contains(@name, "username")]')))
    username_field.send_keys(user_id)

    password_field = WebDriverWait(driver, timeout=5).until(
        EC.presence_of_element_located((By.XPATH, 
        '//input[contains(@name, "password")]')))
    password_field.send_keys(passwd)

    # Click the Log in button
    driver.find_element_by_xpath(
        '//input[contains(@class, "pure-button button-green")]')\
        .click()

    # driver.implicitly_wait(5)
    time.sleep(5)
    print('[!] Logged in successfully')

def search_images(driver, n_images, keyword):
    '''Makes a search in the main page and selects n_images images
    for download.'''
    image_counter = 0
    driver.get(domain)

    search_bar = WebDriverWait(driver, timeout=5).until(
        EC.presence_of_element_located((By.XPATH, 
            '//input[contains(@class, "q")]')))
    search_bar.send_keys(keyword)

    search_button = driver.find_element_by_xpath(
        '//input[contains(@value, "Search")]')
    search_button.click()
    # Let's wait until the search is completed
    time.sleep(7)

    search_results_url = driver.current_url

    while image_counter < n_images:
        images_divs = driver.find_element_by_class_name('search_results')\
            .find_elements_by_class_name('item')
        
        # Select the next image
        image = images_divs[image_counter]
        image.click()

        # Wait for the page to load completely
        time.sleep(3)
        download_image(driver)
        print('[!] Image {} downloaded.'.format(driver.current_url))
        # We return to the search results url
        driver.get(search_results_url)
        image_counter += 1
        


def download_image(driver):
    '''Goes to the image url and downloads it.'''

    download_menu = WebDriverWait(driver, timeout=5).until(
        EC.presence_of_element_located((By.XPATH, 
            '//div[contains(@class, "download_menu")]')))
    download_menu.click()

    download_button = driver.find_element_by_xpath(
        '//a[contains(@class, "dl_btn")]')
    download_button.click()

    driver.implicitly_wait(5)

if __name__ == '__main__':
    domain =  'https://www.pixabay.com'

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--number_of_images', type=int, help='Amount of images')
    parser.add_argument('-k', '--keyword', type=str, help='Keyword for the search')

    args = parser.parse_args()

    driver = prepare_driver()
    login(driver, domain)

    search_images(driver, args.number_of_images, args.keyword)
    print('[!] Download finished.')