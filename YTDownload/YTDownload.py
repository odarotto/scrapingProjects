import selenium
import time
import pytube
import argparse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

domain =  'https://www.youtube.com'

# Usage:
# Automate the Download of Youtube Videos

# optional arguments:
#   -h, --help            show this help message and exit
#   -s SINGLE_DOWNLOAD, --single_download SINGLE_DOWNLOAD
#                         Download a single video from URL
#   -p PLAYLIST_DOWNLOAD, --playlist_download PLAYLIST_DOWNLOAD
#                         Downloads an entire playlist. It will require login.
#   -r RESOLUTION, --resolution RESOLUTION
#                         Video Resolution
#   -a, --audio           Downloads only audio streams


class FinishedDownloadException(Exception):
    pass

def prepare_chromedriver():
    '''Returns a Firefox Webdriver with a custom user-agent.'''
    options = Options()
    # options.add_argument('-headless')
    driver = Chrome(executable_path='chromedriver', options=options)
    print('[!] Chrome WebDriver is ready.')
    return driver

def single_download(url, res='720p', only_audio=False):
    '''Perform a single download using Pytube library.'''
    yt = pytube.YouTube(url)
    print('[!] Downloading {}'.format(yt.title))
    if only_audio:
        yt.streams.filter(only_audio=only_audio)\
            .first()\
            .download()
    else:
        yt.streams.filter(only_audio=only_audio, 
            progressive=True, file_extension='mp4')\
            .order_by('resolution')\
            .desc()\
            .first()\
            .download()
    print('[!] {} Download Completed'.format(yt.title))

def playlist_download(driver, playlist_url, res='720p', only_audio=False):
    '''Downloads a public playlist.'''

    # Visit the playlist URL
    driver.get(playlist_url)
    video_list = driver.find_element_by_id('contents')\
        .find_elements_by_id('content')
    
    for video in video_list:
        video_url = video.find_element_by_tag_name('a')\
            .get_attribute('href')
        video_url = video_url.split('&')[0]
        single_download(video_url, res=res, only_audio=only_audio)
    
    pass

if __name__ == '__main__':

    # Define the argument parser for our script
    parser = argparse.ArgumentParser(description='Automate the Download of\
        Youtube Videos')
    parser.add_argument('-s', '--single_download', 
        help='Download a single video from URL', type=str)
    parser.add_argument('-p', '--playlist_download', 
        help='Downloads an entire playlist. It will require login.', type=str)
    parser.add_argument('-r', '--resolution', help='Video Resolution', 
        type=str, choices=['240p', '320p', '720p', '1080p'])
    parser.add_argument('-a', '--audio', help='Downloads only audio streams',
        action='store_true')
    args = parser.parse_args()


    try:
        time.sleep(5)
        if args.single_download:
            # Calls download function
            single_download(args.single_download, res=args.resolution,
                only_audio=args.audio)
            raise FinishedDownloadException
        elif args.playlist_download:
            driver = prepare_chromedriver()
            playlist_download(driver, args.playlist_download,
                res=args.resolution, only_audio=args.audio)
            raise FinishedDownloadException
    except FinishedDownloadException:
        print('[!] Download finished.')
    except KeyboardInterrupt:
        print('[!] Download Interrupted.')
    except Exception as ex:
        print(ex)
    finally:
        driver.quit()