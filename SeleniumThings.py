from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd


class SeleniumThangs:

    placeholder = "I just want things to run."


if __name__ == "__main__":

    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.tcgplayer.com/search/universus/product?productLineName=universus&view=grid")
    elements = driver.find_elements(By.XPATH, '//*[@id="app"]/div/div/section[2]/section/section/section/section/div')
    
    for i in elements:
        print(i)