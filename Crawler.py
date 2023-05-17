import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, wait
from typing import List

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Executor(ThreadPoolExecutor):
    def __init__(self, max_workers=5):
        ThreadPoolExecutor.__init__(self, max_workers=max_workers)
        self.videos = []

    def fetch_channel_details(self, video_url: str):
        extractor = ChannelExtractor(video_link=video_url)
        extractor.run()

    def crawl_and_fetch(self, seed: str):
        crawler = Crawler(seed, self.videos)
        future: Future = crawler.run()

        while True:
            if self._work_queue.qsize() > 10:
                continue

            if future.done():
                if len(self.videos) < 50:
                    future = Crawler(seed if len(self.videos) == 0 else random.choice(self.videos), self.videos).run()

            if len(self.videos) > 1:
                link = self.videos.pop(0)
                self.submit(self.fetch_channel_details, link)
            else:
                continue


class ChannelExtractor:

    def __init__(self, video_link: str, headless=True):
        service: Service = Service("chromedriver.exe")

        options = webdriver.ChromeOptions()

        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-Advertisement")
        options.add_argument("start-maximized")

        if headless:
            options.add_argument("window-size=1920,1400")
            options.add_argument("--headless")

        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait_driver = WebDriverWait(self.driver, 30)
        self.video_link = video_link

    def sanitizeSubCount(self, raw_string: str):
        num_str = raw_string.split(' ')[0]
        notation = num_str[-1]

        notations = {
            'K': 1000,
            'M': 1000000,
            'B': 1000000000,
            'T': 1000000000000
        }

        if notation.isalpha():
            if notation in notations:
                return str(float(num_str[0:-1]) * notations[notation])
        else:
            return num_str

    def crawl(self, video_link: str):
        self.driver.get(video_link)

        self.wait_driver.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#channel-name #container #text-container #text")))
        self.wait_driver.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#owner-sub-count")))

        channel_element: WebElement = self.driver.find_element(By.CSS_SELECTOR,
                                                               "#channel-name #container #text-container #text a")
        subscriber_element: WebElement = self.driver.find_element(By.CSS_SELECTOR, "#owner-sub-count")

        channel_name = channel_element.text
        channel_url = channel_element.get_attribute("href")
        subscriber = subscriber_element.text

        print(channel_name, channel_url, self.sanitizeSubCount(subscriber))

    def run(self):
        self.crawl(video_link=self.video_link)


class Crawler(ThreadPoolExecutor):

    def __init__(self, seed: str, output_array: List[str], headless=True, videos_per_page=100):

        ThreadPoolExecutor.__init__(self, max_workers=1)
        self.videos_per_page = videos_per_page
        self.seed = seed
        self.output_array: List[str] = output_array

        self.future = None

        service: Service = Service("chromedriver.exe")

        options = webdriver.ChromeOptions()

        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-Advertisement")
        options.add_argument("start-maximized")

        if headless:
            options.add_argument("window-size=1920,20000")
            options.add_argument("--headless")

        self.driver = webdriver.Chrome(service=service, options=options)

    def crawl(self, seed: str):
        self.driver.get(seed)

        start = time.time()

        while True:
            video_url_elements: [WebElement] = self.driver.find_elements(By.XPATH,
                                                                         "//*[(@id = 'video-title')]/parent::*/parent::*")

            print(len(video_url_elements), end=".")

            if len(video_url_elements) >= self.videos_per_page or time.time() - start > 20:
                break

        print()

        for element in video_url_elements:
            href = element.get_attribute("href")
            if href is not None:
                self.output_array.append(href)

    def run(self) -> Future:
        self.seed = random.choice(self.output_array) if len(self.output_array) != 0 else self.seed
        return self.submit(self.crawl, self.seed)




