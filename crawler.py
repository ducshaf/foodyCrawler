import gc
import json
import re
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from iteration_utilities import unique_everseen
import numpy as np
import pandas as pd
from time import sleep
from typing import List, Union
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from glob import glob
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
from common_utils import CommonUtils


class Restaurant:
    def __init__(self, title: str, url: str, sf_url: str, updated_at: str = "",
                 khonggian_rating="", vitri_rating="", phucvu_rating="", giaca_rating="", chatluong_rating="",
                 location="", latitude="", longtitude="", img=""):
        self.title = title
        self.url = url
        self.sf_url = sf_url
        self.khonggian_rating = khonggian_rating
        self.vitri_rating = vitri_rating
        self.phucvu_rating = phucvu_rating
        self.giaca_rating = giaca_rating
        self.chatluong_rating = chatluong_rating
        self.location = location
        self.latitude = latitude
        self.longtitude = longtitude
        self.img = img
        self.updated_at = updated_at

    def to_dict(self):
        return self.__dict__


class Crawler:
    HOMED_URL: str = 'http://www.foody.vn/'
    DISTRICT_VALUES: list = [20, 690, 21, 22, 23, 24, 25, 26, 27, 945, 28, 19, 692, 674, 675, 676,
                             677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 691]
    SCROLL_PAUSE_TIME: float = 1
    USERNAME = 'liberty.sun25651@gmail.com'
    PASSWORD = 'Foody1234'
    MAX_ITEMS = 50000
    MAX_RETRIES = 10
    MAX_WORKERS = 3


    def create_driver(self, headless=False, debug_mode=False, fast_load=True) -> webdriver.Chrome:
        driver = None
        if driver is None:
            options = webdriver.ChromeOptions()
            options.add_argument('--log-level=3')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('--ignore-certificate-errors-spki-list')
            options.add_argument('--blink-settings=imagesEnabled=false')
            # Experiment with json xhr
            options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
            options.add_experimental_option(
                'excludeSwitches', ['enable-logging'])

            if fast_load:
                options.page_load_strategy = 'none'
            if debug_mode:
                options.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
            if headless:
                options.add_argument('--headless')

            service = Service()
            driver = webdriver.Chrome(service=service, options=options)
            driver.maximize_window()
        return driver

    def wait_find(self, driver: webdriver.Chrome, selector_str: str, selector_type: str = 'css', num_ele: str = 'one',
                  timeout: int = 20, wait_type: int = 'present', stop_loading: bool = False) -> Union[WebElement, List[WebElement]]:
        wait = WebDriverWait(driver, timeout)
        try:
            if wait_type == 'present':
                if num_ele == 'one':
                    if selector_type == 'css':
                        ele = wait.until(EC.presence_of_element_located(
                            (By.CSS_SELECTOR, selector_str)))
                    elif selector_type == 'tag':
                        ele = wait.until(EC.presence_of_element_located(
                            (By.TAG_NAME, selector_str)))
                    elif selector_type == 'class':
                        ele = wait.until(EC.presence_of_element_located(
                            (By.CLASS_NAME, selector_str)))
                elif num_ele == 'many':
                    if selector_type == 'css':
                        ele = wait.until(EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, selector_str)))
                    elif selector_type == 'tag':
                        ele = wait.until(EC.presence_of_all_elements_located(
                            (By.TAG_NAME, selector_str)))
                    elif selector_type == 'class':
                        ele = wait.until(EC.presence_of_all_elements_located(
                            (By.CLASS_NAME, selector_str)))

            elif wait_type == 'clickable':
                if num_ele == 'one':
                    if selector_type == 'css':
                        ele = wait.until(EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, selector_str)))
                    elif selector_type == 'tag':
                        ele = wait.until(EC.element_to_be_clickable(
                            (By.TAG_NAME, selector_str)))
                    elif selector_type == 'class':
                        ele = wait.until(EC.element_to_be_clickable(
                            (By.CLASS_NAME, selector_str)))

            elif wait_type == 'visible':
                if num_ele == 'one':
                    if selector_type == 'css':
                        ele = wait.until(EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, selector_str)))
                    elif selector_type == 'tag':
                        ele = wait.until(EC.visibility_of_element_located(
                            (By.TAG_NAME, selector_str)))
                    elif selector_type == 'class':
                        ele = wait.until(EC.visibility_of_element_located(
                            (By.CLASS_NAME, selector_str)))
                elif num_ele == 'many':
                    if selector_type == 'css':
                        ele = wait.until(EC.visibility_of_all_elements_located(
                            (By.CSS_SELECTOR, selector_str)))
                    elif selector_type == 'tag':
                        ele = wait.until(EC.visibility_of_all_elements_located(
                            (By.TAG_NAME, selector_str)))
                    elif selector_type == 'class':
                        ele = wait.until(EC.visibility_of_all_elements_located(
                            (By.CLASS_NAME, selector_str)))
        except Exception as e:
            return None

        if stop_loading:
            driver.execute_script('window.stop();')
        return ele

    def instant_find(self, driver: webdriver.Chrome, selector_str: str, selector_type: str = 'css',
                     num_ele: str = 'one') -> Union[WebElement, List[WebElement]]:
        try:
            if num_ele == 'one':
                if selector_type == 'css':
                    ele = driver.find_element(By.CSS_SELECTOR, selector_str)
                elif selector_type == 'tag':
                    ele = driver.find_element(By.TAG_NAME, selector_str)
                elif selector_type == 'class':
                    ele = driver.find_element(By.CLASS_NAME ,selector_str)
            elif num_ele == 'many':
                if selector_type == 'css':
                    ele = driver.find_elements(By.CSS_SELECTOR, selector_str)
                elif selector_type == 'tag':
                    ele = driver.find_elements(By.TAG_NAME, selector_str)
                elif selector_type == 'class':
                    ele = driver.find_elements(By.CLASS_NAME, selector_str)

        except Exception as e:
            return None
        return ele

    def scroll_to_bottom(self, driver: webdriver.Chrome):
        driver.execute_script(
            'window.scrollTo(0, document.body.scrollHeight);')

    def move_and_click(self, driver: webdriver.Chrome, ele: WebElement):
        ActionChains(driver).move_to_element(ele).click().perform()

    def export_restaurants_list(self, restaurants: List[dict]) -> None:
        CSV_PATH = './results/restaurants_v6.csv'
        restaurants_list_df = pd.DataFrame.from_dict(restaurants)
        restaurants_list_df.to_csv(CSV_PATH, mode='a', index=False)

    def login(self, driver: webdriver.Chrome):
        driver.get('https://id.foody.vn/account/login?returnUrl=https://www.foody.vn/ha-noi')
        username_input = self.wait_find(
            driver=driver, selector_str='#Email', selector_type='css', num_ele='one')
        username_input.send_keys(self.USERNAME)
        password_input = self.wait_find(
            driver=driver, selector_str='#Password', selector_type='css', num_ele='one')
        password_input.send_keys(self.PASSWORD)
        login_btn = self.wait_find(
            driver=driver, selector_str='#bt_submit', selector_type='css', num_ele='one')
        login_btn.click()

    def get_content_item_elements(self, driver: webdriver.Chrome) -> List[WebElement]:
        for i in tqdm(range(30)):
            sleep(5)
            select = Select(driver.find_element(By.ID, 'slDistrictPlace'))
            select.select_by_index(i+1)

            n_items: int = 0
            content_item_eles: List[WebElement] = []
            n_tries = 1
            # First few infinite scrolls
            while 1:
                self.scroll_to_bottom(driver)
                sleep(self.SCROLL_PAUSE_TIME)
                content_item_eles: List[WebElement] = self.wait_find(
                    driver=driver, selector_str='content-item', selector_type='class', num_ele='many')
                if len(content_item_eles) == n_items:
                    break
                n_items = len(content_item_eles)

            curr_n_items = len(content_item_eles)
            food_shops: List[dict] = self.get_restaurant_info(
                driver=driver, content_item_eles=content_item_eles)
            self.export_restaurants_list(food_shops)

            # Xem thêm
            while 1:
                try:
                    xemthem_btn_ele = self.wait_find(
                        driver=driver,
                        selector_str='#ajaxRequestDiv > div > div.content-container.fd-clearbox.ng-scope > div.pn-loadmore.fd-clearbox.ng-scope > a > label',
                        selector_type='css', num_ele='one')
                    content_item_eles: List[WebElement] = []

                    if xemthem_btn_ele:
                        self.move_and_click(driver, xemthem_btn_ele)
                        sleep(0.5)
                        self.scroll_to_bottom(driver)
                        sleep(self.SCROLL_PAUSE_TIME)
                        content_item_eles: List[WebElement] = self.wait_find(
                            driver=driver, selector_str='content-item', selector_type='class', num_ele='many')
                        logging.info(
                            f'{len(content_item_eles)} / {self.MAX_ITEMS} ~ {(len(content_item_eles) * 100 / self.MAX_ITEMS):.2f} % items loaded')

                    if len(content_item_eles) > 0:
                        food_shops.extend(self.get_restaurant_info(
                            driver, content_item_eles[curr_n_items:]))
                        self.export_restaurants_list(food_shops)

                    # Get XHR response from selenium dev tool
                    # logs_raw = driver.get_log("performance")
                    # logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
                    #
                    # def log_filter(log_):
                    #     return (
                    #         # is an actual response
                    #             log_["method"] == "Network.responseReceived"
                    #             # and json
                    #             and "json" in log_["params"]["response"]["mimeType"]
                    #             # get restaurants json only
                    #             and 'https://www.foody.vn/ha-noi/dia-diem?ds=Restaurant' in log_["params"]["response"]["url"]
                    #     )
                    #
                    # for log in filter(log_filter, logs):
                    #     request_id = log["params"]["requestId"]
                    #     response = driver.execute_cdp_cmd("Network.getResponseBody",
                    #                                       {"requestId": request_id})
                    #
                    #     # Extract restaurant info
                    #     body = response['body']
                    #     searchItems = json.loads(body)['searchItems']
                    #     res_list = []
                    #     for i in tqdm(searchItems, desc='Getting restaurants: '):
                    #         res = Restaurant(title=i['Name'], url=i['DetailUrl'], sf_url=i['DeliveryUrl'],
                    #                          small_pic=i['MobilePicturePath'], large_pic=i['PicturePathLarge'],
                    #                          updated_at=CommonUtils.get_date_time())
                    #         res_list.append(res.to_dict())
                    #
                    #     self.export_restaurants_list(res_list)

                    if xemthem_btn_ele is None:
                        break
                    n_tries = 1
                    curr_n_items = len(content_item_eles)

                except Exception as e:
                    logging.error(str(e) + f' retries #{n_tries}')
                    if n_tries > self.MAX_RETRIES:
                        break
                    n_tries += 1
                    continue

        return content_item_eles

    def get_restaurant_info(self, driver: webdriver.Chrome, content_item_eles: List[WebElement]) -> List[dict]:
        def process(content_item_ele: WebElement) -> Restaurant:
            sf_ele: WebElement = content_item_ele.find_element(By.CLASS_NAME, 'avatar')
            a_ele: WebElement = sf_ele.find_element(By.TAG_NAME, 'a')
            sf_url: str = a_ele.get_attribute('href')

            resname_ele: WebElement = content_item_ele.find_element(By.CLASS_NAME, 'title')
            a_ele: WebElement = resname_ele.find_element(By.TAG_NAME, 'a')
            title: str = a_ele.text
            url: str = a_ele.get_attribute('href')

            loc_ele: WebElement = content_item_ele.find_element(By.CSS_SELECTOR, 'div.desc.fd-text-ellip.ng-binding')
            location: str = loc_ele.text

            updated_at: str = CommonUtils.get_date_time()
            restaurant = Restaurant(title=title, url=url, sf_url=sf_url, updated_at=updated_at, location=location)
            return restaurant

        restaurants = CommonUtils.process_list(
            inputs=content_item_eles, func=process, desc='Getting restaurants', method='multi')
        restaurants = [res.to_dict() for res in restaurants]
        return restaurants

    def go_get_restaurant(self):
        driver = self.create_driver()
        driver.get(self.HOMED_URL)
        # login
        self.login(driver)
        # get FoodShop information
        sleep(5)
        content_item_eles: List[WebElement] = self.get_content_item_elements(driver)
        driver.quit()


    def go_get_review(self):
        driver = self.create_driver()
        driver.get(self.HOMED_URL)
        self.login(driver)
        sleep(5)

        F = open('results/in_218_29.jl', encoding='utf8')
        res_list = [json.loads(line) for line in F]
        F.close()
        del F
        gc.collect()

        for res in res_list[206:]:
            try:
                crawler.get_comment(driver, 'https://www.foody.vn' + res['DetailUrl'])
            except Exception as e:
                logging.error(str(e) + " at https://www.foody.vn" + res['DetailUrl'])

        driver.quit()

    def get_comment(self, driver, url):
        driver.get(url)
        print(len(driver.find_elements(By.CSS_SELECTOR, 'div.errorpage')))
        if (len(driver.find_elements(By.CSS_SELECTOR, 'div.errorpage')) != 0):
            return

        review_count_eles = self.wait_find(driver=driver,
                                      selector_str='div.ratings-boxes > div.summary > b',
                                      selector_type='css', num_ele='one')
        review_count = review_count_eles.text

        if review_count_eles is None:
            return

        def get_review_ratings(driver: webdriver.Chrome, review_data):
            hover_eles = self.wait_find(driver=driver,
                                        selector_str='div.review-user.fd-clearbox.ng-scope > div > div.review-points',
                                        selector_type='css', num_ele='many')

            for e in hover_eles:
                while 1:
                    ActionChains(driver).move_to_element(e).perform()
                    sleep(0.75)

                    rating_ele = self.wait_find(driver=driver,
                                                selector_str="#fdDlgReviewRating",
                                                selector_type='css', num_ele='one')

                    if rating_ele is not None:
                        break


        show_more = True
        # Scroll 2 times
        for i in range(2):
            review_item_eles = self.wait_find(driver=driver,
                                              selector_str="review-item",
                                              selector_type='class', num_ele='many')

            self.scroll_to_bottom(driver)
            sleep(self.SCROLL_PAUSE_TIME)

            more_ele = self.wait_find(driver=driver,
                                      selector_str="div.foody-box-review.ng-scope > div.pn-loadmore.fd-clearbox.ng-scope > a > label",
                                      selector_type='css', num_ele='one')
            if more_ele:
                show_more = True
                break
            else:
                show_more = False

            if len(review_item_eles) == int(review_count):
                show_more = False
                break

        # Click show more Review
        while show_more:
            more_eles = self.wait_find(driver=driver,
                                       selector_str="a.fd-btn-more > label",
                                       selector_type='css', num_ele='many')
            more_ele = None
            for e in more_eles:
                if e.text == 'Xem thêm bình luận':
                    more_ele = e
                    break


            if more_ele is not None:
                self.move_and_click(driver, more_ele)
                sleep(0.5)
                review_item_eles = self.wait_find(driver=driver,
                                                  selector_str="review-item",
                                                  selector_type='class', num_ele='many')

                self.scroll_to_bottom(driver)
                sleep(self.SCROLL_PAUSE_TIME)
                # print('{eles}/{count}'.format(eles=len(review_item_eles), count=int(review_count)))

            if more_ele is None:
                break

        review_jsons = []
        # Get inititial first 10 reviews from html
        script_ele = self.instant_find(driver=driver,
                                       selector_str='div.lists.list-reviews > script',
                                       selector_type='css', num_ele='one')

        review_json = re.search("\{.*\}", script_ele.get_attribute('innerHTML'))
        review_jsons.append(review_json.group())

        # Get XHR responses from selenium dev tool
        logs_raw = driver.get_log("performance")
        logs = [json.loads(lr["message"])["message"] for lr in logs_raw]

        def filter_loadmore(log_):
            return (
                # is a response
                log_["method"] == "Network.responseReceived"
                # and json
                and "json" in log_["params"]["response"]["mimeType"]
                # and from ResLoadMore API
                and 'https://www.foody.vn/__get/Review/ResLoadMore' in log_["params"]["response"]["url"]
            )

        def filter_reviewinfo(log_):
            return (
                # is a response
                log_["method"] == "Network.responseReceived"
                # and json
                and "json" in log_["params"]["response"]["mimeType"]
                # and from GetReviewInfo API
                and 'https://www.foody.vn/__get/Review/GetReviewInfo' in log_["params"]["response"]["url"]
            )

        if show_more:
            log_loadmore = filter(filter_loadmore, logs)
            for log in log_loadmore:
                request_id = log["params"]["requestId"]
                response = driver.execute_cdp_cmd("Network.getResponseBody",
                                                  {"requestId": request_id})

                # Extract review info
                body = response['body']
                review_jsons.append(body)


        # Parse review jsons
        review_data = []
        for reviews in review_jsons:
            review_data.extend(self.parse_reviews(reviews))

        # Start crawling detailed ratings, TODO: data missing
        get_review_ratings(driver, review_data)

        logs_raw = driver.get_log("performance")
        logs = [json.loads(lr["message"])["message"] for lr in logs_raw]

        rating_jsons = []
        for log in filter(filter_reviewinfo, logs):
            request_id = log["params"]["requestId"]
            response = driver.execute_cdp_cmd("Network.getResponseBody",
                                              {"requestId": request_id})

            # Extract review info
            body = response['body']
            rating_jsons.append(body)

        # Parse rating jsons
        ratings = []
        while 1:
            for rating_json in rating_jsons:
                ratings.append(self.parse_rating(rating_json))

            hover_eles = self.wait_find(driver=driver,
                                        selector_str='div.review-user.fd-clearbox.ng-scope > div > div.review-points',
                                        selector_type='css', num_ele='many')
            hover_eles_count = len(hover_eles)

            if (hover_eles_count > len(ratings)):
                print('Getting missing data... {cur}/{all}'.format(cur=len(ratings), all=hover_eles_count))
                # Check for missing json
                review_ids = [item['Id'] for item in review_data]
                review_ids_rating = [item['Id'] for item in ratings]
                missing_id = set(review_ids).difference(review_ids_rating)

                # Find the missing elements
                missing_eles = []
                for id in missing_id:
                    ele = driver.find_elements(By.CSS_SELECTOR,
                                              "div.review-points.ng-scope[data-review='review_{}']".format(id))
                    if len(ele) != 0:
                        missing_eles.append(ele[0].find_element(By.CSS_SELECTOR, 'span.ng-binding'))

                # Hover to get the missing json
                for e in missing_eles:
                    while 1:
                        ActionChains(driver).move_to_element(e).perform()
                        sleep(0.75)

                        rating_ele = self.wait_find(driver=driver,
                                                    selector_str="#fdDlgReviewRating",
                                                    selector_type='css', num_ele='one')

                        if rating_ele is not None:
                            break

                # Extract and append to rating_jsons at the right location
                logs_raw = driver.get_log("performance")
                logs = [json.loads(lr["message"])["message"] for lr in logs_raw]

                for log in filter(filter_reviewinfo, logs):
                    request_id = log["params"]["requestId"]
                    response = driver.execute_cdp_cmd("Network.getResponseBody",
                                                      {"requestId": request_id})

                    # Extract review info
                    body = response['body']
                    rating_jsons.append(body)

            else:
                break

        # Join review_data and ratings
        ratings = list(unique_everseen(ratings))
        for i in range(len(review_data)):
            r = None
            for rate in ratings:
                if rate['Id'] == review_data[i]['Id']:
                    r = rate
                    break
            review_data[i]['Ratings'] = r

        # Save review data
        # f = open('results/review_data_29.json', 'w')
        # f.write(json.dumps(review_data))
        # f.close()
        f = open('results/review_data_29.json', 'r+')
        f.seek(0, 2)
        position = f.tell() - 1
        f.seek(position)
        f.write(",{}]".format(json.dumps(review_data)))
        f.close()

        del review_jsons
        del review_data
        del f
        gc.collect()


    def parse_reviews(self, reviews_json):
        reviews = json.loads(reviews_json)['Items']
        review_data = []

        for review in reviews:
            info: dict = {}

            info['Id'] = review['Id']
            info['ResId'] = review['ResId']
            info['Title'] = review['Title']
            info['Like_count'] = review['TotalLike']
            info['View_count'] = review['TotalViews']
            info['Content'] = review['Description']
            info['Create_date'] = review['CreatedOnTimeDiff']
            info['Device'] = review['DeviceName']
            info['Comments'] = review['Comments']
            info['Pictures'] = review['Pictures']

            owner: dict = {}
            owner['Id'] = review['Owner']['Id']
            owner['Url'] = review['Owner']['Url']
            owner['Verify_status'] = review['Owner']['IsVerified']
            owner['Level'] = review['Owner']['Level']
            owner['Picture_count'] = review['Owner']['TotalPictures']
            owner['Review_count'] = review['Owner']['TotalReviews']
            owner['Trust_rate'] = review['Owner']['TrustPercent']

            info['Owner_info'] = owner
            review_data.append(info)

        return review_data


    def parse_rating(self, rating_json):
        rating = json.loads(rating_json)
        rates: dict = {'Id': rating['Id'],
                       'Vi tri': rating['Position'],
                       'Gia ca': rating['Price'],
                       'Chat luong': rating['Food'],
                       'Phuc vu': rating['Services'],
                       'Khong gian': rating['Atmosphere']}
        return rates

    def get_restaurant_details(self, driver, url: str):
        info: dict = {}
        retries = 3
        while retries > 0:
            try:
                driver.get(url)
                res_rating_eles: List[WebElement] = self.wait_find(driver=driver, selector_str="microsite-top-points",
                                                                   selector_type="class", num_ele="many")
                ratings = [res_rating.find_element(By.TAG_NAME, 'span').text for res_rating in res_rating_eles]
                labels = [res_rating.find_element(By.CLASS_NAME, 'label').text for res_rating in res_rating_eles]

                info = dict(zip(labels, ratings))
                info['Location'] = driver.find_element(By.XPATH, '//span[@itemprop="streetAddress"]').text

                html = driver.page_source
                soup = BeautifulSoup(html, 'lxml')
                info["Latitude"] = soup.find("meta", property="place:location:latitude")["content"]
                info["Longitude"] = soup.find("meta", property="place:location:longitude")["content"]
                info["ImgURL"] = soup.find("meta", property="og:image:url")["content"]

                if info != {}:
                    break
            except Exception as e:
                logging.info(str(e) + ' exception url: ' + url)
                retries -= 1
                if (retries <= 0):
                    break
                continue

        return OrderedDict(sorted(info.items()))


def get_restaurants_info_export(res_df: pd.DataFrame):
    res_df.reset_index(drop=True, inplace=True)
    urls = res_df["url"]
    tqdm.pandas()

    driver = crawler.create_driver()
    restaurants = urls.progress_apply(lambda x: crawler.get_restaurant_details(driver, x))
    result = pd.DataFrame.from_records(restaurants, index=res_df.index)
    result = res_df.join(result)
    result.to_csv('results/restaurant_infos.csv', mode='a', header=False, index=False)

    driver.quit()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    crawler = Crawler()
    crawler.go_get_review()

    # res_df = pd.read_csv('results/restaurant_infos.csv', encoding='utf8')
    # res_df = res_df.dropna(how='any', subset=['url', 'title', 'location', 'Latitude', 'Longitude']).reset_index(drop=True)
    # res_df = res_df.drop_duplicates(subset=['url']).reset_index(drop=True)
    #
    # res_df['coordinate'] = res_df.apply(lambda x: str(x.Latitude) + ", " + str(x.Longitude), axis=1)
    # res_df.drop(['Location'], inplace=True, axis=1)
    # res_df.rename(columns={"Giá cả": "giaca", "Chất lượng": "chatluong", "Không gian": "khonggian","Phục vụ": "phucvu",
    #                        "Vị trí": "vitri", "ImgURL": "img_url", "Latitude": "__lat__", "Longitude": "__lon__"}, inplace=True)
    #
    #
    # res_df = res_df[['title', 'location', 'coordinate']]
    # print(res_df)
    # res_df.to_csv('results/re_info.csv', index=False)
    # batches = np.array_split(res_df, 50)

    # for i in range(104, 150):
    #     get_restaurants_info_export(res_df[i*1000:(i+1)*1000])



    # with ThreadPoolExecutor(max_workers=3) as executor:
    #     try:
    #         executor.map(get_restaurants_info_export, batches)
    #     except Exception as e:
    #         print(e)
    #         pass

    # {
    #     "properties": {
    #         "title": {"type": "text"},
    #         "url": {"type": "keyword"},
    #         "sf_url": {"type": "text"},
    #         "location": {"type": "text"},
    #         "updated_at": {
    #             "type": "date",
    #             "format": "dd-MM-yyyy HH:mm"
    #         },
    #         "Chất lượng": {"type": "float"}
    #         "coordinate": {"type": "geo_point"}
    #     }
    # }

    # logger.info(len(df))
