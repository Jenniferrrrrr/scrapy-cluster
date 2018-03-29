from __future__ import absolute_import
import scrapy

from scrapy.http import Request
# from crawling.spiders.lxmlhtml import CustomLxmlLinkExtractor as LinkExtractor
from scrapy.conf import settings

from crawling.items import RawResponseItem
from crawling.spiders.redis_spider import RedisSpider

from sys import platform, maxint
import csv
import os
import random
import string
import scrapy
import time
import uuid

# Docker API
#from docker import Client
#from docker.utils import kwargs_from_env


# Driver
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import StaleElementReferenceException

# Driver Exceptions
from selenium.common.exceptions import *

# Parser
from bs4 import BeautifulSoup
from bs4.element import Comment

import urllib2, cookielib
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
from cStringIO import StringIO

# Display for headless mode
# from pyvirtualdisplay import Display

# Only use this if running on a non linux machine
driverPath = 'chromedriver'

inline_tags = ["b", "big", "i", "small", "tt", "abbr", "acronym", "cite", "dfn",
               "em", "kbd", "strong", "samp", "var", "bdo", "map", "object", "q",
               "span", "sub", "sup"]


class LinkException(Exception):
    """Only called by link class. Add to switch statement as necessary"""

    def __init__(self, switch=-1):
        if switch == 0:
            self.value = "ERROR: Link type was not html or JavaScript"
        elif switch == 1:
            self.value = "ERROR: Link was Unclickable"
        elif switch == 2:
            self.value = "ERROR: Link is JavaScript based but an index value was not set"
        elif switch == -1:
            self.value = "No value was specified in LinkException Switch. " \
                         "Make sure you are properly calling this exception"

    def __str__(self):
        return str(self.value)


class Link(object):
    """Class that stores all of the information regarding a link. Each link has a type (either html of JavaScript),
    the href attribute (what the link redirects to), a fallback url, and an index value (used for JavaScript Links)"""

    def __init__(self, href_attribute, matcher="", calling_url="", index=-1):
        if type(href_attribute) == dict:
            self.dict_init(href_attribute)
            return
        if calling_url == "" and index == -1:
            self.type = "html"
            self.hrefAttribute = href_attribute
            self.matcher = self.hrefAttribute.split(".")[1]
            self.text = ""
            return
        self.type = ""
        self.hrefAttribute = ""
        self.fallbackURL = calling_url
        self.matcher = matcher
        self.index = 0
        if (href_attribute.startswith("http") and href_attribute.split(".")[1] == matcher and len(href_attribute) > len(
                calling_url)):
            self.type = "html"
            self.hrefAttribute = href_attribute
        elif href_attribute.startswith("javascript"):
            self.type = "JavaScript"
            self.hrefAttribute = href_attribute
            self.index = index
        else:
            # print "Link: " + href_attribute + " not added"
            # print "matcher: " + matcher
            # print "calling_url: " + calling_url
            raise LinkException(0)
        self.name = ""
        self.gather_name(delimiter="-")

        self.text = ""

    def dict_init(self, link_dict):
        self.index = link_dict["index"]
        self.name = link_dict["name"]
        self.text = link_dict["text"]
        self.fallbackURL = link_dict["fallbackURL"]
        self.matcher = link_dict["matcher"]
        self.hrefAttribute = link_dict["hrefAttribute"]
        self.type = link_dict["type"]

    @staticmethod
    def tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def gather_text(self, driver):
        page_source_replaced = driver.page_source
        # Remove inline tags
        for it in inline_tags:
            page_source_replaced = page_source_replaced.replace("<" + it + ">", "")
            page_source_replaced = page_source_replaced.replace("</" + it + ">", "")

        # Create random string for tag delimiter
        # random_string = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=75))
        random_string = str(uuid.uuid4().get_hex().upper()) 
        soup = BeautifulSoup(page_source_replaced, 'lxml')

        # remove non-visible tags
        [s.extract() for s in soup(['style', 'script', 'head', 'title', 'meta', '[document]'])]
        visible_text = soup.getText(random_string).replace("\n", "")
        visible_text = visible_text.split(random_string)
        self.text = "\n".join(list(filter(lambda vt: vt.split() != [], visible_text)))

    def click_and_yield(self, driver):
        finished = 0
        if self.type == "html":
            driver.get(self.hrefAttribute)
            self.gather_text(driver)
            new_links = self.get_new_links(driver, self.hrefAttribute)  # Yield new links
            return new_links
        elif self.type == "JavaScript":
            if self.index is None:
                raise LinkException(2)
            # driver.get(self.fallbackURL)
            while finished == 0:
                try:
                    driver.get(self.fallbackURL)
                    finished = 1
                except:
                    time.sleep(5)
            new_links = self.get_new_links(driver, self.fallbackURL)  # Yield new links
            try:
                driver.find_elements_by_xpath("//a[@href]")[self.index].click_and_yield()
                self.gather_text(driver)
                return new_links
            except (WebDriverException, ElementNotVisibleException, ElementNotInteractableException,
                    ElementNotSelectableException):
                link = driver.find_elements_by_xpath("//a[@href]")[self.index]
                move = ActionChains(driver).move_to_element(link)
                move.perform()
                try:
                    link.click_and_yield()
                    self.gather_text(driver)
                    # driver.close()
                    return new_links
                except (WebDriverException, ElementNotVisibleException, ElementNotInteractableException,
                        ElementNotSelectableException):
                    # driver.close()
                    raise LinkException(1)
        else:
            raise LinkException(0)

    def gather_name(self, delimiter=" "):
        if self.type == "html":
            unfiltered_name = self.hrefAttribute[
                              len(self.hrefAttribute) - (len(self.hrefAttribute) - len(self.fallbackURL)):
                              len(self.hrefAttribute)]
            unfiltered_name = unfiltered_name.split("/")
            self.name = ""
            if len(unfiltered_name) != 1:
                for i in range(len(unfiltered_name)):
                    self.name += unfiltered_name[i] + delimiter
            else:
                self.name = unfiltered_name[0]
        elif self.type == "JavaScript":
            self.name = ""

    def write_file(self, filepath, counter):
        file_name = self.name
        if self.type == "html":
            file = open(str(filepath) + "/" + file_name + ".txt", "w")
        elif self.type == "JavaScript":
            file = open(str(filepath) + "/" + "JavaScript Link " + str(counter) + ".txt", "w")
        else:
            raise LinkException(0)
        file.write(self.text)
        file.close()

    def __str__(self):
        s = ""
        s += "Link Type:" + self.type + " "
        s += "hrefAttribute:" + self.hrefAttribute + " "
        s += "name:" + self.name + " "
        s += "FallbackURL(Only used for JS):" + self.fallbackURL + " "
        s += "Index (Only used for JS):" + str(self.index) + " "
        return s

    def get_new_links(self, driver, calling_url):
        elems = driver.find_elements_by_xpath("//a[@href]")
        new_requests = []
        for elem in elems:
            href_attr = ""
            try:
                href_attr = elem.get_attribute("href")
            except StaleElementReferenceException:
                # print "Couldn't get href attribute"
                continue
            if not href_attr.lower().startswith(calling_url.lower()):
                continue
            try:
                link = Link(elem.get_attribute("href"), self.matcher, calling_url=calling_url, index=elems.index(elem))
                new_requests.append(link)
            except LinkException:
                # print elem.get_attribute("href") + " was not added as it did not match the main url"
                continue
            except Exception as e:
                # print e, e.message
                continue
        return new_requests


def check_path_exists(path):
    if os.path.exists(path):
        return True
    return False


if not check_path_exists("results"):
    os.mkdir("results")
if not check_path_exists("diagnostics"):
    os.mkdir("diagnostics")


def read_csv(filename):
    requests = []
    import codecs
    with codecs.open(filename, "r", encoding='utf-8', errors='ignore') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            if reader.line_num != 1 and row[4] != "0":
                if row[4][:-1] == "/":
                    requests.append(row[4][:-1])
                else:
                    requests.append(row[4])
    return requests


class SeleniumSpider(RedisSpider):
    name = "selenium"

    def __init__(self, *args, **kwargs):
        super(SeleniumSpider, self).__init__(*args, **kwargs)
        selenium_host = 'http://hub:4444/wd/hub'
        self.driver = webdriver.Remote(command_executor=selenium_host, desired_capabilities=DesiredCapabilities.CHROME)
        # self.driver.set_page_load_timeout(15)

    def pdfparser(self, filename):
        fp = file(filename, 'rb')
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        # Create a PDF interpreter object.
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        # Process each page contained in the document.

        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
            data =  retstr.getvalue()

        return data
        # os.remove(filename)

    def download_file(self, site):
        hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

        req = urllib2.Request(site, headers=hdr)

        try:
            page = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            print e.fp.read()
        file = open("temp_document.pdf", 'wb')
        file.write(page.read())
        file.close()

    def parse(self, response):
        self._logger.debug("crawling url {}".format(response.request.url))
        is_pdf = False
        url = response.url.lower()
        if (url[len(url)-4:] == '.pdf') or ('.pdf?' in url):
            self._logger.debug("Found a pdf file, not making a Link object")
            is_pdf = True
        else:
            if "link" not in response.meta:
                link = Link(response.url)
            else:
                link = Link(response.meta["link"])

            self._logger.debug("made the link object")
            self._logger.debug("Link created is of type " + link.type)

        cur_depth = 0
        if 'curdepth' in response.meta:
            cur_depth = response.meta['curdepth']

        # capture raw response
        item = RawResponseItem()
        # populated from response.meta
        item['appid'] = response.meta['appid']
        item['crawlid'] = response.meta['crawlid']
        item['attrs'] = response.meta['attrs']

        # populated from raw HTTP response
        item["url"] = response.request.url
        item["response_url"] = response.url
        item["status_code"] = response.status
        item["status_msg"] = "OK"
        item["response_headers"] = self.reconstruct_headers(response)
        item["request_headers"] = response.request.headers
        item["body"] = ""
        item["links"] = []

        item["is_pdf"] = str(is_pdf)

        # if response.meta["maxdepth"] == 0:
            # response.meta["maxdepth"] = maxint

        # if "maxdepth" in response.meta and cur_depth >= response.meta['maxdepth']:
        #     self._logger.debug("Not spidering links in '{}' because" \
        #                        " cur_depth={} >= maxdepth={}".format(
        #         response.url,
        #         cur_depth,
        #         response.meta['maxdepth']))
        # else:
        try:
            # self._logger.debug("Current max depth is " + str(response.meta['maxdepth']))
            if is_pdf:
                self._logger.debug("Downloading pdf file")
                self.download_file(response.url)

                self._logger.debug("Parsing pdf file")
                item["body"] = self.pdfparser("temp_document.pdf")
                self._logger.debug("Finished handling pdf file")

            else:
                self._logger.debug("About to click on link " + link.hrefAttribute)
                new_links = link.click_and_yield(self.driver)
                self._logger.debug("Just finished clicking link")
                item["body"] = link.text
                for l in new_links:
                    item["links"].append(l.hrefAttribute)
                    request = scrapy.Request(l.hrefAttribute, callback=self.parse)
                    request.meta["link"] = l
                    request.meta['priority'] = max(response.meta['priority'] - 10, 0)
                    request.meta['curdepth'] = response.meta['curdepth'] + 1
                    if 'useragent' in response.meta and \
                                    response.meta['useragent'] is not None:
                        request.headers['User-Agent'] = response.meta['useragent']

                    self._logger.debug("Making a requeset object for this newly found link: '{}'".format(request.url))
                    yield request
        except LinkException:
            self._logger.debug("Could not click link:" + str(link))

        # raw response has been processed, yield to item pipeline
        self._logger.debug("Yielding item")
        yield item
