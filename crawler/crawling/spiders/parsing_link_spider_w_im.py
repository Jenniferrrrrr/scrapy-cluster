from __future__ import absolute_import
import scrapy

from scrapy.http import Request
from crawling.spiders.lxmlhtml import CustomLxmlLinkExtractor as LinkExtractor
from scrapy.conf import settings

from crawling.items import RawResponseItem
from crawling.spiders.redis_spider import RedisSpider

import uuid
from bs4 import BeautifulSoup

import urllib2, cookielib
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
from cStringIO import StringIO

import cssutils
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter


inline_tags = ["b", "big", "i", "small", "tt", "abbr", "acronym", "cite", "dfn",
               "em", "kbd", "strong", "samp", "var", "bdo", "map", "object", "q",
               "span", "sub", "sup"]


class ParsingLinkSpider(RedisSpider):
    '''
    A spider that walks all links from the requested URL. This is
    the entrypoint for generic crawling.
    '''
    name = "parsing_link"

    def __init__(self, *args, **kwargs):
        super(ParsingLinkSpider, self).__init__(*args, **kwargs)

    def gather_text(self, html_body):
        page_source_replaced = html_body
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
        visible_text = "\n".join(list(filter(lambda vt: vt.split() != [], visible_text)))
        return visible_text

    def get_domain(self, url):
        domain = ""
        for element in url.split(".")[:-1]:
            domain += element + "."
        domain += url.split(".")[-1].split("/")[0]
        return domain

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

    def download_image(self, domain, image_src):
        hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

        req = urllib2.Request(image_src, headers=hdr)
        
        if image_src[0] != "/":
            image_src = "/" + image_src
        rel_image_src = domain + image_src
        rel_req = urllib2.Request(rel_image_src, headers=hdr)

        try:
            page = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            print e.fp.read()
        except Exception:
            try:
                page = urllib2.urlopen(rel_req)
            except Exception:
                self._logger.debug("Could not download url: {}".format(image_src))
                return False
        file = open("temp_image.png", 'wb')
        file.write(page.read())
        file.close()
        return True

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

    # Runs OCR to get text from an image file
    def get_image_text(self):
        try:
            im = Image.open("temp_image.png") # the second one 
            im = im.filter(ImageFilter.MedianFilter())
            enhancer = ImageEnhance.Contrast(im)
            im = enhancer.enhance(2)
            im = im.convert('1')
            im.save('temp_image2.png')
        except Exception:
            self._logger.debug("Could not format downloaded image properly")
        text = pytesseract.image_to_string(Image.open('temp_image2.png'))
        return text

    # Gets list of all image url's in a page's HTML body
    def get_image_sources(self, page_body):
        soup = BeautifulSoup(page_body, "lxml")
        images = []
        # Get source urls for all img tags
        for img in soup.findAll('img'):
            images.append(img.get('src'))

        # Get source urls for div backgrounds
        for img in soup.findAll("div"):
            if "style" not in img:
                continue
            img = img["style"]
            style = cssutils.parseStyle(div_style)
            div_bkgnd_src = style['background-image']
            div_bkgnd_src = div_bkgnd_src.replace('url(', '').replace(')', '')
            images.append(div_bkgnd_src)
        return images

    # Gets image sources, downloads images, runs OCR, returns list of image texts
    def gather_image_text(self, domain, page_body):
        image_sources = self.get_image_sources(page_body)
        for image_source in image_sources:
            self._logger.debug("Found image source: {}".format(image_source))
        image_texts = []
        for image_src in image_sources:
            download_result = self.download_image(domain, image_src)
            if not download_result: # Image cannot be downloaded
                continue
            self._logger.debug("Downloaded image src: {}".format(image_src))
            image_text = self.get_image_text().encode('utf-8')
            image_texts.append(image_text)
            self._logger.debug("Found image text: {}".format(image_text))
        return image_texts

    def validate_link(self, link, orig_domain):
        link = link.lower()
        if (link[len(link)-4:] == '.pdf') or ('.pdf?' in link):
            return True
        if "calendar" in link:
            return False
        if not link.split("//")[1].startswith(orig_domain.lower().split("//")[1]):
            return False
        return True

    def parse(self, response):
        # Check url at start of parse to catch links that were potentially redirected.
        orig_domain = response.url
        if "orig_domain" in response.meta:
            orig_domain = response.meta["orig_domain"]
        else:
            response.meta["orig_domain"] = orig_domain
        if not self.validate_link(response.url, orig_domain):
            return

        self._logger.debug("starting parse on url {}".format(response.request.url))
        cur_depth = 0
        if 'curdepth' in response.meta:
            cur_depth = response.meta['curdepth']
        else:
            response.meta['curdepth'] = cur_depth
        self._logger.debug("Forming response object")
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
        item["links"] = []
        item["curdepth"] = str(cur_depth)

        is_pdf = False
        url = response.url.lower()
        if (url[len(url)-4:] == '.pdf') or ('.pdf?' in url):
            is_pdf = True

        item["is_pdf"] = str(is_pdf)
        if is_pdf:
            self._logger.debug("Handling pdf file")
            self.download_file(response.url)
            item["body"] = self.pdfparser("temp_document.pdf")
        else:
            item["body"] = self.gather_text(response.body)\
            
            # Get image texts and append to the end of the original body
            item["body"] += "\n\n\n"
            domain = self.get_domain(response.url)
            image_text = self.gather_image_text(domain, response.body)
            for it in image_text:
                item["body"] += it.decode("utf-8") + "\n"

            self._logger.debug("Current depth: " + str(cur_depth))
            # determine whether to continue spidering
            if cur_depth >= response.meta['maxdepth']:
                self._logger.debug("Not spidering links in '{}' because" \
                    " cur_depth={} >= maxdepth={}".format(
                                                          response.url,
                                                          cur_depth,
                                                          response.meta['maxdepth']))
            else:
            # we are spidering -- yield Request for each discovered link
                link_extractor = LinkExtractor(
                                allow_domains=response.meta['allowed_domains'],
                                allow=response.meta['allow_regex'],
                                deny=response.meta['deny_regex'],
                                deny_extensions=response.meta['deny_extensions'])

                for link in link_extractor.extract_links(response):
                    # link that was discovered
                    the_url = link.url
                    the_url = the_url.replace('\n', '')
                    if not self.validate_link(the_url, orig_domain):
                        continue
                    item["links"].append(str({"url": the_url, "text": link.text, }))
                    req = Request(the_url, callback=self.parse)

                    req.meta['priority'] = response.meta['priority'] - 10
                    req.meta['curdepth'] = response.meta['curdepth'] + 1

                    if 'useragent' in response.meta and \
                            response.meta['useragent'] is not None:
                        req.headers['User-Agent'] = response.meta['useragent']

                    self._logger.debug("Trying to follow link '{}'".format(req.url))
                    yield req

        # raw response has been processed, yield to item pipeline
        yield item
