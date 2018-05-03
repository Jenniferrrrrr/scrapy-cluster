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
            item["body"] = self.gather_text(response.body)
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
