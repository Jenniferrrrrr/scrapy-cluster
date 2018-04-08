from __future__ import absolute_import
import scrapy

from scrapy.http import Request
from crawling.spiders.lxmlhtml import CustomLxmlLinkExtractor as LinkExtractor
from scrapy.conf import settings

from crawling.items import RawResponseItem
from crawling.spiders.redis_spider import RedisSpider

import uuid
from bs4 import BeautifulSoup

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

    def parse(self, response):
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
        item["body"] = self.gather_text(response.body)
        item["links"] = []

        item["is_pdf"] = "False"

        # determine whether to continue spidering
        # if cur_depth >= response.meta['maxdepth']:
        #     self._logger.debug("Not spidering links in '{}' because" \
        #         " cur_depth={} >= maxdepth={}".format(
        #                                               response.url,
        #                                               cur_depth,
        #                                               response.meta['maxdepth']))
        # else:
        # we are spidering -- yield Request for each discovered link
        self._logger.debug("Making link extractor")
        link_extractor = LinkExtractor(
                        allow_domains=response.meta['allowed_domains'],
                        allow=response.meta['allow_regex'],
                        deny=response.meta['deny_regex'],
                        deny_extensions=response.meta['deny_extensions'])
        self._logger.debug("Found the following links " + str(link_extractor.extract_links(response)))
        self._logger.debug("allowed_domains=" + str(response.meta['allowed_domains']))
        self._logger.debug("allow_regex=" + str(response.meta['allow_regex']))
        self._logger.debug("deny_regex=" + str(response.meta['deny_regex']))
        self._logger.debug("deny_extensions=" + str(response.meta['deny_extensions']))

        for link in link_extractor.extract_links(response):
            # link that was discovered
            the_url = link.url
            the_url = the_url.replace('\n', '')
            item["links"].append("(url: " + the_url + ", text: " + link.text + ")")
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
