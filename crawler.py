import requests
import time
import validators

from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from urllib.error import URLError

from file_io_handler import FileHandler
from sql_io_handler import SQLHandler
from io_handler import CacheData


class Crawler:
    def __init__(self, index):
        self._index = index
        self._useragent = "jwli"

        self._io_handler = SQLHandler(index)
        self._rp = RobotFileParser()


    def crawl(self):
        while True:
            url = self._get_next_url()
            if url == None:
                time.sleep(2.0)
                continue

            try:
                r = requests.get(url)
            except:
                continue
                
            self._record_visit(url)
            r_parse = BeautifulSoup(r.text, 'html.parser')
            r_title = r_parse.find('title')
            if r_title == None:
                continue

            r_title_string = r_title.string
            if r_title_string is None:
                continue

            self._add_links_to_queue(r_parse)

            url_title = ("[" + url + "] " + r_title_string).replace("\n", "")
            self._io_handler.add_to_pages(url_title)
            print(url_title)

            self._io_handler.dump()

            if self._check_stop_crawling():
                print("Crawl finished!")
                return

            time.sleep(2.0)


    def _get_next_url(self):
        lines = self._io_handler.read_queue()
        found_line = None

        for line in lines:
            allow_crawl = self._get_allow_crawl(line)

            if allow_crawl == -1:
                self._io_handler.erase_from_queue(line)
            if allow_crawl == 1:
                self._io_handler.erase_from_queue(line)
                return line

        return None

    
    def _add_links_to_queue(self, r_parse):
        urls = [a.get('href') for a in r_parse.find_all('a')]
        urls = [u for u in urls if (u is not None) and (validators.url(u))]

        for url in urls:
            self._io_handler.add_to_queue(url)


    def _get_domain(self,url):
        netloc = urlparse(url).netloc
        split = netloc.split(".")
        domain = split[len(split) - 2] + "." + split[len(split) - 1]

        return domain


    def _get_allow_crawl(self,url):
        cache_data = self._io_handler.read_cache(self._get_domain(url))
        allow_delay = False

        if cache_data.cache_hit:
            allow_delay = time.time() > cache_data.last_visited + cache_data.crawl_delay
            
        self._rp.set_url(url)
        try:
            self._rp.read()
            if not self._rp.can_fetch(self._useragent,url):
                return -1
        except:
            return -1

        if not cache_data.cache_hit:
            crawl_delay = self._rp.crawl_delay(self._useragent)
            if crawl_delay == None:
                crawl_delay = 5

            self._io_handler.write_cache(self._get_domain(url), 0, crawl_delay)
            allow_delay = False
        
        if allow_delay == True:
            return 1
        return 0


    def _record_visit(self,url):
        self._io_handler.record_visit_in_cache(self._get_domain(url), time.time())


    def _check_stop_crawling(self):
        return len(self._io_handler.read_pages()) >= 1000
