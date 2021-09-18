import requests
import time

from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from urllib.error import URLError

from file_io_handler import FileHandler
from sql_io_handler import SQLHandler
from io_handler import CacheData
from titles_request_processor import TitlesProcessor


class Crawler:
    def __init__(self, index):
        self._index = index
        self._useragent = "jwli"

        self._io_handler = SQLHandler(index)
        self._rp = RobotFileParser()
        self._request_processor = TitlesProcessor(self._io_handler)

        self._stop_crawl = False


    def crawl(self):
        print(f"Crawler {self._index} started")

        while True:
            url = self._get_next_url()
            if url == None:
                time.sleep(0.1)
                continue

            self._process_url(url)

            self._io_handler.dump()

            if self._check_stop_crawling():
                break

            time.sleep(1.0)

        print(f"Crawler {self._index} terminated")


    def stop_crawl(self):
        self._stop_crawl = True


    def _process_url(self, url):
        self._record_visit(url)

        try:
            req = requests.get(url)
        except:
            return

        self._request_processor.process(req)


    def _get_next_url(self):
        lines = self._io_handler.read_queue()

        for line in lines:
            allow_crawl = self._get_allow_crawl(line)

            if allow_crawl == -1:
                self._io_handler.erase_from_queue(line)
            if allow_crawl == 1:
                self._io_handler.erase_from_queue(line)
                return line

        return None


    def _get_domain(self,url):
        netloc = urlparse(url).netloc
        split = netloc.split(".")
        domain = split[len(split) - 2] + "." + split[len(split) - 1]

        return domain


    def _get_allow_crawl(self, url):
        cache_data = self._io_handler.read_cache(self._get_domain(url))

        if cache_data.cache_hit:
            if time.time() > cache_data.last_visited + cache_data.crawl_delay:
                return 1
        else:
            self._rp.set_url(url)
            try:
                self._rp.read()
                if not self._rp.can_fetch(self._useragent,url):
                    return -1
            except:
                return -1

            crawl_delay = self._rp.crawl_delay(self._useragent)
            if crawl_delay == None:
                crawl_delay = 5

            self._io_handler.write_cache(self._get_domain(url), time.time(), crawl_delay)
            return 0
        
        return 0


    def _record_visit(self,url):
        self._io_handler.record_visit_in_cache(self._get_domain(url), time.time())


    def _check_stop_crawling(self):
        return self._stop_crawl or (self._io_handler.count_pages() >= 1000)
