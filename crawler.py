import requests
import time
import validators

from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from urllib.error import URLError

from io_handler import IOHandler
from io_handler import CacheData


class Crawler:
    def __init__(self, index):
        self._index = index

        self._io_handler = IOHandler(index)

        self._useragent = "jwli"

        self._base_folder = "C:\\Users\\Jeppe\\Documents\\Python\\Crawler"
        self._pages = self._base_folder + "\\pages.txt"
        self._already_visited = self._base_folder + "\\already_visited.txt"

        self._rp = RobotFileParser()


    def crawl(self):
        while True:

            url = self._get_next_url()
            if url == None:
                time.sleep(0.1)
                continue

            time.sleep(0.01)

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

            for a in r_parse.find_all('a'):
                try:
                    self._io_handler.add_to_queue(a['href'])
                except KeyError:
                    __ = False

            url_title = ("[" + url + "] " + r_title_string).replace("\n", "")
            self._append_to_file(self._pages, url_title)
            print(url_title)

            self._io_handler.dump()

            if self._stop_crawling():
                print("Crawl finished!")
                return


    def _get_next_url(self):
        lines = self._io_handler.read_queue()

        for i in range(len(lines)-1, -1, -1):
            allow_crawl = self._get_allow_crawl(lines[i])

            if allow_crawl == "-1":
                lines.pop(i)
            if allow_crawl == "1":
                return lines.pop(i)

        self._io_handler.set_queue(lines)
        return None


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
                return "-1"
        except:
            return "-1"

        if not cache_data.cache_hit:
            crawl_delay = self._rp.crawl_delay(self._useragent)
            if crawl_delay == None:
                crawl_delay = 5

            self._io_handler.write_cache(self._get_domain(url), 0, crawl_delay)
            allow_delay = False
        
        if allow_delay == True:
            return "1"
        return "0"


    def _record_visit(self,url):
        self._io_handler.record_visit_in_cache(self._get_domain(url), time.time())


    def _stop_crawling(self):
        f = open(self._pages, "r", encoding="utf-8")
        lines = f.readlines()
        f.close()

        return len(lines) >= 10:


    def _append_to_file(self,file_path, string):
        f = open(file_path, "a", encoding="utf-8")
        f.write(string + "\n")
        f.close()
