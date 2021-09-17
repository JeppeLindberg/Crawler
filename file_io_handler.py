import os
import pickle

from io_handler import IOHandler
from io_handler import CacheData
from filelock import FileLock

queue_file = "queue.dat"
cache_file = "cache.dat"
pages_file = "pages.txt"


class FileHandler(IOHandler):
    def __init__(self, index):
        super().__init__(index)

        self._bin_path = f"{self._base_folder}\\bin"
        self._index_path = f"{self._bin_path}\\{self._index}"
        self._queue_file = f"{self._index_path}\\{queue_file}"
        self._cache_file = f"{self._bin_path}\\{cache_file}"
        self._pages_file = f"{self._base_folder}\\{pages_file}"

        self._queue = ["https://www.aau.dk/"]
        self._cache = {}

        if not os.path.exists(self._index_path):
            os.makedirs(self._index_path)

        if not os.path.exists(self._queue_file):
            f = open(self._queue_file, "xb")
            pickle.dump(self._queue, f)
            f.close()

        with FileLock(f"{self._cache_file}.lock"):
            if not os.path.exists(self._cache_file):
                f = open(self._cache_file, "xb")
                pickle.dump(self._cache, f)
                f.close()

        with FileLock(f"{self._pages_file}.lock"):
            if not os.path.exists(self._pages_file):
                f = open(self._pages_file, "xb")
                pickle.dump(self._cache, f)
                f.close()


    def read_queue(self) -> []:
        if len(self._queue) == 0:
            f = open(self._queue_file, 'rb')
            self._queue = pickle.load(f)
            f.close()

        return self._queue


    def add_to_queue(self, url):
        self._queue.insert(0, url)


    def set_queue(self, urls):
        self._queue = urls


    def erase_from_queue(self, url):
        raise NotImplementedError()


    def read_cache(self, domain) -> CacheData:
        with FileLock(f"{self._cache_file}.lock"):
            f = open(self._cache_file, 'rb')
            self._cache = pickle.load(f)
            f.close()

        cache_hit = None
        last_visited = None
        crawl_delay = None

        if domain in self._cache:
            cache_hit = True

            if "last_visited" in self._cache[domain]:
                last_visited = self._cache[domain]["last_visited"]
            if "crawl_delay" in self._cache[domain]:
                crawl_delay = self._cache[domain]["crawl_delay"]
        return CacheData(cache_hit, last_visited, crawl_delay)


    def write_cache(self, url, last_visited, crawl_delay):
        with FileLock(f"{self._cache_file}.lock"):
            f = open(self._cache_file, 'rb')
            self._cache = pickle.load(f)
            f.close()

            self._cache[url] = {"last_visited": last_visited, "crawl_delay": crawl_delay}
            
            f = open(self._cache_file, 'wb')
            pickle.dump(self._cache, f)
            f.close()

    
    def record_visit_in_cache(self, domain, last_visited):
        with FileLock(f"{self._cache_file}.lock"):
            f = open(self._cache_file, 'rb')
            self._cache = pickle.load(f)
            f.close()

            self._cache[url]["last_visited"] = last_visited
            
            f = open(self._cache_file, 'wb')
            pickle.dump(self._cache, f)
            f.close()


    def add_to_pages(self, url_title):
        with FileLock(f"{self._pages_file}.lock"):
            f = open(self._pages_file, "a", encoding="utf-8")
            f.write(url_title + "\n")
            f.close()


    def read_pages(self) -> []:
        with FileLock(f"{self._pages_file}.lock"):
            f = open(self._pages_file, "r", encoding="utf-8")
            lines = f.readlines()
            f.close()
        
        return lines


    def dump(self):
        self._dump_queue()
        #self._dump_cache()


    def _dump_queue(self):
        f = open(self._queue_file, 'wb')
        pickle.dump(self._queue, f)
        f.close()


    def _dump_cache(self):
        f = open(self._cache_file, 'wb')
        pickle.dump(self._cache, f)
        f.close()


        
