import os
import pickle

from filelock import FileLock

queue_file = "queue.dat"
cache_file = "cache.dat"

class CacheData:
    def __init__(self, cache_hit, last_visited, crawl_delay):
        self.cache_hit = cache_hit
        self.last_visited = last_visited
        self.crawl_delay = crawl_delay


class IOHandler:
    def __init__(self, index):
        self._index = index

        self._base_folder = os.path.dirname(os.path.abspath(__file__))
        self._bin_path = f"{self._base_folder}\\bin"
        self._index_path = f"{self._bin_path}\\{self._index}"
        self._queue_file = f"{self._index_path}\\{queue_file}"
        self._cache_file = f"{self._bin_path}\\{cache_file}"

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


    def read_queue(self) -> []:
        if len(self._queue) == 0:
            f = open(self._queue_file, 'rb')
            self._queue = pickle.load(f)
            f.close()

        return self._queue


    def add_to_queue(self, line):
        self._queue.insert(0, line)


    def set_queue(self, lines):
        self._queue = lines


    def read_cache(self, url) -> CacheData:
        with FileLock(f"{self._cache_file}.lock"):
            f = open(self._cache_file, 'rb')
            self._cache = pickle.load(f)
            f.close()

        cache_hit = None
        last_visited = None
        crawl_delay = None

        if url in self._cache:
            cache_hit = True

            if "last_visited" in self._cache[url]:
                last_visited = self._cache[url]["last_visited"]
            if "crawl_delay" in self._cache[url]:
                crawl_delay = self._cache[url]["crawl_delay"]
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

    
    def record_visit_in_cache(self, url, last_visited):
        with FileLock(f"{self._cache_file}.lock"):
            f = open(self._cache_file, 'rb')
            self._cache = pickle.load(f)
            f.close()

            self._cache[url]["last_visited"] = last_visited
            
            f = open(self._cache_file, 'wb')
            pickle.dump(self._cache, f)
            f.close()


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


        
