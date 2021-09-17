import os

class CacheData:
    def __init__(self, cache_hit, last_visited, crawl_delay):
        self.cache_hit = cache_hit
        self.last_visited = last_visited
        self.crawl_delay = crawl_delay


class IOHandler:
    def __init__(self, index):
        self._index = index
        self._base_folder = os.path.dirname(os.path.abspath(__file__))


    def read_queue(self) -> []:
        raise NotImplementedError()
        return None


    def add_to_queue(self, url):
        raise NotImplementedError()


    def set_queue(self, urls):
        raise NotImplementedError()


    def erase_from_queue(self, url):
        raise NotImplementedError()


    def read_cache(self, domain) -> CacheData:
        raise NotImplementedError()
        return None


    def write_cache(self, url, last_visited, crawl_delay):
        raise NotImplementedError()

    
    def record_visit_in_cache(self, domain, last_visited):
        raise NotImplementedError()


    def add_to_pages(self, url, title):
        raise NotImplementedError()


    def count_pages(self) -> []:
        raise NotImplementedError()
        return None


    def dump(self):
        raise NotImplementedError()


        
