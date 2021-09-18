import validators

from request_processor import RequestProcessor
from bs4 import BeautifulSoup

class IndexProcessor(RequestProcessor):
    def __init__(self, io_handler):
        super().__init__(io_handler)

    
    def process(self, request):
        raise NotImplementedError()
