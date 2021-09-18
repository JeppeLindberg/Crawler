import validators

from request_processor import RequestProcessor
from bs4 import BeautifulSoup

class TitlesProcessor(RequestProcessor):
    def __init__(self, io_handler):
        super().__init__(io_handler)

    
    def process(self, request):
        print(f"[{request.url}]")
        
        parse = BeautifulSoup(request.text, 'html.parser')

        title = None
        parse_title = parse.find('title')
        if parse_title is not None:
            if parse_title.string is not None:
                title = parse_title.string.replace("\n", "")
                
        self._io_handler.add_to_pages(request.url, title)

        self._io_handler.add_to_queue(self._find_hrefs(parse))


    def _find_hrefs(self, parse):
        urls = [a.get('href') for a in parse.find_all('a')]
        urls = [u for u in urls if (u is not None) and (validators.url(u))]
        urls = [u.split('?')[0] for u in urls]

        return urls

