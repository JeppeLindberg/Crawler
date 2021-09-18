

class RequestProcessor:
    def __init__(self, io_handler):
        self._io_handler = io_handler
        self._href = []

    
    def process(self, request):
        raise NotImplementedError()
        
