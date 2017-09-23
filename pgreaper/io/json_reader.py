# Functions for reading JSON from Python or from a file

from pgreaper.core.table import Table
from .json_tools import PyJSONStreamer

from collections import defaultdict, deque
from json import loads, JSONDecoder
import csv

class JSONStreamingDecoder(PyJSONStreamer):
    def __init__(self, source=None):
        '''
        Args:
            source:     File-like object
                        BytesIO, or some other binary file stream
            loads:      function
                        Function for loading JSON data
        '''
        
        self._streamer = PyJSONStreamer()
        self.source = source
        self.queue = deque()
        
    def __iter__(self):
        return self
        
    def __next__(self):
        ''' Return decoded JSON objects one at a time '''
        if not self.queue:
            data = self.source.read(100000)
            if not data:
                self.source.close()
                raise StopIteration
            else:
                self._streamer.feed_input(data)
                
            for i in self._streamer.get_json():
                self.queue.append(i)
                
        return self.queue.popleft()