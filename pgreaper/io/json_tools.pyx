from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "json_streamer.cpp" namespace "pgreaper":
    cdef cppclass JSONStreamer:
        JSONStreamer() except +
        void feed_input(string)
        vector[string] get_json()
        
cdef class PyJSONStreamer:
    cdef JSONStreamer* c_streamer      # hold a C++ instance which we're wrapping
    def __cinit__(self):
        self.c_streamer = new JSONStreamer()
    def feed_input(self, string):
        self.c_streamer.feed_input(string)
    def get_json(self):
        return self.c_streamer.get_json()
    def __dealloc__(self):
        del self.c_streamer