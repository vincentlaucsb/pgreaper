// g++ -std=c++11 -o test json_sstreamer.cpp
// ./test

#include <iostream>
#include <vector>
#include <string>

using namespace std;

namespace pgreaper {
    // Parses JSON one object at a time
    class JSONStreamer {
        public:
            void feed_input(string in);
            vector<string> get_json();
        private:
            bool currently_parsing = false;
            string current_string;
            vector<string> queue;
            int left_braces = 0;
            int right_braces = 0;
    };

    // Member functions
    void JSONStreamer::feed_input(string in) {
        // Parse JSON by counting left and right braces
        // This implementation also has the nice side effect of ignoring 
        // newlines and spacing between braces
        
        char ch;
        
        for (int i = 0; i < in.length(); i++) {
            ch = in[i];
            
            if (ch == '{') {
                this->currently_parsing = true;
                this->left_braces++;
            }
            else if (ch == '}') {
                this->right_braces++;
                
                if (this->left_braces == this->right_braces) {
                    this->current_string += ch;
                    this->queue.push_back(this->current_string);
                    this->current_string.clear();
                    this->currently_parsing = false;
                }
            }
            
            if (this->currently_parsing) {
                this->current_string += in[i];
            }
        }
    }

    // Return current queue and clear it
    vector<string> JSONStreamer::get_json() {
        vector<string> temp = this->queue;
        this->queue.clear();
        return temp;
    }
}