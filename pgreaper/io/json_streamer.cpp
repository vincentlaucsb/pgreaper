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
            void feed_input(string &in);
            vector<string> get_json();
        private:
            bool currently_parsing = false;
            bool in_string = false;
            bool escape_quote = false;
            string current_string;
            vector<string> queue;
            int left_braces = 0;
            int right_braces = 0;
    };

    // Member functions
    void JSONStreamer::feed_input(string &in) {
        // Parse JSON by counting left and right braces
        // This implementation also has the nice side effect of ignoring 
        // newlines and spacing between braces
        
        for (size_t i = 0, ilen = in.length(); i < ilen; i++) {          
            switch(in[i]) {
                case '{':
                    if (!this->in_string) {
                        this->currently_parsing = true;
                        this->left_braces++;
                    }
                    
                    break;
                case '}':
                    if (!this->in_string) {
                        this->right_braces++;
                    
                        if (this->left_braces == this->right_braces) {
                            this->current_string += in[i];
                            this->queue.push_back(this->current_string);
                            this->current_string.clear();
                            this->currently_parsing = false;
                            this->left_braces = 0;
                            this->right_braces = 0;
                        }
                    }
                    
                    break;
                case '\\':
                    if (in[i + 1] == '"') {
                        // Flag should be unset when quote is read
                        this->escape_quote = true;
                    }
                    break;
                case '"':
                    // Begin value                
                    if (this->escape_quote) {
                        this->escape_quote = false;
                    } else if (this->in_string) {
                        this->in_string = false;
                    } else {
                        this->in_string = true;
                    }
                    
                    break;
                default:
                    break;
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