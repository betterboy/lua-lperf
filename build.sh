rm *.so
g++ -std=c++11 -ggdb -O2 -Wall -fPIC -shared -o lperf.so -llua lperf.cpp