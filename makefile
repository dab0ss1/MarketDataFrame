FILES = main.cpp
LIBS = -lboost_date_time

all: DataFrameTest

DataFrameTest: $(FILES)
	g++ -O3 -std=c++11 -Wall -Wextra $(FILES) $(LIBS) -o DataFrameTest

clean:
	rm -f *.o DataFrameTest
