
all: levelnet repair lmdbnet bloomtime

levelnet: levelnet.cpp
	g++ -Wall -O2 levelnet.cpp -lleveldb -lpthread -o levelnet
lmdbnet: lmdbnet.cpp
	g++ -Wall -O2 lmdbnet.cpp -llmdb -lpthread -o lmdbnet
repair: repair.cpp
	g++ -Wall -O2 repair.cpp -lleveldb -o repair

bloomtime: bloomtime.cpp
	g++ -Wall -O2 bloomtime.cpp -lpthread -o bloomtime

clean:
	rm -rf levelnet repair lmdbnet
