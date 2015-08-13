
all: levelnet repair

levelnet: levelnet.cpp
	g++ -O2 levelnet.cpp -lleveldb -lpthread -o levelnet

repair: repair.cpp
	g++ -O2 repair.cpp -lleveldb -o repair

clean:
	rm -rf levelnet repair
