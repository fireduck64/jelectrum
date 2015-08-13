#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/env.h>
#include <leveldb/write_batch.h>

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <netinet/tcp.h>
#include <list>

using namespace std;



leveldb::DB* db;

int main(int argc, char* argv[])
{
  if (argc != 2)
  {
    cout << "Params: ./repair path_level_db" << endl;
    return -1;
  }

  
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status status = RepairDB(argv[1], options);

  cout << "Repair complete: " << status.ToString() << endl;

}

