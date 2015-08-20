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

static const int RESULT_GOOD = 1273252631;
static const int RESULT_BAD = 9999;
static const int RESULT_NOTFOUND = 133133;


leveldb::DB* db;
leveldb::WriteOptions write_options;

void error(const char *msg)
{
  perror(msg);
  exit(1);
}

struct connection_info
{
  int fd;
};


int read_fully(int fd, char* buffer, int len)
{
  int offset = 0;
  while(offset < len)
  {
    int r = read(fd, buffer + offset, len - offset);
    if (r <= 0) return r;
    offset += r;
  }

  return offset;
}

int write_fully(int fd, const char* buffer, int len)
{
  int offset = 0;
  while(offset < len)
  {
    int r = write(fd, buffer + offset, len - offset);
    if (r < 0) cout << "errno:" << errno << " " << strerror(errno) << endl;
    if (r <= 0) return r;
    offset += r;
  }

  return offset;
}


/*
 * The data in the slice will need to be freed
 */ 
int read_slice(int fd, leveldb::Slice &s)
{
  int sz;
  if(read_fully(fd, (char*)&sz, sizeof(sz)) <= 0) return -1;

  sz = ntohl(sz);

  char* data= NULL;
  
  if (sz > 0)
  {
    data = new char[sz];

    if(read_fully(fd, data, sz) <= 0)
    {
      return -1;
    }
  }

  s = leveldb::Slice(data,sz);

  //delete data;
  return 0;
}

int write_slice(int fd, leveldb::Slice &s)
{
  int sz = htonl(s.size());

  if (write_fully(fd, (char*)&sz, sizeof(sz)) < 0) return -1;
  if (write_fully(fd, s.data(), s.size()) < 0) return -1;
  return 0;
}

leveldb::Slice cloneSlice(leveldb::Slice a)
{
  char* b = new char[a.size()];
  memcpy(b, a.data(), a.size());

  return leveldb::Slice(b, a.size());
}

void* handle_connection(void* arg)
{
  connection_info* info=(connection_info*)arg;
  int fd = info->fd;


  //FILE* f = fdopen(fd, "w+");

  // [action][options]
  // actions 
  //  1 - get (size = bytes of data) (data = key)
  //        [size][keydata]            
  //  2 - put (size = number of keyvalues)
  //        [key_size][keydata][value_size][valuedata]
  //  3 - putall
  //        [items]
  //          [key_size][keydata][value_size][valuedata] (item times)
  //  4 - getprefix
  //        [size][keydata]
  // Returns for puts: int status code, 0 = no problems
  // Return for gets: int status code, 0 = no problems, [size][valuedata]
  // Returns for getprefix: int status code
  //   [items]
  //     [key_size][keydata][value_size][valuedata] (item times)

  bool problems=false;

  while(!problems)
  {
    char action[2];

    if (read_fully(fd, action, 2) <= 0) { problems=true; break;}

    if (action[0] == 1)
    {
      leveldb::Slice key;
      if (read_slice(fd, key) < 0) { problems=true; break;}
      string value;
      leveldb::Status db_stat = db->Get(leveldb::ReadOptions(),key, &value);

      delete key.data();

      int status=RESULT_GOOD;

      if (db_stat.IsNotFound())
      {
        status=RESULT_NOTFOUND;
      }
      
      int net_status=htonl(status);
      write_fully(fd, (char*)&net_status, sizeof(net_status));

      if (status==RESULT_GOOD)
      {
        leveldb::Slice s = leveldb::Slice(value);
        if (write_slice(fd, s) < 0) { problems=true; break;}
      }

      fsync(fd);

    }
    else if (action[0] == 2)
    {
      leveldb::Slice key;
      leveldb::Slice value;

      if (read_slice(fd, key) < 0) { problems=true; break;}
      if (read_slice(fd, value) < 0) { problems=true; break;}

      leveldb::Status db_stat = db->Put(write_options,key, value);
      
      delete key.data();
      delete value.data();

      int status=RESULT_BAD;

      if (db_stat.ok())
      {
        status=RESULT_GOOD;
      }
      else
      {
        cout << db_stat.ToString() << endl;
        if (db_stat.IsCorruption() || db_stat.IsIOError()) exit(10);
      }
      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));
      fsync(fd);
      

    }
    else if (action[0] == 3)
    {
      int items;
      if(read_fully(fd, (char*)&items, sizeof(items)) <= 0) { problems=true; break;}
      items = ntohl(items);

      leveldb::WriteBatch write_batch;

      list<leveldb::Slice> slices;

      for(int i = 0; i<items; i++)
      {

        leveldb::Slice key;
        leveldb::Slice value;

        if (read_slice(fd, key) < 0) { problems=true; break;}
        if (read_slice(fd, value) < 0) { problems=true; break;}

        write_batch.Put(key, value);
        slices.push_back(key);
        slices.push_back(value);
      }
     
      if (problems) break;
      leveldb::Status db_stat = db->Write(write_options, &write_batch);

      int status=RESULT_BAD;
      if (db_stat.ok())
      {
        status=RESULT_GOOD;
      }
      else
      {
        cout << db_stat.ToString() << endl;
        if (db_stat.IsCorruption() || db_stat.IsIOError()) exit(10);
      }
 
      for(list<leveldb::Slice>::iterator I = slices.begin(); I!=slices.end(); I++)
      {
        delete I->data();
      }

      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));
      fsync(fd);

    }
    else if (action[0] == 4)
    {
      leveldb::Slice prefix;

      if (read_slice(fd, prefix) < 0) { problems=true; break;}

      list<leveldb::Slice> slices;
      slices.clear();

      leveldb::Iterator* I = db->NewIterator(leveldb::ReadOptions());

      int items=0;
      for(I->Seek(prefix); I->Valid() && I->key().starts_with(prefix) ;I->Next())
      {
      
        slices.push_back(cloneSlice(I->key()));
        slices.push_back(cloneSlice(I->value()));
        items++;
      }

      int status=RESULT_GOOD;
      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));

      items=htonl(items);
      write_fully(fd, (char*)&items, sizeof(items));

      for(list<leveldb::Slice>::iterator it = slices.begin(); it!=slices.end(); it++)
      {
        leveldb::Slice s=*it;
        if (write_slice(fd, s) < 0) { problems=true; break;}
        delete s.data();
      }

      delete prefix.data();
      delete I;
      

    }
    else
    {
      problems=true;
    }

  }
  //cout << "Closing socket with problems" << endl;
  close(fd);

}

int main(int argc, char* argv[])
{
  if (argc != 2)
  {
    cout << "Params: ./levelnet path_level_db" << endl;
    return -1;
  }

  cout << "LevelDB version " << leveldb::kMajorVersion << "." << leveldb::kMinorVersion << endl;

  
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, argv[1], &db);
  
  write_options.sync = true;

  if (!status.ok()) cerr << status.ToString() << endl;

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in serv_addr;
  int yes=1;

  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(8844);
  if (bind(sockfd, (struct sockaddr *) &serv_addr,
    sizeof(serv_addr)) < 0) 
    error("ERROR on binding");

  listen(sockfd,16);

  while(true)
  {
    socklen_t clilen;
    struct sockaddr_in cli_addr;
    clilen = sizeof(cli_addr);

    int newsockfd = accept(sockfd, 
      (struct sockaddr *) &cli_addr, 
      &clilen);

    pthread_t pt;
    connection_info info;
    info.fd = newsockfd;
    pthread_create(&pt, NULL, handle_connection, &info);


  }

}
