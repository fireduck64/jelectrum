
#include <sys/stat.h>
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
#include <lmdb.h>
#include <strings.h>
#include <string.h>

using namespace std;

static const int RESULT_GOOD = 1273252631;
static const int RESULT_BAD = 9999;
static const int RESULT_NOTFOUND = 133133;
static const int RESULT_TOO_MANY = 28365921;
static const char OP_MAX_RESULTS = 1;


MDB_env *env;
MDB_dbi dbi;

pthread_mutex_t db_lock;


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
int read_slice(int fd, MDB_val &s)
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

  s.mv_size = sz;
  s.mv_data = data;

  return 0;
}

int write_slice(int fd, MDB_val &s)
{
  int sz = htonl(s.mv_size);

  if (write_fully(fd, (char*)&sz, sizeof(sz)) < 0) return -1;
  if (write_fully(fd, (const char*)s.mv_data, s.mv_size) < 0) return -1;
  return 0;
}

bool startsWith(MDB_val prefix, MDB_val key)
{
  if (key.mv_size < prefix.mv_size) return false;
  
  return (strncmp((char*)prefix.mv_data, (char*)key.mv_data, prefix.mv_size) == 0);
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
  //  5 - ping
  // Returns for puts: int status code, 0 = no problems
  // Return for gets: int status code, 0 = no problems, [size][valuedata]
  // Returns for getprefix: int status code
  //   [items]
  //     [key_size][keydata][value_size][valuedata] (item times)

  bool problems=false;
  string pr="";

  while(!problems)
  {
    char action[2];

    if (read_fully(fd, action, 2) <= 0) { problems=true; pr="action_read"; break;}

    if (action[0] == 1)
    {
      MDB_val key;
      if (read_slice(fd, key) < 0) { problems=true; pr="get_key_read"; break;}

      //pthread_mutex_lock(&db_lock);

      MDB_val value;

      MDB_txn *txn;
      mdb_txn_begin(env,NULL,MDB_RDONLY,&txn);
      int get_return = mdb_get(txn, dbi, &key, &value);
      mdb_txn_commit(txn);

      //pthread_mutex_unlock(&db_lock);

      int status=RESULT_GOOD;

      if (get_return == 0)
      {
        status = RESULT_GOOD;
      }
      else if (get_return == MDB_NOTFOUND)
      {
        status=RESULT_NOTFOUND;
      }
      else
      {
        cout << "DB get error: " << mdb_strerror(get_return) << endl;
        problems=true;
        pr="get_db_error";
        status=RESULT_BAD;
      }

      
      int net_status=htonl(status);
      write_fully(fd, (char*)&net_status, sizeof(net_status));

      if (status==RESULT_GOOD)
      {
        if (write_slice(fd, value) < 0) { problems=true; pr="write_get_slice"; break;}
      }

      delete[] (char*) key.mv_data;
      fsync(fd);

    }
    else if (action[0] == 2)
    {
      MDB_val key;
      MDB_val value;

      if (read_slice(fd, key) < 0) { problems=true; pr="read_put_key"; break;}
      if (read_slice(fd, value) < 0) { problems=true; pr="read_put_value"; break;}

      MDB_txn *txn;
      mdb_txn_begin(env,NULL,0,&txn);
      int put_return = mdb_put(txn, dbi, &key, &value,0);
      mdb_txn_commit(txn);
      
      delete[] (char*) key.mv_data;
      delete[] (char*) value.mv_data;

      int status=RESULT_BAD;

      if (put_return == 0)
      {
        status=RESULT_GOOD;
      }
      else
      {
        cout << "DB put error: " << mdb_strerror(put_return) << endl;
        problems=true;
        pr="db_put_error";
        status=RESULT_BAD;
      }
      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));
      fsync(fd);
      

    }
    else if (action[0] == 3)
    {
      int items;
      if(read_fully(fd, (char*)&items, sizeof(items)) <= 0) { problems=true; pr="putall_read_item_count"; break;}
      items = ntohl(items);

      int status=RESULT_GOOD;

      MDB_txn *txn;
      mdb_txn_begin(env,NULL,0,&txn);
      for(int i = 0; i<items; i++)
      {
        MDB_val key;
        MDB_val value;

        if (read_slice(fd, key) < 0) { problems=true; break;}
        if (read_slice(fd, value) < 0) { problems=true; break;}

        int put_return = mdb_put(txn, dbi, &key, &value,0);
        
        delete[] (char*) key.mv_data;
        delete[] (char*) value.mv_data;
        if (put_return != 0)
        {
          cout << "DB put error: " << mdb_strerror(put_return) << endl;
          problems=true;
          pr="db_put_error";
          status=RESULT_BAD;
          break;
        }

      }

      mdb_txn_commit(txn);
     
      if (problems) break;

      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));
      fsync(fd);

    }
    else if (action[0] == 4)
    {
      int max_results = 1000000000;
      if (action[1] & OP_MAX_RESULTS)
      { 
        if(read_fully(fd, (char*)&max_results, sizeof(max_results)) <= 0) { problems=true; break;}
        max_results = ntohl(max_results);
      }

      MDB_val prefix;

      if (read_slice(fd, prefix) < 0) { problems=true; break;}

      int items=0;

      MDB_txn *txn;
      MDB_cursor *cursor;

      mdb_txn_begin(env,NULL,MDB_RDONLY,&txn);
      mdb_cursor_open(txn,dbi,&cursor);

      MDB_val key = prefix;
      MDB_val value;

      list<MDB_val> slices;
      int ret = mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE);
      if (ret == MDB_NOTFOUND)
      {
      }
      else if (ret != 0)
      {
        cout << "DB cursor error: " << mdb_strerror(ret) << endl;
        problems=true;
        pr="db_cursor_error";
      }

      int status=RESULT_GOOD;

      if (ret == 0)
      while(startsWith(prefix, key))
      {
        slices.push_back(key);
        slices.push_back(value);
        items++;
        if (items > max_results)
        {
          status=RESULT_TOO_MANY;
          
          break;
        }
 
        ret = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
        if (ret == MDB_NOTFOUND)
        {
          break;
        }
        if (ret != 0)
        {
          cout << "DB cursor error: " << mdb_strerror(ret) << endl;
          problems=true;
          pr="db_cursor_error";
          break;
        }


      }

      mdb_cursor_close(cursor);
      mdb_txn_abort(txn);

      delete[] (char*) prefix.mv_data;

      if (status==RESULT_GOOD)
      {

        status=htonl(status);
        write_fully(fd, (char*)&status, sizeof(status));

        items=htonl(items);
        write_fully(fd, (char*)&items, sizeof(items));

        for(list<MDB_val>::iterator it = slices.begin(); it!=slices.end(); it++)
        {
          MDB_val s=*it;
          if (write_slice(fd, s) < 0) { problems=true; pr="get_prefix_write_slice"; break;}
        }
      }
      else
      {

        status=htonl(status);
        write_fully(fd, (char*)&status, sizeof(status));
      }

    }
    else if (action[0] == 5)
    {
      int status=RESULT_GOOD;
      status=htonl(status);
      write_fully(fd, (char*)&status, sizeof(status));
    }
    else
    {
      problems=true;
      pr="unexpected action: " + (int)action[0];
    }

  }

  cout << "closing socket with "<<  pr << endl;
  close(fd);
  return NULL;

}

int main(int argc, char* argv[])
{
  if ((argc < 2) || (argc > 3))
  {
    cout << "Params: ./levelnet path_level_db [port]" << endl;
    return -1;
  }
  short port = 8844;
  if (argc==3)
  {
    port = atoi(argv[2]);
  }
  cout << MDB_VERSION_STRING << endl;

  pthread_mutex_init(&db_lock, NULL);

  mkdir(argv[1], 0775);

  mdb_env_create(&env);
  size_t k = 1024;
  size_t max_size = k * k * k * k; //1 TB
  mdb_env_set_mapsize(env, max_size);
  mdb_env_open(env, argv[1], 0, 0644);

  MDB_txn *txn;
  mdb_txn_begin(env,NULL,0,&txn);
  mdb_open(txn,NULL,0,&dbi);
  mdb_txn_commit(txn);

  //cout << "LevelDB version " << leveldb::kMajorVersion << "." << leveldb::kMinorVersion << endl;

  
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in serv_addr;
  int yes=1;

  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(port);
  if (bind(sockfd, (struct sockaddr *) &serv_addr,
    sizeof(serv_addr)) < 0) 
    error("ERROR on binding");

  listen(sockfd,128);

  while(true)
  {
    socklen_t clilen;
    struct sockaddr_in cli_addr;
    clilen = sizeof(cli_addr);

    int newsockfd = accept(sockfd, 
      (struct sockaddr *) &cli_addr, 
      &clilen);

    struct timeval timeout;      
      timeout.tv_sec = 3600;
      timeout.tv_usec = 0;

    setsockopt(newsockfd, SOL_SOCKET, SO_RCVTIMEO, (void *)&timeout,
                    sizeof(timeout));

    pthread_t pt;
    connection_info info;
    info.fd = newsockfd;
    pthread_create(&pt, NULL, handle_connection, &info);


  }

}
