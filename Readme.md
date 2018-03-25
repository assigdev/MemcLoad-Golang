# MemcLoad Golang

Otus homework project

### install:

clone repository:

    $ git clone https://github.com/assigdev/MemcLoad-Golang
    
build program:

    go build

### Run


up 4 memcashed in docker
    
    docker-compose up

start program
    
    ./gocurrency
    
### Configs

     -adid string
            memcash  ip:port for android adid (default "127.0.0.1:33015")
      -dry
            Debug mode
      -dvid string
            memcash  ip:port for android dvid (default "127.0.0.1:33016")
      -gaid string
            memcash  ip:port for android gaid (default "127.0.0.1:33014")
      -idfa string
            memcash  ip:port for iphone ids (default "127.0.0.1:33013")
      -pattern string
            Pattern for files path (default "/home/assig/pysrc/otus/12_concurrency/concurrency/data2/*.tsv.gz")
      -retryCount int
            count of retry set data in memcache (default 5)

