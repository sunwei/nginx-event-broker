# nginx-event-broker
Nginx lightweight event broker

0 downtime, reloadable, focus on event broker, notify once, support event sourcing 

```nginx

http {
  ngx_event_broker_memory_allocate 10m;
  ngx_event_broker_topic t1;
  ngx_event_broker_topic t2;
  ngx_event_broker_topic t3;
  ...

  server {
    location /processQueue {
      ngx_event_broker_publish $arg_target;
    }
    
    ...
  }
  ...
}

```

Installation
============

```bash
wget 'http://nginx.org/download/nginx-1.17.3.tar.gz'
tar -xzvf nginx-1.17.3.tar.gz

curl -OL https://ftp.pcre.org/pub/pcre/pcre-8.41.tar.gz
tar -xvzf pcre-8.41.tar.gz

cd nginx-1.17.3/

# ./configure --add-module=/path/to/ngx_event_broker
./configure --add-module=/Users/wwsun/Sunzhongmou/github/nginx-event-broker --with-pcre=../pcre-8.41

# curl -OL https://www.openssl.org/source/openssl-1.1.0g.tar.gz
# tar xvzf openssl-1.1.0g.tar.gz && rm openssl-1.1.0g.tar.gz 
# ./configure --add-module=/Users/wwsun/Playground/ngx_lfqueue --with-pcre=./pcre-8.41/ --with-http_ssl_module --with-openssl=/usr/local/src/openssl-1.1.0g

sudo make -j2
sudo make install

export PATH="/usr/local/nginx/sbin:$PATH" 
curl http://localhost

```

Test
====

```bash
# Install test module
sudo cpan Test::Nginx
cd /path/to/nginx-event-broker
sudo prove t
```