# nginx-event-broker
Nginx lightweight event broker

0 downtime, reloadable, focus on event broker, notify once, support event sourcing 

```nginx

http {
  ngx_event_broker_memory_allocate 10m;
  ngx_event_broker_topic t1;
  ngx_event_broker_topic t2;
  ngx_event_broker_topic t3;
  ngx_event_broker_store |@| /tmp/ngx_event_broker_store_data.txt;
  ...

  server {
    location /event-broker-publish {
       ngx_event_broker_publish $arg_event;
    }
    
    location /event-broker-subscribe {
       ngx_event_broker_subscribe $arg_topic;
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
cd nginx-1.17.3/

./configure --add-module=/path/to/ngx_event_broker

make -j2
sudo make install
```
