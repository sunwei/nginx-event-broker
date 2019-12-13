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
      ngx_event_broker $arg_target;
    }
    
    ...
  }
  ...
}

```

Installation
============

```bash
make nginx
make pcre
make ssl

# For static configuration with pcre only
# There is `conf-ssl` depends on your requirement
make conf

make build
```

Test
====

```bash
# Install test module
make install-test

export PATH="/usr/local/nginx/sbin:$PATH";
make test
```