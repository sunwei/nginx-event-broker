use lib '/home/booking/nginx_build/test-nginx/inc';
use lib '/home/booking/nginx_build/test-nginx/lib';
use strict;
use warnings FATAL => 'all';
use Test::Nginx::Socket 'no_plan';


our $http_config = <<'_EOC_';
    ngx_event_broker_memory_allocate 10m;
    ngx_event_broker_topic t1;
    ngx_event_broker_topic t2;
    ngx_event_broker_topic t3;
_EOC_

no_shuffle();
run_tests();


__DATA__



=== TEST 1: enqueue q1
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker_publish $arg_target;
    }
--- request
POST /processQueue?target=t1
{"data":"MESSAGE1"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain
