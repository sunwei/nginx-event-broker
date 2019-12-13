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



=== TEST 1: enqueue t1
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
POST /processQueue?target=t1
{"data":"MESSAGE1"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain


=== TEST 2: enqueue t1 second time
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
POST /processQueue?target=t1
{"data":"MESSAGE2"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain


=== TEST 1: enqueue t2
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
POST /processQueue?target=t2
{"data":"MESSAGE1"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain


=== TEST 1: enqueue t3
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
POST /processQueue?target=t3
{"data":"MESSAGE1"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain



=== TEST 2: enqueue t3 second time
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
POST /processQueue?target=t3
{"data":"MESSAGE2"}
--- error_code: 202
--- timeout: 3
--- response_headers
Content-Type: text/plain



=== TEST 5: dequeue t1
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
GET /processQueue?target=t1
--- error_code: 200
--- timeout: 10
--- response_headers
Content-Type: text/plain
--- response_body_like eval chomp
qr/.*?\"MESSAGE1\".*/


=== TEST 6: dequeue t1 second time
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
GET /processQueue?target=t1
--- error_code: 200
--- timeout: 10
--- response_headers
Content-Type: text/plain
--- response_body_like eval chomp
qr/.*?\"MESSAGE2\".*/


=== TEST 5: dequeue t2
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
GET /processQueue?target=t2
--- error_code: 200
--- timeout: 10
--- response_headers
Content-Type: text/plain
--- response_body_like eval chomp
qr/.*?\"MESSAGE1\".*/


=== TEST 5: dequeue t3
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
GET /processQueue?target=t3
--- error_code: 200
--- timeout: 10
--- response_headers
Content-Type: text/plain
--- response_body_like eval chomp
qr/.*?\"MESSAGE1\".*/



=== TEST 6: dequeue t3 second time
--- http_config eval: $::http_config
--- config
    location /processQueue {
       ngx_event_broker $arg_target;
    }
--- request
GET /processQueue?target=t3
--- error_code: 200
--- timeout: 10
--- response_headers
Content-Type: text/plain
--- response_body_like eval chomp
qr/.*?\"MESSAGE2\".*/

