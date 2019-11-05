#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <abq/abqueue.h>

#define MODULE_NAME "ngx_event_broker"
#define MAX_DEQ_TRY 1000
#define MAX_SIZE_DIGIT_TRNFM 128
#define ABQUEUE_DATA_FILE "ngx_event_broker_store_data.txt"

typedef struct {
  ngx_str_t   topic;
  ngx_array_t *subscriber_urls;
  abqueue_t event_q;
} ngx_http_event_broker_topic_ctx_t;

typedef struct {
  ngx_str_node_t                    sn;
  ngx_http_event_broker_topic_ctx_t *topic_ctx;
} ngx_http_event_broker_node_t;

typedef struct {
  ngx_rbtree_t  rbtree;
  ngx_rbtree_node_t sentinel;
  ngx_slab_pool_t *shpool;
} ngx_http_event_broker_shm_t;

typedef struct {
  ngx_str_t shm_zone;
  ngx_http_event_broker_shm_t *shared_mem;
} ngx_http_event_broker_shm_ctx_t;

typedef struct {
  ngx_http_event_broker_shm_ctx_t *shm_ctx;
  ngx_array_t                     *topics;
  ngx_str_t                       saved_path;
  ngx_str_t                       split_delim;
} ngx_http_event_broker_main_conf_t;

typedef struct {
  ngx_http_complex_value_t target_topic$;
} ngx_http_event_broker_loc_conf_t;

typedef struct {
  unsigned done: 1;
  unsigned waiting_more_body: 1;
  ngx_int_t req_conf;
  ngx_http_event_broker_shm_t *shared_mem;
  union {
    ngx_str_t payload;
    ngx_str_t response;
  };
  ngx_http_request_t *r;
  ngx_str_t target_topic;
  abqueue_t *targeted_topic_q;
} ngx_http_event_broker_ctx_t;


static ngx_command_t ngx_http_event_broker_commands[] = {
  {
    ngx_string("ngx_event_broker_memory_allocate"),
    NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
    ngx_http_event_broker_set_shm_size_cmd,
    NGX_HTTP_MAIN_CONF_OFFSET,
    0,
    NULL
  },
  {
    ngx_string("ngx_event_broker_topic"),
    NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_str_array_slot,
    NGX_HTTP_MAIN_CONF_OFFSET,
    offsetof(ngx_http_event_broker_main_conf_t, topics),
    NULL
  },
  {
    ngx_string("ngx_event_broker_publish"),
    NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_http_event_broker_publish_cmd,
    NGX_HTTP_LOC_CONF_OFFSET,
    0,
    NULL
  },
  {
    ngx_string("ngx_event_broker_subscribe"),
    NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
    ngx_http_event_broker_subscribe_cmd,
    NGX_HTTP_LOC_CONF_OFFSET,
    0,
    NULL
  },
  {  ngx_string("ngx_event_broker_store"),
    NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE12,
    ngx_http_event_broker_store_cmd,
    NGX_HTTP_MAIN_CONF_OFFSET,
    0,
    NULL
  },
  ngx_null_command /* command termination */
};

static ngx_http_module_t ngx_http_event_broker_module_ctx = {
  ngx_http_event_broker_pre_conf,
  ngx_http_event_broker_post_conf,

  ngx_http_event_broker_create_main_conf,
  ngx_http_event_broker_init_main_conf,

  NULL, /* create server configuration */
  NULL, /* merge server configuration */

  ngx_http_event_broker_create_loc_conf,
  ngx_http_event_broker_merge_loc_conf
};

ngx_module_t ngx_http_event_broker_module = {
  NGX_MODULE_V1,
  &ngx_http_event_broker_module_ctx,
  ngx_http_event_broker_commands,
  NGX_HTTP_MODULE,
  NULL, /* init master */
  ngx_http_event_broker_module_init, /* init module */
  NULL, /* init process */
  NULL, /* init thread */
  NULL, /* exit thread */
  NULL, /* exit process */
  ngx_http_event_broker_module_exit, /* exit master */
  NGX_MODULE_V1_PADDING
};