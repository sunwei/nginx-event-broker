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
  ngx_str_t shm_zone_name;
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

static ngx_int_t ngx_http_event_broker_pre_conf(ngx_conf_t *cf) {
#if (NGX_THREADS)
  ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", " with aio threads feature");
#endif
  return NGX_OK;
}

static ngx_int_t ngx_http_event_broker_post_conf(ngx_conf_t *cf) {
  ngx_http_event_broker_main_conf_t *mcf;
  mcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_event_broker_module);
  
  if (mcf != NULL ) {
    ngx_http_handler_pt        *handler;
    ngx_http_core_main_conf_t  *core_main_conf;
    core_main_conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    
    handler = ngx_array_push(&core_main_conf->phases[NGX_HTTP_REWRITE_PHASE].handlers);
    if(NULL == handler){
      return NGX_ERROR;
    }
    *handler = ngx_http_event_broker_rewrite_handler;
    
#if (nginx_version > 1013003)
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", "USING NGX_HTTP_PRECONTENT_PHASE");
    handler = ngx_array_push(&core_main_conf->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);
#else
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", "USING NGX_HTTP_ACCESS_PHASE");
    handler = ngx_array_push(&core_main_conf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
#endif
    
    if(NULL == handler){
      return NGX_ERROR;
    }
    *handler = ngx_http_event_broker_precontent_handler;
  }
  
  if (mcf != NULL && !mcf->is_cache_defined ) {
    ngx_conf_log_error(NGX_LOG_DEBUG, cf,   0, "event broker, %s", "Init Default Share memory with 10mb");
    ngx_str_t default_size = ngx_string("10M");
    ngx_shm_zone_t *shm_zone = ngx_shared_memory_add(cf, &mcf->shm_ctx->shm_zone_name, ngx_parse_size(&default_size), &ngx_http_event_broker_module);
    if(NULL == shm_zone){
      ngx_conf_log_error(NGX_LOG_EMERG, cf,  0, "event broker, %s", "Unable to allocate memory with specified size");
      return NGX_ERROR;
    }
    shm_zone->init = ngx_http_event_broker_shm_init;
    shm_zone->data = mcf->shm_ctx;
  }
  
  return NGX_OK;
}

static ngx_int_t ngx_http_event_broker_rewrite_handler(ngx_http_request_t *r) {
  
  ngx_http_event_broker_loc_conf_t  *lcf = ngx_http_get_module_loc_conf(r, ngx_http_event_broker_module);
  ngx_http_event_broker_main_conf_t *mcf = ngx_http_get_module_main_conf(r, ngx_http_event_broker_module);
  ngx_http_event_broker_ctx_t *ctx;
  ngx_int_t req_conf;
  abqueue_t *targeted_q = NULL;
  ngx_str_t target_topic;

  if (ngx_http_complex_value(r, &lcf->target_topic$, &target_topic) != NGX_OK) {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "%s", "No target topic set");
  } 
  
  if (target_topic.len == 0) {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "%s", " No target found ");
  } else {
    ngx_http_event_broker_node_t *node = node_lookup(mcf, target_topic);
    if(node){
      targeted_q = node->topic_ctx->event_q;
    }
  }

  if (r->method & (NGX_HTTP_POST | NGX_HTTP_PUT | NGX_HTTP_PATCH)) {
    ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);

    if (ctx != NULL) {
      if(ctx->done){
        return NGX_DECLINED;
      } else {
        return NGX_DONE;
      }
    }

    ctx = create_eb_context(r);
    if (ctx == NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Insufficient Memory to create event broker context structure");
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    init_eb_context(ctx, r, targeted_q, &target_topic);
    ngx_http_set_ctx(r, ctx, ngx_http_event_broker_module);

    if(NULL == ctx->targeted_topic_q) {
      return NGX_DECLINED;
    }

    rc = ngx_http_read_client_request_body(r, ngx_http_eb_client_body_handler);

    if (rc == NGX_ERROR || rc >= NGX_HTTP_SPECIAL_RESPONSE) {
#if (nginx_version < 1002006) || (nginx_version >= 1003000 && nginx_version < 1003009)
      r->main->count--;
#endif
      return rc;
    }

    if (rc == NGX_AGAIN) {
      ctx->waiting_more_body = 1;
      return NGX_DONE;
    }
    return NGX_DECLINED;
    
  } else {
    ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
    if (ctx == NULL) {
      ctx = create_eb_context(r);
        if (ctx == NULL) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Insufficient Memory to create event broker context structure");
          return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        init_eb_context(ctx, r, targeted_q, &target_topic);
        ngx_http_set_ctx(r, ctx, ngx_http_event_broker_module);
    }
    return NGX_DECLINED;
  }
}

ngx_http_event_broker_ctx_t* create_eb_context(ngx_http_request_t *r){
  return ngx_pcalloc(r->pool, sizeof(ngx_http_event_broker_ctx_t));
}

void init_eb_context(ngx_http_event_broker_ctx_t *ctx, ngx_http_request_t *r, abqueue_t *targeted_q, ngx_str_t *target_topic){
  ctx->r = r;
  ctx->req_conf = NGX_CONF_UNSET;
  ctx->targeted_topic_q = targeted_q;
  ctx->target_topic.data = target_topic->data;
  ctx->target_topic.len = target_topic->len;
}

static void ngx_http_eb_client_body_handler(ngx_http_request_t *r) {
  ngx_http_event_broker_ctx_t *ctx;
  ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
  ctx->done = 1;

#if defined(nginx_version) && nginx_version >= 8011
  r->main->count--;
#endif
  if (ctx->waiting_more_body) {
    ctx->waiting_more_body = 0;
    ngx_http_core_run_phases(r);
  }
}

static ngx_int_t ngx_http_event_broker_module_init(ngx_cycle_t *cycle) {
  ngx_core_conf_t *ccf;
  ngx_http_event_broker_main_conf_t *mcf;
  ngx_http_conf_ctx_t *ctx;
  ngx_http_event_broker_shm_t *shm;
  ngx_str_t *topic_s;
  ngx_http_event_broker_node_t *node;
  abqueue_t *topic_q;
  
  ccf = (ngx_core_conf_t *)ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  check_worker_processes(ccf);
  
  ctx = (ngx_http_conf_ctx_t *)ngx_get_conf(cycle->conf_ctx, ngx_http_module);
  mcf = ctx->main_conf[ngx_http_event_broker_module.ctx_index];
  if(mcf->topics == NGX_CONF_UNSET_PTR || mcf->topics->nelts <= 0){
    return NGX_OK;
  }
  
  check_nginx_aio()
  
  for (i = 0; i < mcf->topics->nelts; i++) {
    topic_s = mcf->topics->elts + i;
    node = node_lookup(mcf, topic_s);
    
    if (node) {
      topic_q = node->topic_ctx->event_q;
      if(NULL != topic_q){
        topic_q.mpl = mcf->shared_ctx->shared_mem;
      } else {
        break;
      }
//    TODO
  }
}


void check_worker_processes(ngx_core_conf_t *ccf){
  if(ccf->worker_processes > 1){
      ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0, "%s", "1 worker_processes suggested");
  }
}

void check_nginx_aio(void){
#if (NGX_THREADS)
  ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " aio threads enabled for event broker module ");
#endif
}

ngx_http_event_broker_node_t* node_lookup(ngx_http_event_broker_main_conf_t *mcf, ngx_str_t *s){
  uint32_t hash = ngx_crc32_long(s->data, s->len);
  ngx_http_event_broker_shm_t *shm = mcf->shared_ctx->shared_mem;
  ngx_http_event_broker_node_t *node;
  
  node = (ngx_http_event_broker_node_t *)ngx_str_rbtree_lookup(&shm->rbtree, s, hash);
  
  return node;
}