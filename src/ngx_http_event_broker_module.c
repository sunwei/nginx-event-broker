#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <abq/abqueue.h>

#define MODULE_NAME "ngx_event_broker"
#define MAX_DEQ_TRY 1000
#define MAX_SIZE_DIGIT_TRNFM 128
#define EB_DATA_FILE "/tmp/ngx_event_broker_store_data.txt"
#define SPLIT_DELIM "_@_"

typedef struct {
  u_char *data;
  size_t len;
} ngx_http_event_broker_msg_t;

typedef struct {
  ngx_str_t   topic;
  abqueue_t   event_q;
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

static void *ngx_http_eb_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_eb_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);
static void * ngx_http_eb_create_main_conf(ngx_conf_t *cf);
static char * ngx_http_eb_init_main_conf(ngx_conf_t *cf, void *conf);
static ngx_int_t ngx_http_eb_pre_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_eb_post_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_eb_rewrite_handler(ngx_http_request_t *r);
void init_eb_context(ngx_http_event_broker_ctx_t *ctx, ngx_http_request_t *r, abqueue_t *targeted_q, ngx_str_t *target_topic);
static ngx_int_t ngx_http_eb_precontent_handler(ngx_http_request_t *r);
void get_message_from_payload(ngx_http_event_broker_msg_t *message, ngx_str_t *payload);
#if (NGX_THREADS)
static void ngx_http_eb_process_t_handler(void *data, ngx_log_t *log);
static void ngx_http_eb_after_t_handler(ngx_event_t *ev);
#endif
static void ngx_http_eb_process(ngx_http_request_t *r, ngx_http_event_broker_ctx_t *ctx);
static void ngx_http_eb_output_filter(ngx_http_request_t *r);
ngx_str_t* get_request_body(ngx_http_request_t *r);
static void ngx_http_eb_client_body_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_event_broker_module_init(ngx_cycle_t *cycle);
ngx_int_t restore_events(ngx_http_event_broker_main_conf_t *mcf, ngx_cycle_t *cycle);
ngx_int_t restore_topic_ctx(ngx_http_event_broker_main_conf_t *mcf, ngx_cycle_t *cycle);
static void ngx_http_event_broker_module_exit(ngx_cycle_t *cycle);
ngx_int_t backup_data_store(ngx_array_t *data_store, ngx_cycle_t *cycle);
ngx_str_t* get_delim_event_key(ngx_cycle_t *cycle);
ngx_str_t* get_delim_topic_key(ngx_cycle_t *cycle);
void check_worker_processes(ngx_core_conf_t *ccf, ngx_cycle_t *cycle);
void check_nginx_aio(void);
ngx_http_event_broker_node_t* node_lookup(ngx_http_event_broker_main_conf_t *mcf, ngx_str_t *s);
static u_char* get_if_contain(u_char *start, u_char *end, u_char *delim_s, size_t delim_len);
ngx_int_t ngx_http_eb_shm_init(ngx_shm_zone_t *shm_zone, void *data);
static char* ngx_http_eb_set_shm_size_cmd(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_eb_publish_cmd(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


static inline void* ngx_eb_alloc(void *pl, size_t sz) {
  return ngx_slab_alloc( ((ngx_http_event_broker_shm_t*)pl)->shpool, sz);
}

static inline void ngx_eb_free(void *pl, void *ptr) {
  ngx_slab_free( ((ngx_http_event_broker_shm_t*)pl)->shpool, ptr);
}

static ngx_command_t ngx_http_event_broker_commands[] = {
  {
    ngx_string("ngx_event_broker_memory_allocate"),
    NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
    ngx_http_eb_set_shm_size_cmd,
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
    ngx_http_eb_publish_cmd,
    NGX_HTTP_LOC_CONF_OFFSET,
    0,
    NULL
  },
  ngx_null_command /* command termination */
};

static ngx_http_module_t ngx_http_event_broker_module_ctx = {
  ngx_http_eb_pre_conf,
  ngx_http_eb_post_conf,

  ngx_http_eb_create_main_conf,
  ngx_http_eb_init_main_conf,

  NULL, /* create server configuration */
  NULL, /* merge server configuration */

  ngx_http_eb_create_loc_conf,
  ngx_http_eb_merge_loc_conf
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

static void *ngx_http_eb_create_loc_conf(ngx_conf_t *cf) {
  ngx_http_event_broker_loc_conf_t  *conf;
  
  conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_event_broker_loc_conf_t));
  if (conf == NULL) {
    return NULL;
  }
  ngx_memzero(&conf->target_topic$, sizeof(ngx_http_complex_value_t));
  
  return conf;
} 

static char *ngx_http_eb_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
  return NGX_CONF_OK;
}

static void * ngx_http_eb_create_main_conf(ngx_conf_t *cf) {
  ngx_http_event_broker_main_conf_t *mcf;
  
  mcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_event_broker_main_conf_t));
  if (mcf == NULL) {
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", "main configuration alloc error");
    return NGX_CONF_ERROR;
  }

  mcf->shm_ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_event_broker_shm_ctx_t));
  if (mcf->shm_ctx == NULL) {
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", "shared memory context alloc error");
    return NGX_CONF_ERROR;
  }    

  ngx_str_set(&mcf->shm_ctx->shm_zone_name , "ngx_eb_shm_capacity");

  mcf->shm_ctx->shared_mem = NULL;
  mcf->topics = NGX_CONF_UNSET_PTR;
  mcf->saved_path.len = 0;

  return mcf;
}

static char * ngx_http_eb_init_main_conf(ngx_conf_t *cf, void *conf) {
  return NGX_CONF_OK;
}

static ngx_int_t ngx_http_eb_pre_conf(ngx_conf_t *cf) {
#if (NGX_THREADS)
  ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", " with aio threads feature");
#endif
  return NGX_OK;
}

static ngx_int_t ngx_http_eb_post_conf(ngx_conf_t *cf) {
  ngx_http_event_broker_main_conf_t *mcf;
  mcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_event_broker_module);
  
  if (mcf != NULL ) {
    ngx_http_handler_pt        *handler;
    ngx_shm_zone_t             *shm_zone;
    ngx_http_core_main_conf_t  *core_main_conf;
    core_main_conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    
    handler = ngx_array_push(&core_main_conf->phases[NGX_HTTP_REWRITE_PHASE].handlers);
    if(NULL == handler){
      return NGX_ERROR;
    }
    *handler = ngx_http_eb_rewrite_handler;
    
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "event broker, %s", "USING NGX_HTTP_PRECONTENT_PHASE");
    handler = ngx_array_push(&core_main_conf->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);
    
    if(NULL == handler){
      return NGX_ERROR;
    }
    *handler = ngx_http_eb_precontent_handler;
    
    ngx_conf_log_error(NGX_LOG_DEBUG, cf,   0, "event broker, %s", "Init Default Share memory with 10mb");
    ngx_str_t default_size = ngx_string("10M");
    shm_zone = ngx_shared_memory_add(cf, &mcf->shm_ctx->shm_zone_name, ngx_parse_size(&default_size), &ngx_http_event_broker_module);
    if(NULL == shm_zone){
      ngx_conf_log_error(NGX_LOG_EMERG, cf,  0, "event broker, %s", "Unable to allocate memory with specified size");
      return NGX_ERROR;
    }
    shm_zone->init = ngx_http_eb_shm_init;
    shm_zone->data = mcf->shm_ctx;
  }
  
  return NGX_OK;
}

static ngx_int_t ngx_http_eb_rewrite_handler(ngx_http_request_t *r) {
  
  ngx_http_event_broker_main_conf_t *mcf;
  ngx_http_event_broker_loc_conf_t  *lcf;
  ngx_http_event_broker_ctx_t *ctx;
  ngx_int_t req_conf;
  abqueue_t *targeted_q = NULL;
  ngx_str_t target_topic;
  
  mcf = ngx_http_get_module_main_conf(r, ngx_http_event_broker_module);
  lcf = ngx_http_get_module_loc_conf(r, ngx_http_event_broker_module);

  ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "%s", "eb : in rewrite handler...");
  
  if (ngx_http_complex_value(r, &lcf->target_topic$, &target_topic) != NGX_OK) {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "%s", "No target topic set");
  }
  
  ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "eb : topic name \"%V\"", &target_topic);
  
  if (target_topic.len == 0) {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "eb : %s", " No target found ");
  } else {
    ngx_http_event_broker_node_t *node = node_lookup(mcf, &target_topic);
    if(node){
      targeted_q = &node->topic_ctx->event_q;
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

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_event_broker_ctx_t));
    if (ctx == NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Insufficient Memory to create event broker context structure");
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    init_eb_context(ctx, r, targeted_q, &target_topic);
    ngx_http_set_ctx(r, ctx, ngx_http_event_broker_module);
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, " eb : %s", "context setup");
    if(NULL == ctx->targeted_topic_q) {
      return NGX_DECLINED;
    }

    req_conf = ngx_http_read_client_request_body(r, ngx_http_eb_client_body_handler);

    if (req_conf == NGX_ERROR || req_conf >= NGX_HTTP_SPECIAL_RESPONSE) {
      return req_conf;
    }

    if (req_conf == NGX_AGAIN) {
      ctx->waiting_more_body = 1;
      return NGX_DONE;
    }
    return NGX_DECLINED;
    
  } else {
    ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
    if (ctx == NULL) {
      ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_event_broker_ctx_t));
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

void init_eb_context(ngx_http_event_broker_ctx_t *ctx, ngx_http_request_t *r, abqueue_t *targeted_q, ngx_str_t *target_topic){
  ctx->r = r;
  ctx->req_conf = NGX_CONF_UNSET;
  ctx->targeted_topic_q = targeted_q;
  ctx->target_topic.data = target_topic->data;
  ctx->target_topic.len = target_topic->len;
}

static ngx_int_t ngx_http_eb_precontent_handler(ngx_http_request_t *r) {
  ngx_http_event_broker_main_conf_t *mcf;
  ngx_http_event_broker_ctx_t       *ctx;

  mcf = ngx_http_get_module_main_conf(r, ngx_http_event_broker_module);
  ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
  
  if (ctx == NULL) {
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "eb : error while processing request");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  } else if (ctx->targeted_topic_q == NULL) {
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "eb : request not for event broker");
    return NGX_DECLINED;
  }

  if (ctx->req_conf != NGX_CONF_UNSET) {
    ngx_http_eb_output_filter(r);
    return NGX_DONE;
  }

  ctx->shared_mem = mcf->shm_ctx->shared_mem;
  ctx->req_conf = NGX_HTTP_INTERNAL_SERVER_ERROR;

  if (r->method & (NGX_HTTP_POST)) {
    ngx_str_t *body = NULL;
    
    body = get_request_body(r);

    if (body) {
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "request_content=%*s \n", body->len, body);
      ctx->payload.data = body->data;
      ctx->payload.len = body->len;
    } else {
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "%s\n", "No data received to enqueue");
      return NGX_HTTP_BAD_REQUEST;
    }
  } else {
    if (ngx_http_discard_request_body(r) != NGX_OK) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
  }

#if(NGX_THREADS)
  ngx_thread_pool_t                 *tp;
  ngx_http_core_loc_conf_t          *clcf;
    
  clcf  = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  tp = clcf->thread_pool;

  if (tp == NULL) {
    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "processing with single thread, enable \"aio threads;\" in server/loc block for concurrent request");
    goto single_thread;
    
  } else {
    ngx_thread_task_t *task = ngx_thread_task_alloc(r->pool, sizeof(ngx_http_request_t));
    ngx_memcpy(task->ctx, r, sizeof(ngx_http_request_t));
    task->handler = ngx_http_eb_process_t_handler;
    task->event.data = r;
    task->event.handler = ngx_http_eb_after_t_handler;
    
    if(ngx_thread_task_post(tp, task) != NGX_OK) {
        return NGX_ERROR;
    }
    r->main->blocked++;
    r->aio = 1;
    return NGX_DONE;
  }
single_thread:
#endif

  ngx_http_eb_process(r, ctx);
  ngx_http_eb_output_filter(r);
      
  return NGX_DONE;
}

void get_message_from_payload(ngx_http_event_broker_msg_t *message, ngx_str_t *payload){
  message->data = ((u_char*)message) + sizeof(ngx_http_event_broker_msg_t);
  message->len = payload->len;
  ngx_memcpy(message->data, payload->data, payload->len);
}

#if (NGX_THREADS)
static void ngx_http_eb_process_t_handler(void *data, ngx_log_t *log){
  ngx_http_event_broker_ctx_t *ctx;
  ngx_http_request_t *r = data;
  
  ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
  
  ngx_http_eb_process(r, ctx);
}

static void ngx_http_eb_after_t_handler(ngx_event_t *ev) {
  ngx_connection_t    *c;
  ngx_http_request_t  *r;
  
  r = ev->data;
  c = r->connection;
  
  ngx_http_set_log_request(c->log, r);
  
  r->main->blocked--;
  r->aio = 0;
  
  r->write_event_handler(r);
  ngx_http_run_posted_requests(c);
}
#endif

static void ngx_http_eb_process(ngx_http_request_t *r, ngx_http_event_broker_ctx_t *ctx){
  ngx_http_event_broker_msg_t *message;
  ngx_str_t *payload = &ctx->payload;
  ngx_uint_t i;
  u_char *res_msg;
  
  if (r->method & (NGX_HTTP_POST | NGX_HTTP_PUT | NGX_HTTP_PATCH)) {
    message = ngx_slab_alloc(ctx->shared_mem->shpool, sizeof(ngx_http_event_broker_msg_t) + payload->len);
    if(NULL == message){
      ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, " No enough share memory for message");
      ctx->req_conf = NGX_HTTP_INTERNAL_SERVER_ERROR;
      return;
    }
    
    get_message_from_payload(message, payload);
    payload->len = 0;
    
    abqueue_enq(ctx->targeted_topic_q, message);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "abqueue_enq : %s", message->data);
    
    ctx->req_conf = NGX_HTTP_ACCEPTED;
  } else {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, " %s", "dequeueing in every 10 sec");
    for(i=0; i < MAX_DEQ_TRY; i++){
      if((message = abqueue_deq(ctx->targeted_topic_q))){
        if(message->len){
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "abqueue_deq : %s", message->data);
          res_msg = ngx_palloc(r->pool, message->len);
          ngx_memcpy(res_msg, message->data, message->len);
          ctx->response.data = res_msg;
          ctx->response.len = message->len;
          ngx_slab_free(ctx->shared_mem->shpool, message);
          ctx->req_conf = NGX_HTTP_OK;
          return;
        }
      } else {
        ngx_msleep(10);
      }
    }
    ctx->req_conf = NGX_HTTP_NO_CONTENT;
    return;
  }
}

static void ngx_http_eb_output_filter(ngx_http_request_t *r) {
  ngx_http_event_broker_ctx_t *ctx;
  ngx_str_t *response;
  size_t resp_len;
  ngx_int_t rc;
  ngx_buf_t *buf;
  ngx_chain_t output;
  
  ctx = ngx_http_get_module_ctx(r, ngx_http_event_broker_module);
  if(NULL == ctx){
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "Session invalid");
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  if(ctx->req_conf == NGX_HTTP_INTERNAL_SERVER_ERROR){
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "Server error");
    ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  response = &ctx->response;
  r->headers_out.status = ctx->req_conf;
  r->headers_out.content_type.len = sizeof("text/plain") - 1;
  r->headers_out.content_type.data = (u_char *)"text/plain";
  
  if((resp_len = response->len)){
    r->headers_out.content_length_n = resp_len;
    rc = ngx_http_send_header(r);
    if(rc == NGX_ERROR){
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "response processing failed.");
      rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return;
    }
    
    buf = ngx_create_temp_buf(r->pool, resp_len);
    buf->last = ngx_copy(buf->last, response->data, resp_len);
    buf->memory = 1; /*read only*/
    buf->last_buf = 1;
    
    output.buf = buf;
    output.next = NULL;
    ngx_http_finalize_request(r, ngx_http_output_filter(r, &output));
  } else {
    r->headers_out.content_length_n = 0;
    r->header_only = 1;
    ngx_http_finalize_request(r, ngx_http_send_header(r));
  }
}

ngx_str_t* get_request_body(ngx_http_request_t *r){
  u_char         *body, *tmp_body = NULL;
  size_t         len;
  ngx_chain_t    *bufs;
  ngx_buf_t      *buf;
  ngx_str_t      *res_body;
  
  if(r->request_body != NULL && r->request_body->bufs != NULL) {
    len = 0;
    for(bufs=r->request_body->bufs; bufs; bufs=bufs->next){
      if(bufs->buf->in_file){
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "insufficient client_body_buffer_size");
        return NULL;
      }
      len += bufs->buf->last - bufs->buf->pos;
    }
    if(len > 0){
      body = ngx_palloc(r->pool, len);
      if(body){
        tmp_body = body;
        for(bufs=r->request_body->bufs; bufs; bufs=bufs->next){
          tmp_body = ngx_copy(tmp_body, bufs->buf->pos, bufs->buf->last - bufs->buf->pos);
        }
      } else {
        ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "insufficient memory.");
        return NULL;
      }
    }
  } else {
    buf = r->request_body->bufs->buf;
    if((len = ngx_buf_size(buf)) != 0){
      body = ngx_palloc(r->pool, len);
      ngx_memcpy(body, buf->pos, len);
    } else {
      return NULL;
    }
  }
  
  res_body = ngx_palloc(r->pool, sizeof(ngx_str_t));
  if(NULL == res_body){
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "insufficient memory for request body");
    return NULL;
  }
  
  res_body->len = len;
  res_body->data = ngx_pcalloc(r->pool, len);
  ngx_memcpy(res_body->data, body, len);
  
  return res_body;  
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
  ngx_str_t *topic_s, *qstr;
  ngx_http_event_broker_node_t *node;
  abqueue_t *topic_q;
  ngx_uint_t queue_initialized = 0;
  ngx_uint_t i;
  
  ccf = (ngx_core_conf_t *)ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  check_worker_processes(ccf, cycle);
  
  ctx = (ngx_http_conf_ctx_t *)ngx_get_conf(cycle->conf_ctx, ngx_http_module);
  mcf = ctx->main_conf[ngx_http_event_broker_module.ctx_index];
  if(mcf->topics == NGX_CONF_UNSET_PTR || mcf->topics->nelts <= 0){
    return NGX_OK;
  }
  
  check_nginx_aio();
  
  qstr = mcf->topics->elts;
  for (i = 0; i < mcf->topics->nelts; i++) {
    topic_s = qstr + i;
    node = node_lookup(mcf, topic_s);
    
    if (node) {
      topic_q = &node->topic_ctx->event_q;
      if(NULL != topic_q){
        topic_q->mpl = mcf->shm_ctx->shared_mem;
        queue_initialized = 1;
      } else {
        break;
      }
    }
  }
  
  if(0 == queue_initialized){
    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " Initializing event broker queues");
    if(NGX_OK == restore_topic_ctx(mcf, cycle)){
      if(NGX_OK == restore_events(mcf, cycle)){
        ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " event broker has been restored");
      } else {
        return NGX_ERROR;
      }
    } else {
      return NGX_ERROR;
    }
  }
  
  ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " event broker has been initialized");
  return NGX_OK; 
}

ngx_int_t restore_events(ngx_http_event_broker_main_conf_t *mcf, ngx_cycle_t *cycle){
  u_char *local_file_path, *file_content, *pflip, *pend, *p_tmp;
  ngx_fd_t        read_fd;
  ngx_file_info_t fi;
  off_t           store_sz;
  ngx_uint_t      decode_len;
  ngx_str_t       plain_data, encoded_data;
  ngx_str_t       *delim_event_key, *delim_topic_key, *topic, *qstr;
  ngx_array_t     *topic_arr;
  uintptr_t       *topic_p;
  ngx_uint_t      i, j;
  uint32_t        hash;
  ngx_http_event_broker_node_t *node;
  ngx_http_event_broker_msg_t  *event;
  abqueue_t       *event_q;
  
  local_file_path = (u_char *)ngx_palloc(cycle->pool, sizeof(EB_DATA_FILE));
  ngx_copy(local_file_path, EB_DATA_FILE, sizeof(EB_DATA_FILE));
  
  read_fd = ngx_open_file(local_file_path, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
  if(NGX_INVALID_FILE != read_fd){
    if(NGX_FILE_ERROR != ngx_fd_info(read_fd, &fi)){
      if((store_sz = ngx_file_size(&fi))){
        ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " restore events read file size %O bytes ", store_sz);
        
        file_content = (u_char*) ngx_pcalloc(cycle->pool, store_sz);
        if(read(read_fd, file_content, store_sz) == -1) {
          ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "read local backup file \"%s\" failed", local_file_path);
          return NGX_ERROR;
        } else if(NGX_FILE_ERROR == ngx_close_file(read_fd) || NGX_FILE_ERROR == ngx_delete_file(local_file_path)){
          ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "unable to close / remove local data file %s", local_file_path);
        }
        
        ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "restoring... ");
        encoded_data.len = store_sz;
        encoded_data.data = file_content;
        decode_len = ngx_base64_decoded_length(store_sz);
        plain_data.len = decode_len;
        plain_data.data = (u_char *)ngx_pcalloc(cycle->pool, decode_len);
        ngx_decode_base64(&plain_data, &encoded_data);
        
        pflip = plain_data.data;
        pend = pflip + plain_data.len;
        
        delim_event_key = get_delim_event_key(cycle);
        delim_topic_key = get_delim_topic_key(cycle);
        
        topic_arr = ngx_array_create(cycle->pool, 128, sizeof(uintptr_t));
        p_tmp = pflip;
        while ((p_tmp = get_if_contain(p_tmp, pend, delim_topic_key->data, delim_topic_key->len))) {
          topic_p = ngx_array_push(topic_arr);
          p_tmp = p_tmp + delim_topic_key->len;
          *topic_p = (uintptr_t)(u_char*)p_tmp;
        }
        
        topic_p = (uintptr_t*)topic_arr->elts;
        for(i=0; i<topic_arr->nelts; i++){
          pflip = (u_char*)topic_p[i];
          if((i + 1) == topic_arr->nelts){
            pend = plain_data.data + plain_data.len;
          } else {
            pend = (u_char*)topic_p[i + 1];
            pend -= delim_topic_key->len;
          }
          
          qstr = mcf->topics->elts;
          for(j=0; j < mcf->topics->nelts; j++){
            ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "eb : topic name %s -- %z", "456=???", j);
            if((p_tmp = get_if_contain(pflip, pend, delim_event_key->data, delim_event_key->len))){
              topic = qstr + j;
              ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "eb : topic222 name \"%V\"", topic);
              
              if(topic->len == (size_t)(p_tmp - pflip) && ngx_strncmp(topic->data, pflip, (p_tmp - pflip)) == 0) {
                hash = ngx_crc32_long(topic->data, topic->len);
                node = (ngx_http_event_broker_node_t *)ngx_str_rbtree_lookup(&mcf->shm_ctx->shared_mem->rbtree, topic, hash);
                if(node){
                  event_q = &node->topic_ctx->event_q;
                  if(NULL != event_q){
                    pflip = p_tmp + delim_event_key->len;
                    
                    while((p_tmp = get_if_contain(pflip, pend, delim_event_key->data, delim_event_key->len))){
                      event = ngx_slab_alloc(mcf->shm_ctx->shared_mem->shpool, sizeof(ngx_http_event_broker_msg_t) + (p_tmp - pflip));
                      if(NULL == event){
                        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, " not enough share memory given for event message");
                        return NGX_ERROR;
                      }

                      event->data = ((u_char*)event) + sizeof(ngx_http_event_broker_msg_t);
                      event->len = p_tmp - pflip;
                      ngx_memcpy(event->data, pflip, event->len);

                      ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "eb : message size2 %z", event->len);
                      abqueue_enq(event_q, event);
                      ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "abqueue_enq : %s", event->data);
                      pflip = p_tmp + delim_event_key->len;
                    }
                    
                    event = ngx_slab_alloc(mcf->shm_ctx->shared_mem->shpool, sizeof(ngx_http_event_broker_msg_t) + (pend - pflip));
                    if(NULL == event){
                      ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, " not enough share memory given for event message");
                      return NGX_ERROR;
                    }
                    
                    event->data = ((u_char*)event) + sizeof(ngx_http_event_broker_msg_t);
                    event->len = pend - pflip;
                    ngx_memcpy(event->data, pflip, event->len);
                    
                    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "eb : message size3 %z", event->len);
                    abqueue_enq(event_q, event);
                    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "abqueue_enq : %s", event->data);
                  }
                }
              }
            }
          }
        }
        
        ngx_pfree(cycle->pool, delim_topic_key->data);
        ngx_pfree(cycle->pool, delim_event_key->data);
        ngx_pfree(cycle->pool, file_content);
        ngx_pfree(cycle->pool, plain_data.data);
        ngx_array_destroy(topic_arr);
        
        ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, " backup data %s has been restored ", local_file_path);
      }
    }
  }
  
  ngx_pfree(cycle->pool, local_file_path);
  return NGX_OK;
}

ngx_int_t restore_topic_ctx(ngx_http_event_broker_main_conf_t *mcf, ngx_cycle_t *cycle){
  ngx_uint_t i;
  ngx_str_t *topic, *qstr;
  ngx_http_event_broker_node_t *node;
  ngx_http_event_broker_topic_ctx_t *topic_ctx;
  
  topic_ctx = ngx_slab_calloc(mcf->shm_ctx->shared_mem->shpool, mcf->topics->nelts * sizeof(ngx_http_event_broker_topic_ctx_t));
  if(NULL == topic_ctx){
    ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, " share memory allocation topic_ctx error ");
    return NGX_ERROR;
  }
  
  qstr = mcf->topics->elts;
  for(i = 0; i < mcf->topics->nelts; i++){
    topic = qstr + i;
    
    node = (ngx_http_event_broker_node_t *)ngx_slab_alloc(mcf->shm_ctx->shared_mem->shpool, sizeof(ngx_http_event_broker_node_t));
    if(NULL == node){
      ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, " share memory allocation error for http event broker node");
      return NGX_ERROR;
    }
    
    if(-1 == abqueue_init(&topic_ctx[i].event_q, mcf->shm_ctx->shared_mem, ngx_eb_alloc, ngx_eb_free)){
      ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, " abqueue initializing error... ");
      return NGX_ERROR;
    }
    
    ngx_str_t *str_key = &(node->sn.str);
    str_key->len = topic->len;
    str_key->data = (u_char*)ngx_slab_alloc(mcf->shm_ctx->shared_mem->shpool, sizeof(u_char) * (str_key->len + 1));
    ngx_memcpy(str_key->data, topic->data, str_key->len);
    str_key->data[str_key->len] = 0;
    
    topic_ctx[i].topic.len = str_key->len;
    topic_ctx[i].topic.data = (u_char*)ngx_slab_alloc(mcf->shm_ctx->shared_mem->shpool, sizeof(u_char) * str_key->len);
    ngx_memcpy(topic_ctx[i].topic.data, str_key->data, topic_ctx[i].topic.len);
    
    uint32_t hash = ngx_crc32_long(topic_ctx[i].topic.data, topic_ctx[i].topic.len);
    node->sn.node.key = hash;
    node->topic_ctx = &topic_ctx[i];
    
    ngx_rbtree_insert(&mcf->shm_ctx->shared_mem->rbtree, &node->sn.node);
  }
  
  return NGX_OK;
}

static void ngx_http_event_broker_module_exit(ngx_cycle_t *cycle){
  ngx_http_conf_ctx_t *ctx;
  ngx_http_event_broker_main_conf_t *mcf;
  ngx_str_t *delim_event_key, *delim_topic_key;
  ngx_str_t *f_topic, *topic;
  ngx_uint_t i;
  uint32_t hash;
  ngx_http_event_broker_node_t *node;
  abqueue_t *queue;
  ngx_array_t *data_store;
  u_char *p_char;
  ngx_http_event_broker_msg_t *event;
  
  ctx = (ngx_http_conf_ctx_t *)ngx_get_conf(cycle->conf_ctx, ngx_http_module);
  if(NULL == ctx){
    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "event broker module exit in error");
    return;
  }
  
  ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "eb: exiting...");
  
  mcf = ctx->main_conf[ngx_http_event_broker_module.ctx_index];
  if(mcf->topics->nelts > 0){
    delim_event_key = get_delim_event_key(cycle);
    delim_topic_key = get_delim_topic_key(cycle);
    data_store = ngx_array_create(cycle->pool, 1024, sizeof(u_char));
    
    f_topic = mcf->topics->elts;
    for(i = 0; i < mcf->topics->nelts; i++){
      topic = f_topic + i;
      
      hash = ngx_crc32_long(topic->data, topic->len);
      node = (ngx_http_event_broker_node_t *)ngx_str_rbtree_lookup(&mcf->shm_ctx->shared_mem->rbtree, topic, hash);
      if(node){
        queue = &node->topic_ctx->event_q;
        
        if((abqueue_size(queue)) > 0){
          p_char = ngx_array_push_n(data_store, delim_topic_key->len + topic->len);
          p_char = ngx_copy(p_char, delim_topic_key->data, delim_topic_key->len);
          p_char = ngx_copy(p_char, topic->data, topic->len);
        } else {
          continue;
        }
        
        while((event = abqueue_deq(queue))){
          p_char = ngx_array_push_n(data_store, delim_event_key->len + event->len);
          p_char = ngx_copy(p_char, delim_event_key->data, delim_event_key->len);
          p_char = ngx_copy(p_char, event->data, event->len);
        }
      }
    }
    if(data_store->nelts > 0){
      ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "event broker backup size %O bytes", data_store->nelts);
      if(NGX_OK == backup_data_store(data_store, cycle)){
        return;
      } else {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Error while storing backup data file, unable to write to file");
      }
    } else {
      ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "No data to backup");
    }
  } else {
    ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "No topic found to backup");
  }
}

ngx_int_t backup_data_store(ngx_array_t *data_store, ngx_cycle_t *cycle){
  u_char *local_file_path;
  FILE *local_file;
  ngx_uint_t encoded_len;
  
  local_file_path = (u_char *)ngx_palloc(cycle->pool, sizeof(EB_DATA_FILE));
  ngx_copy(local_file_path, EB_DATA_FILE, sizeof(EB_DATA_FILE));
  
  local_file = fopen((char *)local_file_path, "w");
  if(NULL != local_file){
    ngx_str_t encoded_data, plain_data;
    
    plain_data.data = (u_char *)data_store->elts;
    plain_data.len = data_store->nelts;
    
    encoded_len = ngx_base64_encoded_length(plain_data.len);
    encoded_data.data = (u_char *)ngx_pcalloc(cycle->pool, encoded_len);
    encoded_data.len = encoded_len;
    
    ngx_encode_base64(&encoded_data, &plain_data);
    
    if(fwrite(encoded_data.data, encoded_data.len, 1, local_file) == 1){
      ngx_log_error(NGX_LOG_INFO, cycle->log, 0, "Data has been successfully saved to %s", local_file_path);
      return NGX_OK;
    }
  }
  
  return NGX_ERROR;
}

ngx_str_t* get_delim_event_key(ngx_cycle_t *cycle){
  ngx_str_t *delim_event_key;
  
  delim_event_key = ngx_palloc(cycle->pool, sizeof(ngx_str_t));
  if(NULL == delim_event_key){
    ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, "insufficient memory for delim event key");
    return NULL;
  }
  
  delim_event_key->len = (sizeof(SPLIT_DELIM) - 1) + (sizeof("e@") - 1);
  delim_event_key->data = ngx_pcalloc(cycle->pool, delim_event_key->len);
  ngx_memcpy(delim_event_key->data, SPLIT_DELIM, sizeof(SPLIT_DELIM) - 1);
  ngx_memcpy(delim_event_key->data + sizeof(SPLIT_DELIM) - 1, "e@", (sizeof("e@") - 1));
  
  return delim_event_key;
}

ngx_str_t* get_delim_topic_key(ngx_cycle_t *cycle){
  ngx_str_t *delim_topic_key;
  
  delim_topic_key = ngx_palloc(cycle->pool, sizeof(ngx_str_t));
  if(NULL == delim_topic_key){
    ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, "insufficient memory for delim topic key");
    return NULL;
  }
  
  delim_topic_key->len = (sizeof(SPLIT_DELIM) - 1) + (sizeof("t@") - 1);
  delim_topic_key->data = ngx_pcalloc(cycle->pool, delim_topic_key->len);
  ngx_memcpy(delim_topic_key->data, SPLIT_DELIM, sizeof(SPLIT_DELIM) - 1);
  ngx_memcpy(delim_topic_key->data + sizeof(SPLIT_DELIM) - 1, "t@", (sizeof("t@") - 1));
  
  return delim_topic_key;
}

void check_worker_processes(ngx_core_conf_t *ccf, ngx_cycle_t *cycle){
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
  ngx_http_event_broker_shm_t *shm = mcf->shm_ctx->shared_mem;
  ngx_http_event_broker_node_t *node;
  
  node = (ngx_http_event_broker_node_t *)ngx_str_rbtree_lookup(&shm->rbtree, s, hash);
  
  return node;
}

static u_char* get_if_contain(u_char *start, u_char *end, u_char *delim_s, size_t delim_len){
  u_char *result;
  while ((result = ngx_strlchr(start, end, *delim_s))){
    if(ngx_strncmp(result, delim_s, delim_len) == 0){
      return result;
    }
    start = ++result;
  }
  return NULL;
}

ngx_int_t ngx_http_eb_shm_init(ngx_shm_zone_t *shm_zone, void *data) {
  size_t                    len;
  ngx_http_event_broker_shm_ctx_t *oshm = data;
  ngx_http_event_broker_shm_ctx_t *nshm = shm_zone->data;
  ngx_slab_pool_t *shpool;
  
  if (oshm) {
    nshm->shm_zone_name = oshm->shm_zone_name;
    nshm->shared_mem = oshm->shared_mem;
    return NGX_OK;
  }

  shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

  if (shm_zone->shm.exists) {
    shm_zone->data = shpool->data;
    return NGX_OK;
  }
  
  nshm->shared_mem = ngx_slab_alloc(shpool, sizeof(ngx_http_event_broker_shm_t));
  ngx_rbtree_init(&nshm->shared_mem->rbtree, &nshm->shared_mem->sentinel, ngx_str_rbtree_insert_value);
  
  nshm->shared_mem->shpool = shpool;
  
  len = sizeof(" in nginx event broker session shared cache \"\"") + shm_zone->shm.name.len;
  
  nshm->shared_mem->shpool->log_ctx = ngx_slab_alloc(nshm->shared_mem->shpool, len);
  if (nshm->shared_mem->shpool->log_ctx == NULL) {
    return NGX_ERROR;
  }
  
  ngx_sprintf(nshm->shared_mem->shpool->log_ctx, " nginx event broker session shared cache \"%V\"%Z",
              &shm_zone->shm.name);
  
  nshm->shared_mem->shpool->log_nomem = 0;
  
  return NGX_OK;
}

/* Commands */

static char* ngx_http_eb_set_shm_size_cmd(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_event_broker_main_conf_t *mcf = conf;
  ngx_str_t                         *values;
  ngx_shm_zone_t                    *shm_zone;
  ngx_int_t                         pg_size;
  
  values = cf->args->elts;
  pg_size = ngx_parse_size(&values[1]);
  
  if (pg_size == NGX_ERROR) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf,  0, "eb : %s", "Invalid cache size, please specify like 1m, 100M or etc.");
    return NGX_CONF_ERROR;
  }
  
  shm_zone = ngx_shared_memory_add(cf, &mcf->shm_ctx->shm_zone_name, pg_size, &ngx_http_event_broker_module);
  if (shm_zone == NULL) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf,  0, "eb : %s", "Unable to allocate apps defined size");
    return NGX_CONF_ERROR;
  }
  shm_zone->init = ngx_http_eb_shm_init;
  shm_zone->data = mcf->shm_ctx;
  
  return NGX_CONF_OK;
}

static char* ngx_http_eb_publish_cmd(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_event_broker_loc_conf_t  *lflcf = conf;
  ngx_http_compile_complex_value_t  ccv;
  ngx_str_t                         *value;
  
  if (lflcf->target_topic$.value.len != 0) {
    return "one topic for one location";
  }
  
  value = cf->args->elts;
  if (value[1].len == 0) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "Event Broker, %s", "no topic name given ");
    return NGX_CONF_ERROR;
  }
  
  ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));
  
  ccv.cf = cf;
  ccv.value = &value[1];
  ccv.complex_value = &lflcf->target_topic$;
  
  if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
    return NGX_CONF_ERROR;
  }
  
  return NGX_CONF_OK;
}