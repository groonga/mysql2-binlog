#include <ruby.h>
#include <ruby/encoding.h>
#include <ruby/thread.h>

/* libmariadb */
#include <mysql.h>
#include <mariadb_com.h>
#include <mariadb_rpl.h>

/* mysql2 */
#include <client.h>

void Init_mysql2_replication(void);

static VALUE rb_cMysql2Error;

static VALUE rb_cMysql2ReplicationEvent;
static VALUE rb_cMysql2ReplicationRotateEvent;
static VALUE rb_cMysql2ReplicationFormatDescriptionEvent;
static VALUE rb_cMysql2ReplicationTableMapEvent;
static VALUE rb_cMysql2ReplicationWriteRowsEvent;
static VALUE rb_cMysql2ReplicationUpdateRowsEvent;
static VALUE rb_cMysql2ReplicationDeleteRowsEvent;

static ID
rbm2_column_type_to_id(enum enum_field_types column_type)
{
  switch (column_type) {
  case MYSQL_TYPE_DECIMAL:
    return rb_intern("decimal");
  case MYSQL_TYPE_TINY:
    return rb_intern("tiny");
  case MYSQL_TYPE_SHORT:
    return rb_intern("short");
  case MYSQL_TYPE_LONG:
    return rb_intern("long");
  case MYSQL_TYPE_FLOAT:
    return rb_intern("float");
  case MYSQL_TYPE_DOUBLE:
    return rb_intern("double");
  case MYSQL_TYPE_NULL:
    return rb_intern("null");
  case MYSQL_TYPE_TIMESTAMP:
    return rb_intern("timestamp");
  case MYSQL_TYPE_LONGLONG:
    return rb_intern("longlong");
  case MYSQL_TYPE_INT24:
    return rb_intern("int24");
  case MYSQL_TYPE_DATE:
    return rb_intern("date");
  case MYSQL_TYPE_TIME:
    return rb_intern("time");
  case MYSQL_TYPE_DATETIME:
    return rb_intern("datetime");
  case MYSQL_TYPE_YEAR:
    return rb_intern("year");
  case MYSQL_TYPE_NEWDATE:
    return rb_intern("newdate");
  case MYSQL_TYPE_VARCHAR:
    return rb_intern("varchar");
  case MYSQL_TYPE_BIT:
    return rb_intern("bit");
  case MYSQL_TYPE_TIMESTAMP2:
    return rb_intern("timestamp2");
  case MYSQL_TYPE_DATETIME2:
    return rb_intern("datetime2");
  case MYSQL_TYPE_TIME2:
    return rb_intern("time2");
  case MYSQL_TYPE_JSON:
    return rb_intern("json");
  case MYSQL_TYPE_NEWDECIMAL:
    return rb_intern("newdecimal");
  case MYSQL_TYPE_ENUM:
    return rb_intern("enum");
  case MYSQL_TYPE_SET:
    return rb_intern("set");
  case MYSQL_TYPE_TINY_BLOB:
    return rb_intern("tiny_blob");
  case MYSQL_TYPE_MEDIUM_BLOB:
    return rb_intern("medium_blob");
  case MYSQL_TYPE_LONG_BLOB:
    return rb_intern("long_blob");
  case MYSQL_TYPE_BLOB:
    return rb_intern("blob");
  case MYSQL_TYPE_VAR_STRING:
    return rb_intern("var_string");
  case MYSQL_TYPE_STRING:
    return rb_intern("string");
  case MYSQL_TYPE_GEOMETRY:
    return rb_intern("geometry");
  default:
    return rb_intern("unknown");
  }
}

static VALUE
rbm2_column_type_to_symbol(enum enum_field_types column_type)
{
  return rb_id2sym(rbm2_column_type_to_id(column_type));
}

static void
rbm2_metadata_parse(enum enum_field_types *column_type,
                    const uint8_t **metadata,
                    VALUE rb_column)
{
  switch (*column_type) {
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_LONG:
    break;
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("size")),
                 UINT2NUM((*metadata)[0]));
    (*metadata) += 1;
    break;
  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_YEAR:
  case MYSQL_TYPE_NEWDATE:
    break;
  case MYSQL_TYPE_VARCHAR:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("max_length")),
                 UINT2NUM(((const uint16_t *)(*metadata))[0]));
    (*metadata) += 2;
    break;
  case MYSQL_TYPE_BIT:
    {
      uint8_t bits = (*metadata)[0];
      uint8_t bytes = (*metadata)[1];
      rb_hash_aset(rb_column,
                   rb_id2str(rb_intern("bits")),
                   UINT2NUM((bytes * 8) + bits));
      (*metadata) += 2;
    }
    break;
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIME2:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("decimals")),
                 UINT2NUM((*metadata)[0]));
    (*metadata) += 1;
    break;
  case MYSQL_TYPE_JSON:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("length_size")),
                 UINT2NUM((*metadata)[0]));
    (*metadata) += 1;
    break;
  case MYSQL_TYPE_NEWDECIMAL:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("precision")),
                 UINT2NUM((*metadata)[0]));
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("decimals")),
                 UINT2NUM((*metadata)[1]));
    (*metadata) += 2;
    break;
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_SET:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("size")),
                 UINT2NUM((*metadata)[1]));
    (*metadata) += 2;
    break;
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
    break;
  case MYSQL_TYPE_BLOB:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("length_size")),
                 UINT2NUM((*metadata)[0]));
    (*metadata) += 1;
    break;
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_STRING:
    {
      *column_type = (*metadata)[0];
      switch (*column_type) {
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
        rb_hash_aset(rb_column,
                     rb_id2str(rb_intern("size")),
                     UINT2NUM((*metadata)[1]));
        break;
      default:
        rb_hash_aset(rb_column,
                     rb_id2str(rb_intern("max_length")),
                     UINT2NUM(((((*metadata)[0] >> 4) & 0x300) ^ 0x300) +
                              (*metadata)[1]));
        break;
      }
      (*metadata) += 2;
    }
    break;
  case MYSQL_TYPE_GEOMETRY:
    rb_hash_aset(rb_column,
                 rb_id2str(rb_intern("length_size")),
                 UINT2NUM((*metadata)[0]));
    (*metadata) += 1;
    break;
  default:
    break;
  }
}

static VALUE
rbm2_replication_event_new(MARIADB_RPL_EVENT *event)
{
  VALUE klass;
  VALUE rb_event;
  switch (event->event_type) {
  case ROTATE_EVENT:
    klass = rb_cMysql2ReplicationRotateEvent;
    rb_event = rb_class_new_instance(0, NULL, klass);
    {
      struct st_mariadb_rpl_rotate_event *e = &(event->event.rotate);
      rb_iv_set(rb_event, "@position", ULL2NUM(e->position));
      rb_iv_set(rb_event, "@file_name", rb_str_new_cstr(e->filename.str));
    }
    break;
  case FORMAT_DESCRIPTION_EVENT:
    klass = rb_cMysql2ReplicationFormatDescriptionEvent;
    rb_event = rb_class_new_instance(0, NULL, klass);
    {
      struct st_mariadb_rpl_format_description_event *e =
        &(event->event.format_description);
      rb_iv_set(rb_event, "@format", USHORT2NUM(e->format));
      rb_iv_set(rb_event, "@server_version", rb_str_new_cstr(e->server_version));
      rb_iv_set(rb_event, "@timestamp", UINT2NUM(e->timestamp));
      rb_iv_set(rb_event, "@header_length", UINT2NUM(e->header_len));
    }
    break;
  case TABLE_MAP_EVENT:
    klass = rb_cMysql2ReplicationTableMapEvent;
    rb_event = rb_class_new_instance(0, NULL, klass);
    {
      struct st_mariadb_rpl_table_map_event *e = &(event->event.table_map);
      rb_iv_set(rb_event, "@table_id", ULONG2NUM(e->table_id));
      rb_iv_set(rb_event, "@database", rb_str_new(e->database.str,
                                                  e->database.length));
      rb_iv_set(rb_event, "@table", rb_str_new(e->table.str,
                                               e->table.length));
      {
        VALUE rb_columns = rb_ary_new_capa(e->column_count);
        const uint8_t *column_types = (const uint8_t *)(e->column_types.str);
        const uint8_t *metadata = (const uint8_t *)(e->metadata.str);
        uint32_t i;
        for (i = 0; i < e->column_count; i++) {
          uint8_t column_type = column_types[i];
          enum enum_field_types real_column_type = column_type;
          VALUE rb_column = rb_hash_new();
          rbm2_metadata_parse(&real_column_type,
                              &metadata,
                              rb_column);
          rb_hash_aset(rb_column,
                       rb_id2str(rb_intern("type")),
                       rbm2_column_type_to_symbol(real_column_type));
          rb_ary_push(rb_columns, rb_column);
        }
        rb_iv_set(rb_event, "@columns", rb_columns);
      }
    }
    break;
  case WRITE_ROWS_EVENT_V1:
  case WRITE_ROWS_EVENT:
  case UPDATE_ROWS_EVENT_V1:
  case UPDATE_ROWS_EVENT:
  case DELETE_ROWS_EVENT_V1:
  case DELETE_ROWS_EVENT:
    switch (event->event_type) {
    case WRITE_ROWS_EVENT_V1:
    case WRITE_ROWS_EVENT:
      klass = rb_cMysql2ReplicationWriteRowsEvent;
      break;
    case UPDATE_ROWS_EVENT_V1:
    case UPDATE_ROWS_EVENT:
      klass = rb_cMysql2ReplicationUpdateRowsEvent;
      break;
    default:
      klass = rb_cMysql2ReplicationDeleteRowsEvent;
      break;
    }
    rb_event = rb_class_new_instance(0, NULL, klass);
    {
      struct st_mariadb_rpl_rows_event *e = &(event->event.rows);
      rb_iv_set(rb_event, "@table_id", ULONG2NUM(e->table_id));
      rb_iv_set(rb_event, "@flags", USHORT2NUM(e->flags));
      {
        VALUE rb_columns = rb_ary_new_capa(e->column_count);
        uint32_t i;
        for (i = 0; i < e->column_count; i++) {
          rb_ary_push(rb_columns,
                      ((e->column_bitmap[i % 8] & (i / 8)) != 0) ?
                      RUBY_Qtrue :
                      RUBY_Qfalse);
        }
        rb_iv_set(rb_event, "@columns", rb_columns);
      }
      if (klass == rb_cMysql2ReplicationUpdateRowsEvent) {
        VALUE rb_columns = rb_ary_new_capa(e->column_count);
        uint32_t i;
        for (i = 0; i < e->column_count; i++) {
          rb_ary_push(rb_columns,
                      ((e->column_update_bitmap[i % 8] & (i / 8)) != 0) ?
                      RUBY_Qtrue :
                      RUBY_Qfalse);
        }
        rb_iv_set(rb_event, "@updated_columns", rb_columns);
      }
      rb_iv_set(rb_event, "@row_data", rb_enc_str_new(e->row_data,
                                                      e->row_data_size,
                                                      rb_ascii8bit_encoding()));
    }
    break;
  default:
    klass = rb_cMysql2ReplicationEvent;
    rb_event = rb_class_new_instance(0, NULL, klass);
    break;
  }
  rb_iv_set(rb_event, "@type", UINT2NUM(event->event_type));
  rb_iv_set(rb_event, "@timestamp", UINT2NUM(event->timestamp));
  rb_iv_set(rb_event, "@server_id", UINT2NUM(event->server_id));
  rb_iv_set(rb_event, "@length", UINT2NUM(event->event_length));
  rb_iv_set(rb_event, "@next_position", UINT2NUM(event->next_event_pos));
  rb_iv_set(rb_event, "@flags", USHORT2NUM(event->flags));
  return rb_event;
}

typedef struct
{
  MARIADB_RPL *rpl;
  MARIADB_RPL_EVENT *rpl_event;
  VALUE rb_client;
} rbm2_replication_client_wrapper;

static void
rbm2_replication_client_mark(void *data)
{
  rbm2_replication_client_wrapper *wrapper = data;
  rb_gc_mark(wrapper->rb_client);
}

static void
rbm2_replication_client_free(void *data)
{
  rbm2_replication_client_wrapper *wrapper = data;
  if (wrapper->rpl_event) {
    mariadb_free_rpl_event(wrapper->rpl_event);
  }
  if (wrapper->rpl) {
    mariadb_rpl_close(wrapper->rpl);
  }
  ruby_xfree(wrapper);
}

static const rb_data_type_t rbm2_replication_client_type = {
  "Mysql2Replication::Client",
  {
    rbm2_replication_client_mark,
    rbm2_replication_client_free,
  },
  NULL,
  NULL,
  RUBY_TYPED_FREE_IMMEDIATELY,
};

static VALUE
rbm2_replication_client_alloc(VALUE klass)
{
  rbm2_replication_client_wrapper *wrapper;
  VALUE rb_wrapper = TypedData_Make_Struct(klass,
                                           rbm2_replication_client_wrapper,
                                           &rbm2_replication_client_type,
                                           wrapper);
  wrapper->rpl = NULL;
  wrapper->rpl_event = NULL;
  wrapper->rb_client = RUBY_Qnil;
  return rb_wrapper;
}

static inline rbm2_replication_client_wrapper *
rbm2_replication_client_get_wrapper(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper;
  TypedData_Get_Struct(self,
                       rbm2_replication_client_wrapper,
                       &rbm2_replication_client_type,
                       wrapper);
  return wrapper;
}

static inline mysql_client_wrapper *
rbm2_replication_client_wrapper_get_client_wrapper(
  rbm2_replication_client_wrapper *replication_client_wrapper)
{
  GET_CLIENT(replication_client_wrapper->rb_client);
  return wrapper;
}

static inline MYSQL *
rbm2_replication_client_wrapper_get_client(
  rbm2_replication_client_wrapper *wrapper)
{
  return rbm2_replication_client_wrapper_get_client_wrapper(wrapper)->client;
}

static void
rbm2_replication_client_raise(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  mysql_client_wrapper *client_wrapper =
    rbm2_replication_client_wrapper_get_client_wrapper(wrapper);
  VALUE rb_error_message =
    rb_enc_str_new_cstr(mysql_error(client_wrapper->client),
                        rb_utf8_encoding());
  VALUE rb_sql_state =
    rb_enc_str_new_cstr(mysql_sqlstate(client_wrapper->client),
                        rb_usascii_encoding());
  ID new_with_args;
  CONST_ID(new_with_args, "new_with_args");
  VALUE rb_error = rb_funcall(rb_cMysql2Error,
                              new_with_args,
                              4,
                              rb_error_message,
                              LONG2NUM(client_wrapper->server_version),
                              UINT2NUM(mysql_errno(client_wrapper->client)),
                              rb_sql_state);
  rb_exc_raise(rb_error);
}

static VALUE
rbm2_replication_client_initialize(int argc, VALUE *argv, VALUE self)
{
  VALUE rb_client;
  VALUE rb_options;
  VALUE rb_checksum = RUBY_Qnil;

  rb_scan_args(argc, argv, "10:", &rb_client, &rb_options);
  if (!NIL_P(rb_options)) {
    static ID keyword_ids[1];
    VALUE keyword_args[1];
    if (keyword_ids[0] == 0) {
      CONST_ID(keyword_ids[0], "checksum");
    }
    rb_get_kwargs(rb_options, keyword_ids, 0, 1, keyword_args);
    if (keyword_args[0] != RUBY_Qundef) {
      rb_checksum = keyword_args[0];
    }
  }

  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  wrapper->rb_client = rb_client;
  wrapper->rpl =
    mariadb_rpl_init(rbm2_replication_client_wrapper_get_client(wrapper));
  if (!wrapper->rpl) {
    rbm2_replication_client_raise(self);
  }

  {
    VALUE rb_query;
    if (RB_NIL_P(rb_checksum)) {
      rb_query = rb_str_new_cstr("SET @master_binlog_checksum = "
                                 "@@global.binlog_checksum");
    } else {
      rb_query = rb_sprintf("SET @master_binlog_checksum = '%"PRIsVALUE"'",
                            rb_checksum);
    }
    ID id_query;
    CONST_ID(id_query, "query");
    rb_funcall(rb_client, id_query, 1, rb_query);
  }

  return RUBY_Qnil;
}

static VALUE
rbm2_replication_client_get_file_name(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  const char *file_name;
  size_t file_name_length;
  int result = mariadb_rpl_get_optionsv(wrapper->rpl,
                                        MARIADB_RPL_FILENAME,
                                        &file_name,
                                        &file_name_length);
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return rb_str_new(file_name, file_name_length);
}

static VALUE
rbm2_replication_client_set_file_name(VALUE self, VALUE file_name)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  int result;
  if (RB_NIL_P(file_name)) {
    result = mariadb_rpl_optionsv(wrapper->rpl,
                                  MARIADB_RPL_FILENAME,
                                  NULL,
                                  0);
  } else {
    result = mariadb_rpl_optionsv(wrapper->rpl,
                                  MARIADB_RPL_FILENAME,
                                  RSTRING_PTR(file_name),
                                  RSTRING_LEN(file_name));
  }
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return file_name;
}

static VALUE
rbm2_replication_client_get_start_position(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  unsigned long start_position;
  int result = mariadb_rpl_get_optionsv(wrapper->rpl,
                                        MARIADB_RPL_START,
                                        &start_position);
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return ULONG2NUM(start_position);
}

static VALUE
rbm2_replication_client_set_start_position(VALUE self, VALUE start_position)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  int result = mariadb_rpl_optionsv(wrapper->rpl,
                                    MARIADB_RPL_START,
                                    NUM2ULONG(start_position));
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return start_position;
}

static VALUE
rbm2_replication_client_get_server_id(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  unsigned int server_id;
  int result = mariadb_rpl_get_optionsv(wrapper->rpl,
                                        MARIADB_RPL_SERVER_ID,
                                        &server_id);
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return UINT2NUM(server_id);
}

static VALUE
rbm2_replication_client_set_server_id(VALUE self, VALUE server_id)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  int result = mariadb_rpl_optionsv(wrapper->rpl,
                                    MARIADB_RPL_SERVER_ID,
                                    NUM2UINT(server_id));
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return server_id;
}

static VALUE
rbm2_replication_client_get_flags(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  unsigned int flags;
  int result = mariadb_rpl_get_optionsv(wrapper->rpl,
                                        MARIADB_RPL_FLAGS,
                                        &flags);
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return UINT2NUM(flags);
}

static VALUE
rbm2_replication_client_set_flags(VALUE self, VALUE flags)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  int result = mariadb_rpl_optionsv(wrapper->rpl,
                                    MARIADB_RPL_FLAGS,
                                    NUM2UINT(flags));
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  return flags;
}

static void *
rbm2_replication_client_close_without_gvl(void *data)
{
  rbm2_replication_client_wrapper *wrapper = data;
  if (wrapper->rpl_event) {
    mariadb_free_rpl_event(wrapper->rpl_event);
    wrapper->rpl_event = NULL;
  }
  if (wrapper->rpl) {
    mariadb_rpl_close(wrapper->rpl);
    wrapper->rpl = NULL;
  }
  return NULL;
}

static VALUE
rbm2_replication_client_close(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  rb_thread_call_without_gvl(rbm2_replication_client_close_without_gvl,
                             wrapper,
                             RUBY_UBF_IO,
                             0);
  return Qnil;
}

static void *
rbm2_replication_client_open_without_gvl(void *data)
{
  rbm2_replication_client_wrapper *wrapper = data;
  int result = mariadb_rpl_open(wrapper->rpl);
  return (void *)(intptr_t)result;
}

static VALUE
rbm2_replication_client_open(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  int result =
    (intptr_t)rb_thread_call_without_gvl(
      rbm2_replication_client_open_without_gvl,
      wrapper,
      RUBY_UBF_IO,
      0);
  if (result != 0) {
    rbm2_replication_client_raise(self);
  }
  if (rb_block_given_p()) {
    return rb_ensure(rb_yield, self,
                     rbm2_replication_client_close, self);
  } else {
    return Qnil;
  }
}

static void *
rbm2_replication_client_fetch_without_gvl(void *data)
{
  rbm2_replication_client_wrapper *wrapper = data;
  wrapper->rpl_event = mariadb_rpl_fetch(wrapper->rpl, wrapper->rpl_event);
  return wrapper->rpl_event;
}

static VALUE
rbm2_replication_client_fetch(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  MYSQL *client = rbm2_replication_client_wrapper_get_client(wrapper);
  do {
    MARIADB_RPL_EVENT *event =
      rb_thread_call_without_gvl(rbm2_replication_client_fetch_without_gvl,
                                 wrapper,
                                 RUBY_UBF_IO,
                                 0);
    if (mysql_errno(client) != 0) {
      rbm2_replication_client_raise(self);
    }
    if (!event) {
      if (wrapper->rpl->buffer_size == 0) {
        return RUBY_Qnil;
      }
      continue;
    }
    return rbm2_replication_event_new(event);
  } while (true);
}

static VALUE
rbm2_replication_client_each(VALUE self)
{
  rbm2_replication_client_wrapper *wrapper =
    rbm2_replication_client_get_wrapper(self);
  MYSQL *client = rbm2_replication_client_wrapper_get_client(wrapper);
  do {
    MARIADB_RPL_EVENT *event =
      rb_thread_call_without_gvl(rbm2_replication_client_fetch_without_gvl,
                                 wrapper,
                                 RUBY_UBF_IO,
                                 0);
    if (mysql_errno(client) != 0) {
      rbm2_replication_client_raise(self);
    }
    if (!event) {
      if (wrapper->rpl->buffer_size == 0) {
        return RUBY_Qnil;
      }
      continue;
    }
    rb_yield(rbm2_replication_event_new(event));
  } while (true);
  return RUBY_Qnil;
}

void
Init_mysql2_replication(void)
{
  VALUE rb_mMysql2 = rb_const_get(rb_cObject, rb_intern("Mysql2"));
  rb_cMysql2Error = rb_const_get(rb_mMysql2, rb_intern("Error"));

  VALUE rb_mMysql2Replication = rb_define_module("Mysql2Replication");

  rb_cMysql2ReplicationEvent =
    rb_define_class_under(rb_mMysql2Replication, "Event", rb_cObject);
  rb_define_attr(rb_cMysql2ReplicationEvent, "type", true, false);
  rb_define_attr(rb_cMysql2ReplicationEvent, "timestamp", true, false);
  rb_define_attr(rb_cMysql2ReplicationEvent, "server_id", true, false);
  rb_define_attr(rb_cMysql2ReplicationEvent, "length", true, false);
  rb_define_attr(rb_cMysql2ReplicationEvent, "next_position", true, false);
  rb_define_attr(rb_cMysql2ReplicationEvent, "flags", true, false);

  rb_cMysql2ReplicationRotateEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "RotateEvent",
                          rb_cMysql2ReplicationEvent);
  rb_define_attr(rb_cMysql2ReplicationRotateEvent, "position", true, false);
  rb_define_attr(rb_cMysql2ReplicationRotateEvent, "file_name", true, false);

  rb_cMysql2ReplicationFormatDescriptionEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "FormatDescriptionEvent",
                          rb_cMysql2ReplicationEvent);
  rb_define_attr(rb_cMysql2ReplicationFormatDescriptionEvent,
                 "format", true, false);
  rb_define_attr(rb_cMysql2ReplicationFormatDescriptionEvent,
                 "server_version", true, false);
  rb_define_attr(rb_cMysql2ReplicationFormatDescriptionEvent,
                 "timestamp", true, false);
  rb_define_attr(rb_cMysql2ReplicationFormatDescriptionEvent,
                 "header_length", true, false);

  VALUE rb_cMysql2ReplicationRowsEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "RowsEvent",
                          rb_cMysql2ReplicationEvent);
  rb_define_attr(rb_cMysql2ReplicationRowsEvent, "table_id", true, false);
  rb_define_attr(rb_cMysql2ReplicationRowsEvent, "flags", true, false);
  rb_define_attr(rb_cMysql2ReplicationRowsEvent, "columns", true, false);
  rb_define_attr(rb_cMysql2ReplicationRowsEvent, "row_data", true, false);

  rb_cMysql2ReplicationWriteRowsEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "WriteRowsEvent",
                          rb_cMysql2ReplicationRowsEvent);
  rb_cMysql2ReplicationUpdateRowsEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "UpdateRowsEvent",
                          rb_cMysql2ReplicationRowsEvent);
  rb_define_attr(rb_cMysql2ReplicationUpdateRowsEvent,
                 "updated_columns", true, false);
  rb_cMysql2ReplicationDeleteRowsEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "DeleteRowsEvent",
                          rb_cMysql2ReplicationRowsEvent);

  rb_cMysql2ReplicationTableMapEvent =
    rb_define_class_under(rb_mMysql2Replication,
                          "TableMapEvent",
                          rb_cMysql2ReplicationEvent);
  rb_define_attr(rb_cMysql2ReplicationTableMapEvent, "table_id", true, false);
  rb_define_attr(rb_cMysql2ReplicationTableMapEvent, "database", true, false);
  rb_define_attr(rb_cMysql2ReplicationTableMapEvent, "table", true, false);
  rb_define_attr(rb_cMysql2ReplicationTableMapEvent, "columns", true, false);

  VALUE rb_cMysql2ReplicationClient =
    rb_define_class_under(rb_mMysql2Replication,
                          "Client",
                          rb_cObject);
  rb_define_alloc_func(rb_cMysql2ReplicationClient,
                       rbm2_replication_client_alloc);
  rb_include_module(rb_cMysql2ReplicationClient, rb_mEnumerable);

  rb_define_method(rb_cMysql2ReplicationClient,
                   "initialize", rbm2_replication_client_initialize, -1);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "file_name", rbm2_replication_client_get_file_name, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "file_name=", rbm2_replication_client_set_file_name, 1);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "start_position",
                   rbm2_replication_client_get_start_position, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "start_position=",
                   rbm2_replication_client_set_start_position, 1);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "server_id", rbm2_replication_client_get_server_id, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "server_id=", rbm2_replication_client_set_server_id, 1);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "flags", rbm2_replication_client_get_flags, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "flags=", rbm2_replication_client_set_flags, 1);

  rb_define_method(rb_cMysql2ReplicationClient,
                   "open", rbm2_replication_client_open, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "fetch", rbm2_replication_client_fetch, 0);
  rb_define_method(rb_cMysql2ReplicationClient,
                   "close", rbm2_replication_client_close, 0);

  rb_define_method(rb_cMysql2ReplicationClient,
                   "each", rbm2_replication_client_each, 0);

  VALUE rb_cMysql2ReplicationFlags =
    rb_define_module_under(rb_mMysql2Replication, "Flags");
  rb_define_const(rb_cMysql2ReplicationFlags,
                  "BINLOG_DUMP_NON_BLOCK",
                  UINT2NUM(MARIADB_RPL_BINLOG_DUMP_NON_BLOCK));
  rb_define_const(rb_cMysql2ReplicationFlags,
                  "BINLOG_SEND_ANNOTATE_ROWS",
                  UINT2NUM(MARIADB_RPL_BINLOG_SEND_ANNOTATE_ROWS));
  rb_define_const(rb_cMysql2ReplicationFlags,
                  "IGNORE_HEARTBEAT",
                  UINT2NUM(MARIADB_RPL_IGNORE_HEARTBEAT));
}
