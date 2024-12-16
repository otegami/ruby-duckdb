#include "ruby-duckdb.h"

static VALUE cDuckDBColumn;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_column__type(VALUE oDuckDBColumn);
static VALUE duckdb_column__logical_type(VALUE oDuckDBColumn);
static VALUE duckdb_column_get_name(VALUE oDuckDBColumn);

VALUE duckdb_logical_type_name_decimal(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_name_list(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_name(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_name_struct(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_map(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_array(duckdb_logical_type logical_type);
VALUE duckdb_logical_type_name(duckdb_logical_type logical_type);

static const rb_data_type_t column_data_type = {
    "DuckDB/Column",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBColumn *p = (rubyDuckDBColumn *)ctx;

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBColumn *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBColumn));
    return TypedData_Wrap_Struct(klass, &column_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBColumn);
}

/* :nodoc: */
VALUE duckdb_column__type(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    rubyDuckDBResult *ctxresult;
    VALUE result;
    duckdb_type type;

    TypedData_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, &column_data_type, ctx);

    result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    ctxresult = get_struct_result(result);
    type = duckdb_column_type(&(ctxresult->result), ctx->col);

    return INT2FIX(type);
}

VALUE duckdb_type_name(duckdb_type type_id) {
    const char* type_name;

    switch(type_id) {
        case DUCKDB_TYPE_INVALID: type_name = "INVALID"; break;
        case DUCKDB_TYPE_BOOLEAN: type_name = "BOOLEAN"; break;
        case DUCKDB_TYPE_TINYINT: type_name = "TINYINT"; break;
        case DUCKDB_TYPE_SMALLINT: type_name = "SMALLINT"; break;
        case DUCKDB_TYPE_INTEGER: type_name = "INTEGER"; break;
        case DUCKDB_TYPE_BIGINT: type_name = "BIGINT"; break;
        case DUCKDB_TYPE_UTINYINT: type_name = "UTINYINT"; break;
        case DUCKDB_TYPE_USMALLINT: type_name = "USMALLINT"; break;
        case DUCKDB_TYPE_UINTEGER: type_name = "UINTEGER"; break;
        case DUCKDB_TYPE_UBIGINT: type_name = "UBIGINT"; break;
        case DUCKDB_TYPE_FLOAT: type_name = "FLOAT"; break;
        case DUCKDB_TYPE_DOUBLE: type_name = "DOUBLE"; break;
        case DUCKDB_TYPE_TIMESTAMP: type_name = "TIMESTAMP"; break;
        case DUCKDB_TYPE_DATE: type_name = "DATE"; break;
        case DUCKDB_TYPE_TIME: type_name = "TIME"; break;
        case DUCKDB_TYPE_INTERVAL: type_name = "INTERVAL"; break;
        case DUCKDB_TYPE_HUGEINT: type_name = "HUGEINT"; break;
        case DUCKDB_TYPE_UHUGEINT: type_name = "UHUGEINT"; break;
        case DUCKDB_TYPE_VARCHAR: type_name = "VARCHAR"; break;
        case DUCKDB_TYPE_BLOB: type_name = "BLOB"; break;
        case DUCKDB_TYPE_DECIMAL: type_name = "DECIMAL"; break;
        case DUCKDB_TYPE_TIMESTAMP_S: type_name = "TIMESTAMP_S"; break;
        case DUCKDB_TYPE_TIMESTAMP_MS: type_name = "TIMESTAMP_MS"; break;
        case DUCKDB_TYPE_TIMESTAMP_NS: type_name = "TIMESTAMP_NS"; break;
        case DUCKDB_TYPE_ENUM: type_name = "ENUM"; break;
        case DUCKDB_TYPE_LIST: type_name = "LIST"; break;
        case DUCKDB_TYPE_STRUCT: type_name = "STRUCT"; break;
        case DUCKDB_TYPE_MAP: type_name = "MAP"; break;
        case DUCKDB_TYPE_ARRAY: type_name = "ARRAY"; break;
        case DUCKDB_TYPE_UUID: type_name = "UUID"; break;
        case DUCKDB_TYPE_UNION: type_name = "UNION"; break;
        case DUCKDB_TYPE_BIT: type_name = "BIT"; break;
        case DUCKDB_TYPE_TIME_TZ: type_name = "TIME_TZ"; break;
        case DUCKDB_TYPE_TIMESTAMP_TZ: type_name = "TIMESTAMP_TZ"; break;
        case DUCKDB_TYPE_ANY: type_name = "ANY"; break;
        case DUCKDB_TYPE_VARINT: type_name = "VARINT"; break;
        case DUCKDB_TYPE_SQLNULL: type_name = "SQLNULL"; break;
        default: type_name = "UNKNOWN"; break;
    }

    return rb_str_new_cstr(type_name);
}

VALUE duckdb_logical_type_name_decimal(duckdb_logical_type logical_type) {
    int width = duckdb_decimal_width(logical_type);
    int scale = duckdb_decimal_scale(logical_type);

    return rb_sprintf("DECIMAL(%d,%d)", width, scale);
}

VALUE duckdb_logical_type_name_list(duckdb_logical_type logical_type) {
    VALUE child_logical_type_name = Qnil;
    duckdb_logical_type child_logical_type;

    child_logical_type = duckdb_list_type_child_type(logical_type);
    child_logical_type_name = duckdb_logical_type_name(child_logical_type);
    duckdb_destroy_logical_type(&child_logical_type);

    return rb_sprintf("%s[]", StringValueCStr(child_logical_type_name));;
}

VALUE duckdb_logical_type_name_struct(duckdb_logical_type logical_type) {
    idx_t child_count = duckdb_struct_type_child_count(logical_type);
    VALUE struct_type_name = rb_str_new_cstr("STRUCT(");

    for (idx_t i = 0; i < child_count; i++) {
        char *child_name = duckdb_struct_type_child_name(logical_type, i);

        duckdb_logical_type child_type = duckdb_struct_type_child_type(logical_type, i);
        VALUE child_type_name = duckdb_logical_type_name(child_type);

        if (i > 0) {
            rb_str_cat_cstr(struct_type_name, ", ");
        }

        rb_str_catf(struct_type_name, "%s %s", child_name, StringValueCStr(child_type_name));

        duckdb_destroy_logical_type(&child_type);
        duckdb_free(child_name);
    }

    return rb_str_cat_cstr(struct_type_name, ")");
}

VALUE duckdb_logical_type_name_map(duckdb_logical_type logical_type) {
    VALUE key_type_name = Qnil;
    VALUE value_type_name = Qnil;
    duckdb_logical_type key_logical_type;
    duckdb_logical_type value_logical_type;

    key_logical_type = duckdb_map_type_key_type(logical_type);
    key_type_name = duckdb_logical_type_name(key_logical_type);

    value_logical_type = duckdb_map_type_value_type(logical_type);
    value_type_name = duckdb_logical_type_name(value_logical_type);

    duckdb_destroy_logical_type(&key_logical_type);
    duckdb_destroy_logical_type(&value_logical_type);

    return rb_sprintf("MAP(%s, %s)", StringValueCStr(key_type_name), StringValueCStr(value_type_name));
}

VALUE duckdb_logical_type_name_array(duckdb_logical_type logical_type) {
    VALUE child_logical_type_name = Qnil;
    duckdb_logical_type child_logical_type;

    idx_t size = duckdb_array_type_array_size(logical_type);
    child_logical_type = duckdb_array_type_child_type(logical_type);
    child_logical_type_name = duckdb_logical_type_name(child_logical_type);

    duckdb_destroy_logical_type(&child_logical_type);

    return rb_sprintf("%s[%zu]", StringValueCStr(child_logical_type_name), size);
}

VALUE duckdb_logical_type_name(duckdb_logical_type logical_type) {
    duckdb_type type_id = duckdb_get_type_id(logical_type);
    VALUE logical_type_name = Qnil;

    switch(type_id) {
        case DUCKDB_TYPE_INVALID:
            break;
        case DUCKDB_TYPE_DECIMAL:
            logical_type_name = duckdb_logical_type_name_decimal(logical_type);
            break;
        case DUCKDB_TYPE_LIST:
            logical_type_name = duckdb_logical_type_name_list(logical_type);
            break;
        case DUCKDB_TYPE_STRUCT:
            logical_type_name = duckdb_logical_type_name_struct(logical_type);
            break;
        case DUCKDB_TYPE_MAP:
            logical_type_name = duckdb_logical_type_name_map(logical_type);
            break;
        case DUCKDB_TYPE_ARRAY:
            logical_type_name = duckdb_logical_type_name_array(logical_type);
            break;
        default:
            logical_type_name = duckdb_type_name(type_id);
    }

    return logical_type_name;
}

VALUE duckdb_column__logical_type(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    rubyDuckDBResult *ctxresult;
    VALUE result;
    duckdb_logical_type _logical_type;
    VALUE logical_type = Qnil;

    TypedData_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, &column_data_type, ctx);

    result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    ctxresult = get_struct_result(result);
    _logical_type = duckdb_column_logical_type(&(ctxresult->result), ctx->col);

    if (_logical_type) {
        logical_type = duckdb_logical_type_name(_logical_type);
    }
    duckdb_destroy_logical_type(&_logical_type);
    return logical_type;
}

/*
 *  call-seq:
 *    column.name -> string.
 *
 *  Returns the column name.
 *
 */
VALUE duckdb_column_get_name(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    VALUE result;
    rubyDuckDBResult *ctxresult;

    TypedData_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, &column_data_type, ctx);

    result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));

    ctxresult = get_struct_result(result);

    return rb_utf8_str_new_cstr(duckdb_column_name(&(ctxresult->result), ctx->col));
}

VALUE rbduckdb_create_column(VALUE oDuckDBResult, idx_t col) {
    VALUE obj;
    rubyDuckDBColumn *ctx;

    obj = allocate(cDuckDBColumn);
    TypedData_Get_Struct(obj, rubyDuckDBColumn, &column_data_type, ctx);

    rb_ivar_set(obj, rb_intern("result"), oDuckDBResult);
    ctx->col = col;

    return obj;
}

void rbduckdb_init_duckdb_column(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBColumn = rb_define_class_under(mDuckDB, "Column", rb_cObject);
    rb_define_alloc_func(cDuckDBColumn, allocate);

    rb_define_private_method(cDuckDBColumn, "_type", duckdb_column__type, 0);
    rb_define_private_method(cDuckDBColumn, "_logical_type", duckdb_column__logical_type, 0);
    rb_define_method(cDuckDBColumn, "name", duckdb_column_get_name, 0);
}
