#include "ruby-duckdb.h"

static VALUE cDuckDBLogicalType;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE logical_type__width(VALUE self);
static VALUE logical_type__scale(VALUE self);

static const rb_data_type_t logical_type_data_type = {
    "DuckDB/LogicalType",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBLogicalType *p = (rubyDuckDBLogicalType *)ctx;

    if (p->logical_type) {
        duckdb_destroy_logical_type(&(p->logical_type));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBLogicalType *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBLogicalType));
    return TypedData_Wrap_Struct(klass, &logical_type_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBLogicalType);
}

VALUE rbduckdb_create_logical_type(duckdb_logical_type logical_type) {
    VALUE obj;
    rubyDuckDBLogicalType *ctx;

    obj = allocate(cDuckDBLogicalType);
    TypedData_Get_Struct(obj, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    ctx->logical_type = logical_type;

    return obj;
}

VALUE logical_type__width(VALUE self) {
    rubyDuckDBLogicalType *ctx;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    uint8_t width = duckdb_decimal_width(ctx->logical_type);

    return UINT2NUM(width);
}

VALUE logical_type__scale(VALUE self) {
    rubyDuckDBLogicalType *ctx;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    uint8_t width = duckdb_decimal_scale(ctx->logical_type);

    return UINT2NUM(width);
}

void rbduckdb_init_duckdb_logical_type(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBLogicalType = rb_define_class_under(mDuckDB, "LogicalType", rb_cObject);
    rb_define_alloc_func(cDuckDBLogicalType, allocate);

    rb_define_method(cDuckDBLogicalType, "_width", logical_type__width, 0);
    rb_define_method(cDuckDBLogicalType, "_scale", logical_type__scale, 0);
}
