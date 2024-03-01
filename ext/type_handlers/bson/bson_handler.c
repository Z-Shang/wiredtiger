/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <string.h>
#include <limits.h>
#include <errno.h>
#include <stdlib.h>

#include <wiredtiger_ext.h>

#include <stdio.h>

typedef struct {
    WT_EXT_TYPE ext_type;     /* Must come first */
    WT_EXTENSION_API *wt_api; /* Extension API */
} BSON_HANDLER;

// static int
// bson_error(const BSON_HANDLER *bson_handle, WT_SESSION *session, int err, const char *msg)
// {
//     WT_EXTENSION_API *wt_api;

//     wt_api = bson_handle->wt_api;
//     (void)wt_api->err_printf(
//       wt_api, session, "bson handler: %s: %s", msg, wt_api->strerror(wt_api, NULL, err));
//     return (err);
// }

static size_t typeSizes[32] = {0, 8, 0, 0, 0, 0, 0, 12, 1, 8, 0, 0, 0, 0, 0, 0, 4, 8, 8, 16};

static size_t
bson_field_size(const unsigned char *bytes)
{
    unsigned char type;
    const unsigned char *data;
    size_t field_name_size, field_data_size;

    type = bytes[0];
    data = bytes + 1;

    field_name_size = 1 + strlen((const char *)data);
    data = data + field_name_size;

    printf("type: %u, field_name_size: %zu\n", type, field_name_size);
    if (typeSizes[type] > 0) {
        printf("fixed size: %zu\n", typeSizes[type]);
        return typeSizes[type] + field_name_size;
    }

    field_data_size = 0;

    switch (type) {
    case 3:
    case 4: {
        field_data_size = data[0];
        field_data_size += data[1] << 8;
        field_data_size += data[2] << 16;
        field_data_size += data[3] << 24;
        return field_data_size + field_data_size;
    }
    case 5: {
        field_data_size = 5;
        field_data_size += data[0];
        field_data_size += data[1] << 8;
        field_data_size += data[2] << 16;
        field_data_size += data[3] << 24;
        return field_data_size + field_name_size;
    }
    case 2:
    case 13: {
        field_data_size = 4;
        field_data_size += data[0];
        field_data_size += data[1] << 8;
        field_data_size += data[2] << 16;
        field_data_size += data[3] << 24;
        return field_data_size + field_name_size;
    }
    case 11: {
        field_data_size = strlen((const char *)data) + 1;
        field_data_size += strlen((const char *)data + field_data_size) + 1;
        return field_data_size + field_name_size;
    }
    }
    return 0;
}

/*
 * bson_project
 *     WiredTiger BSON Projection
 *     For the simplicity as a POC, let's just consider about top level single field projection
 */
static int
bson_project(WT_EXT_TYPE *type_handle, WT_SESSION *session, const char *proj, const WT_ITEM *value,
  WT_ITEM *result)
{
    const BSON_HANDLER *bson_handle;
    WT_EXTENSION_API *wt_api;
    size_t len, offset, field_size;
    // int i, ret, val;
    // char *copy, *p, *pend, *valstr;
    unsigned char *data;
    unsigned char typetag;

    bson_handle = (const BSON_HANDLER *)type_handle;
    wt_api = bson_handle->wt_api;

    (void)wt_api->msg_printf(wt_api, session, "bson project: %s", proj);

    len = 0;
    data = (unsigned char *)value->data;
    len += data[0];
    len += data[1] << 8;
    len += data[2] << 16;
    len += data[3] << 24;
    for (offset = 4; offset < len;) {
        typetag = data[offset];
        if (typetag == 0)
            return (1);
        field_size = bson_field_size(data + offset);
        if (strcmp(proj, (const char *)data + offset + 1) == 0) {
            result->data = data + offset;
            result->size = 1 + field_size;
            (void)wt_api->msg_printf(wt_api, session, "found at bson offset: %d, size: %d", offset, result->size);
            return (0);
        }
        offset += 1 + field_size;
    }

    (void)wt_api->msg_printf(wt_api, session, "field not found: %s", proj);
    return (1);
}

static int
bson_terminate(WT_EXT_TYPE *type_handle, WT_SESSION *session)
{
    (void)session; /* Unused parameters */

    /* Free the allocated memory. */
    free(type_handle);
    return (0);
}

int bson_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config);
/*
 * wiredtiger_extension_init --
 *     WiredTiger BSON extension.
 */
int
bson_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    BSON_HANDLER *bson_handle;
    int ret;

    (void)config; /* Unused parameters */

    printf("INIT BSON\n");
    if ((bson_handle = calloc(1, sizeof(BSON_HANDLER))) == NULL)
        return (errno);

    bson_handle->ext_type.project = bson_project;
    bson_handle->ext_type.terminate = bson_terminate;
    bson_handle->wt_api = connection->get_extension_api(connection);

    if ((ret = connection->add_ext_type(connection, "bson", (WT_EXT_TYPE *)bson_handle)) == 0)
        return (0);

    free(bson_handle);
    return (ret);
}

#ifndef HAVE_BUILTIN_EXTENSION_BSON
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    return (bson_extension_init(connection, config));
}
#endif
