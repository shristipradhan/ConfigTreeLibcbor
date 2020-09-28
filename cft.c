/*
 * To simplify the implementation, we have the following rules:
 *   1. A key is always a string.
 *   2. Always parse from the beginning of the data.
 *   3. Always write to the flash when modifying the data.
 *   4. Do not support array.
 *   5. Do not support indefinite data (byte string, string, array, map).
 *   6. Do not support optional flags.
 *   7. Do not support fast provisioning. Always prepare provision data offline.
 *   8. Support limited pointer level (configurable).
 */

#include "cft.h"

#include <string.h>

#define log(fmt, ...)                            \
    do {                                         \
        if (ENABLE_LOG)                          \
            fprintf(stderr, fmt, ##__VA_ARGS__); \
    } while (0)

static int _pow(int b, int ex) {
    if (ex == 0)
        return 1;
    int res = b;
    while (--ex > 0)
        res *= b;
    return res;
}

static void enc_value(void* context);


static void push(container_context_t* element, struct container_context stack[], int stackSize, int* top) {
    if (*top == -1) {
        memset(&stack[stackSize - 1], 0, sizeof(container_context_t));
        stack[stackSize - 1].type = element->type;
        stack[stackSize - 1].size = element->size;
        stack[stackSize - 1].current_index = element->current_index;
        stack[stackSize - 1].should_ignore = element->should_ignore;
        strncpy(stack[stackSize - 1].map_pointer, element->map_pointer, MAX_POINTER_LEN);
        memset(stack[stackSize - 1].key, 0, sizeof(stack[stackSize - 1].key));
        *top = stackSize - 1;
    } else if (*top == 0) {
        log("The stack is already full. \n");
    } else {
        memset(&stack[(*top) - 1], 0, sizeof(container_context_t));
        stack[(*top) - 1].type = element->type;
        stack[(*top) - 1].size = element->size;
        stack[(*top) - 1].current_index = element->current_index;
        stack[(*top) - 1].should_ignore = element->should_ignore;
        strncpy(stack[(*top) - 1].map_pointer, element->map_pointer, MAX_POINTER_LEN);
        memset(stack[(*top) - 1].key, 0, sizeof(stack[(*top) - 1].key));
        (*top)--;
    }
}

static void pop(struct container_context stack[], int stackSize, int* top) {
    if (*top == -1) {
        log("The container context stack is empty. \n");
    } else {
        log("container context popped: type=%d, size=%" PRIu64 ", current_index=%d, map_pointer=%s\n", stack[(*top)].type, stack[(*top)].size,
            stack[(*top)].current_index, stack[(*top)].map_pointer);

        // If the element popped was the last element in the stack
        // then set top to -1 to show that the stack is empty
        if ((*top) == stackSize - 1) {
            (*top) = -1;
        } else {
            (*top)++;
        }
    }
}

static container_context_t* get_top(struct container_context stack[], int stackSize, int top) {
    if (top == -1) {
        return NULL;
    }

    return &(stack[top]);
}

static void dec_map_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.map_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.map_metadata.allocated = size;
    ctx->item.metadata.map_metadata.end_ptr = 0;
    ctx->item.type = CBOR_TYPE_MAP;

    struct container_context cc = {0};
    cc.type = CBOR_TYPE_MAP;
    cc.size = size;
    cc.current_index = 0;
    cc.keep_searching = false;

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        strcpy(cc.map_pointer, "/");
        cc.should_ignore = false;
    } else {
        // Check if the specified pointer is a map.
        // If it is a map, then return syntax error, because we should not
        // specify a map. We should specify a key.
        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strcmp(ctx->pointer, tmp_pointer) == 0) {
            ctx->err = CFT_ERR_POINTER_IS_MAP;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "pointer \"%s\" should not be a map", ctx->pointer);
            return;
        }

        strcpy(cc.map_pointer, cur_cc->map_pointer);
        strcat(cc.map_pointer, cur_cc->key);
        strcat(cc.map_pointer, "/");

        // Before we dive into the map, do we really need to check the map content?
        // If the current map should already be ignored, we should ignore the coming map, too.
        // If the current map should not be ignored, we should check the current key.
        // If the current key is not what we are looking for, we should ignore the coming map.
        if (cur_cc->should_ignore) {
            cc.should_ignore = true;
        } else {
            cc.should_ignore = cur_cc->keep_searching ? false : true;
        }
    }

    push(&cc, ctx->stack, MAX_LEVEL, &(ctx->stack_top));

    log("==> map start, size = %" PRIu64 ", map_pointer = %s\n", size, cc.map_pointer);
}

static bool dec_prepare_context_for_value(void* context, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return false;
    }

    if (length > ctx->data_size) {
        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", length);
        return false;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (!cur_cc) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return false;
    }

    memset(ctx->item.data, 0, ctx->data_size);

    // If this item is a key

    if (strlen(cur_cc->key) == 0) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value cannot be a key\n");
        return false;
    }

    // If this item is a value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    if (cur_cc->keep_searching && strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If we are searching /b/f/k, but /b/f is a key for an integer, not a map, it means the pointer specified by user is wrong.
        // It is either the original CBOR data structure is wrong (/b/f should be a map, not an integer),
        // or user is wrong (should not expect /b/f to be a map).
        // We consider this a syntax error - a data type mismatch error.
        ctx->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "wrong data type: \"%s\" should be a map", tmp_pointer);
        return false;
    }

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool keep_searching = cur_cc->keep_searching;
    bool should_ignore = cur_cc->should_ignore;

    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        container_context_t* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical to search the next key in the parent map

            if (parent_cc->keep_searching && !keep_searching) {
                // If we can reach here, it means that the parent key exists in the user pointer, but the current
                // key doesn't exist in the map.
                // This is a very important information, because we know that we need to insert new key/value pair
                // into this map. Store the pointer somewhere so we know we reach this key when we re-parse the data.
                strcpy(ctx->insertion_map_pointer, cur_cc->map_pointer);
            }
        }
    }

    // The values in the current map should be ignored, because the key of the map is not what we're looking for.
    // We don't need this value, because the key of it is not what we're looking for.
    if (should_ignore || !keep_searching) {
        return false;
    }

    return true;
}

static void dec_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    if (length >= ctx->data_size) {
        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for the string (%" PRIu64 " bytes)", length);
        return;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return;
    }

    memset(ctx->item.data, 0, ctx->data_size);

    // If this string is a key

    if (strlen(cur_cc->key) == 0) {
        memset(cur_cc->key, 0, sizeof(cur_cc->key));
        memcpy(cur_cc->key, data, length);

        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strstr(ctx->pointer, tmp_pointer) == ctx->pointer) {
            cur_cc->keep_searching = true;
        } else {
            cur_cc->keep_searching = false;
        }
        return;
    }

    // If this string is a value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    if (cur_cc->keep_searching && strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If we are searching /b/f/k, but /b/f is a key for an integer, not a map, it means the pointer specified by user is wrong.
        // It is either the original CBOR data structure is wrong (/b/f should be a map, not an integer),
        // or user is wrong (should not expect /b/f to be a map).
        // We consider this a syntax error - a data type mismatch error.
        ctx->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "wrong data type: \"%s\" should be a map", tmp_pointer);
        return;
    }

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool keep_searching = cur_cc->keep_searching;
    bool should_ignore = cur_cc->should_ignore;

    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        container_context_t* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical

            if (parent_cc->keep_searching && !keep_searching) {
                // If we can reach here, it means that the parent key exists in the user pointer, but the current
                // key doesn't exist in the map.
                // This is a very important information, because we know that we need to insert new key/value pair
                // into this map. Store the pointer somewhere so we know we reach this key when we re-parse the data.
                strcpy(ctx->insertion_map_pointer, cur_cc->map_pointer);
            }
        }
    }

    // We don't need this value, because it's not what we're looking for
    if (should_ignore || !keep_searching) {
        return;
    }

    // We need this value, because it's what we're looking for
    ctx->item.type = CBOR_TYPE_STRING;
    ctx->item.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.string_metadata.length = length;
    memcpy(ctx->item.data, data, length);
    ctx->pointer_found = true;
    log("==> string (value) = %s\n", (char*)ctx->item.data);
}

static void dec_uint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint8_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_8;
    memcpy(ctx->item.data, &value, sizeof(uint8_t));
    ctx->pointer_found = true;
    log("==> uint8 = %u\n", *((uint8_t*)ctx->item.data));
}

static void dec_uint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint16_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_16;
    memcpy(ctx->item.data, &value, sizeof(uint16_t));
    ctx->pointer_found = true;
    log("==> uint16 = %u\n", *((uint16_t*)ctx->item.data));
}

static void dec_uint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint32_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_32;
    memcpy(ctx->item.data, &value, sizeof(uint32_t));
    ctx->pointer_found = true;
    log("==> uint32 = %u\n", *((uint32_t*)ctx->item.data));
}

static void dec_uint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint64_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_64;
    memcpy(ctx->item.data, &value, sizeof(uint64_t));
    ctx->pointer_found = true;
    log("==> uint64 = %" PRIu64 "\n", *((uint64_t*)ctx->item.data));
}

static void dec_negint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint8_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_8;
    memcpy(ctx->item.data, &value, sizeof(uint8_t));
    ctx->pointer_found = true;
    log("==> negint8 = -%u\n", *((uint8_t*)ctx->item.data) + 1);
}

static void dec_negint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint16_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_16;
    memcpy(ctx->item.data, &value, sizeof(uint16_t));
    ctx->pointer_found = true;
    log("==> negint16 = -%u\n", *((uint16_t*)ctx->item.data) + 1);
}

static void dec_negint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint32_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_32;
    memcpy(ctx->item.data, &value, sizeof(uint32_t));
    ctx->pointer_found = true;
    log("==> negint32 = -%u\n", *((uint32_t*)ctx->item.data) + 1);
}

static void dec_negint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint64_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_64;
    memcpy(ctx->item.data, &value, sizeof(uint64_t));
    ctx->pointer_found = true;
    log("==> negint64 = -%" PRIu64 "\n", *((uint64_t*)ctx->item.data) + 1);
}

static void dec_byte_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, length)) {
        return;
    }

    ctx->item.type = CBOR_TYPE_BYTESTRING;
    ctx->item.metadata.bytestring_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.bytestring_metadata.length = length;
    memcpy(ctx->item.data, data, length);
    ctx->pointer_found = true;

    log("==> bytes =");
    for (int i = 0; i < length; i++) {
        log(" %x", data[i]);
    }
    log("\n");
}

static void dec_float2_callback(void* context, float value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_16;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float2 = %f\n", *((float*)ctx->item.data));
}

static void dec_float4_callback(void* context, float value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_32;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float4 = %f\n", *((float*)ctx->item.data));
}

static void dec_float8_callback(void* context, double value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_64;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float8 = %f\n", *((double*)ctx->item.data));
}

static void dec_null_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = CBOR_CTRL_NULL;
    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    log("==> null\n");
}

static void dec_undefined_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = CBOR_CTRL_UNDEF;
    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    log("==> undefined\n");
}

static void dec_boolean_callback(void* context, bool value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(bool))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = value ? CBOR_CTRL_TRUE : CBOR_CTRL_FALSE;
    memcpy(ctx->item.data, &value, sizeof(bool));
    ctx->pointer_found = true;
    log("==> %s\n", *((bool*)ctx->item.data) ? "true" : "false");
}

static void dec_array_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "array is not supported");
    return;
}

static void dec_tag_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "optional tag is not supported");
    return;
}

static void dec_byte_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
    return;
}

static void dec_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
    return;
}

static void dec_indef_array_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite array is not supported");
    return;
}

static void dec_indef_map_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite map is not supported");
    return;
}

static void dec_indef_break_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite break is not supported");
    return;
}

////////////////////////////////////////////////////////////////////////////////

char* get_newkey_toinsert(char* pointer)
{
    char* p = strrchr(pointer, '/'); // /a/b/c/d --> d is the new key to be inserted
    return (p+1);
}


void enc_map_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    struct container_context cc = {0};
    cc.type = CBOR_TYPE_MAP;
    cc.size = size;
    cc.current_index = 0;
    cc.keep_searching = false;

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        strcpy(cc.map_pointer, "/");
        cc.should_ignore = false;
    } else {
        // Check if the specified pointer is a map.
        // If it is a map, then return syntax error, because we should not
        // specify a map. We should specify a key.
        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strcmp(ctx->pointer, tmp_pointer) == 0) {
            if (!ctx->erase) {
                ctx->err = CFT_ERR_POINTER_IS_MAP;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "pointer \"%s\" should not be a map", ctx->pointer);
                return;
            }

            cc.should_ignore = true;
        }

        strcpy(cc.map_pointer, cur_cc->map_pointer);
        strcat(cc.map_pointer, cur_cc->key);
        strcat(cc.map_pointer, "/");

        // Before we dive into the map, do we really need to encode and write the map content?
        // If the current map should already be ignored, we should ignore the coming map, too.
        // If the current map should not be ignored, we should check the current key.
        // If the current key is not what we are looking for, we should ignore the coming map.
        if (cur_cc->should_ignore) {
            cc.should_ignore = true;
        }
    }

    // Notice that, when we plan to remove an item from the map, the size of the map pushed to
    // the container context stack should be the original value, or the last element in the map
    // won't see the correct container context, because it will be popped out just before we see
    // the last element.
    push(&cc, ctx->stack, MAX_LEVEL, &(ctx->stack_top));

    if (cc.should_ignore) {
        return;
    }

    if (!ctx->erase && !ctx->set && strcmp(cc.map_pointer, ctx->insertion_map_pointer) == 0) {
        // If inserting new key, set insert flag to true and increase the size of the map.
        ctx->insert = true;
        cc.size++;
    }

    if (ctx->erase && strcmp(cc.map_pointer, ctx->insertion_map_pointer) == 0) {
        // Before erasing an item, we will first search the item. If the item exists,
        // we will store its parent pointer in the insertion_map_pointer variable, and
        // start to look for the item from the beginning of the cbor data again.
        // If the current map pointer is the insertion_map_pointer, it means we have
        // reached the parent map of the item that we want to delete. In this case, we
        // should decrease the parent map size by one, and then encode the parent map
        // starting byte.
        cc.size--;
    }

    log("==> map start, size = %" PRIu64 ", map_pointer = %s\n", cc.size, cc.map_pointer);

    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
    size_t written = cbor_encode_map_start(cc.size, buf, sizeof(buf));
    fwrite(buf, written, 1, ctx->fdw);
    ctx->bytes_written += written;

    if (ctx->insert)
    {
        char cftpointer[MAX_POINTER_LEN + 1] = {0}; // to copy ctx->pointer for strtok
        memcpy(cftpointer, ctx->pointer, strlen(ctx->pointer));

        char* token = strtok(cftpointer + strlen(ctx->insertion_map_pointer), "/");

        while (token != NULL)
        {
            // If a new key is being inserted into a deep nested map with it's parent keys not
            // already present in the config tree, we need to iterate and parse all the keys to create
            // the needed nested map and insert the new key with its value as key-value pair as an
            // entry to the last nested map.
            const unsigned char* in_key = token;

            cbor_item_t new_keyitem = {0};
            char in_keybuf[8] = {0};
            new_keyitem.type = CBOR_TYPE_STRING;
            new_keyitem.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
            new_keyitem.metadata.string_metadata.length = strlen(in_key);
            new_keyitem.data = in_keybuf;
            memset(new_keyitem.data, 0, sizeof(in_keybuf));
            memcpy(new_keyitem.data, in_key, strlen(in_key));

            unsigned char buf_key[MAX_INIT_BYTES_LEN] = {0};
            size_t written_key = cbor_encode_string_start(cbor_string_length(&new_keyitem), buf_key, sizeof(buf_key));
            if (written_key == 0) {
                ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for string initial bytes");
                return;
            }

            fwrite(buf_key, written_key, 1, ctx->fdw);
            fwrite(cbor_string_handle(&new_keyitem), cbor_string_length(&new_keyitem), 1, ctx->fdw);
            ctx->bytes_written += written_key;
            ctx->bytes_written += cbor_string_length(&new_keyitem);
            log("==> set string key = %s\n", cbor_string_handle(&new_keyitem));

            if (strcmp(get_newkey_toinsert(ctx->pointer), token) != 0)
            {
                unsigned char buf_n[MAX_INIT_BYTES_LEN] = {0};
                size_t written = cbor_encode_map_start(1, buf_n, sizeof(buf_n));
                fwrite(buf_n, written, 1, ctx->fdw);
                ctx->bytes_written += written;
            }

            token = strtok(NULL, "/");
        }

        enc_value(ctx);
        ctx->insert = false;
    }
}

static bool enc_prepare_context_for_value(void* context, bool* write_new_value) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return false;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (!cur_cc) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return false;
    }

    // If this item is a key

    if (strlen(cur_cc->key) == 0) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value cannot be a key\n");
        return false;
    }

    // If this item is a value

    // Check if we need to write a new value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    *write_new_value = strcmp(ctx->pointer, tmp_pointer) == 0 ? true : false;

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool should_ignore = cur_cc->should_ignore;
    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        struct container_context* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical to search the next key in the parent map
        }
    }

    return should_ignore ? false : true;
}

static void enc_value(void* context) {
    cft_context_t* ctx = context;
    if ((ctx->err != CFT_ERR_OK || ctx->erase || !ctx->insert) && (!ctx->set)) {
        return;
    }

    cbor_item_t* i = &ctx->item;
    switch (cbor_typeof(i)) {
        case CBOR_TYPE_UINT: {
            unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
            size_t written = 0;
            switch (cbor_int_get_width(i)) {
                case CBOR_INT_8: {
                    written = cbor_encode_uint8(cbor_get_uint8(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_16: {
                    written = cbor_encode_uint16(cbor_get_uint16(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_32: {
                    written = cbor_encode_uint16(cbor_get_uint32(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_64: {
                    written = cbor_encode_uint64(cbor_get_uint64(i), buf, sizeof(buf));
                    break;
                }
            }

            if (!written) {
                ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint (%dB)", _pow(2, cbor_int_get_width(i)));
                return;
            }

            fwrite(buf, written, 1, ctx->fdw);
            ctx->bytes_written += written;
            log("==> set uint (%dB) value = %" PRIu64 "\n", _pow(2, cbor_int_get_width(i)), cbor_get_int(i));
            break;
        };
        case CBOR_TYPE_NEGINT: {
            unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
            size_t written = 0;
            switch (cbor_int_get_width(i)) {
                case CBOR_INT_8: {
                    written = cbor_encode_negint8(cbor_get_uint8(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_16: {
                    written = cbor_encode_negint16(cbor_get_uint16(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_32: {
                    written = cbor_encode_negint16(cbor_get_uint32(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_64: {
                    written = cbor_encode_negint64(cbor_get_uint64(i), buf, sizeof(buf));
                    break;
                }
            }

            if (!written) {
                ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint (%dB)", _pow(2, cbor_int_get_width(i)));
                return;
            }

            fwrite(buf, written, 1, ctx->fdw);
            ctx->bytes_written += written;
            log("==> set negint (%dB) value = -%" PRIu64 "\n", _pow(2, cbor_int_get_width(i)), cbor_get_int(i) + 1);
            break;
        };
        case CBOR_TYPE_BYTESTRING: {
            if (cbor_bytestring_is_indefinite(i)) {
                ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
                return;
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = cbor_encode_bytestring_start(cbor_bytestring_length(i), buf, sizeof(buf));
                if (written == 0) {
                    ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for byte string initial bytes");
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                fwrite(cbor_bytestring_handle(i), cbor_bytestring_length(i), 1, ctx->fdw);
                ctx->bytes_written += written;
                ctx->bytes_written += cbor_bytestring_length(i);
                log("==> set byte string value (%" PRIu64 "B)\n", cbor_bytestring_length(i));
            }
            break;
        };
        case CBOR_TYPE_STRING: {
            if (cbor_string_is_indefinite(i)) {
                ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
                return;
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = cbor_encode_string_start(cbor_string_length(i), buf, sizeof(buf));
                if (written == 0) {
                    ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for string initial bytes");
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                fwrite(cbor_string_handle(i), cbor_string_length(i), 1, ctx->fdw);
                ctx->bytes_written += written;
                ctx->bytes_written += cbor_string_length(i);
                log("==> set string value = %s\n", cbor_string_handle(i));
            }
            break;
        };
        case CBOR_TYPE_FLOAT_CTRL: {
            if (cbor_float_ctrl_is_ctrl(i)) {
                if (cbor_is_bool(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_bool(cbor_get_bool(i), buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for boolean");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set boolean value = %s\n", cbor_get_bool(i) ? "true" : "false");
                } else if (cbor_is_undef(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_undef(buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for undefined");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set undefined value\n");
                } else if (cbor_is_null(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_null(buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for null");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set null value\n");
                } else {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_ctrl(cbor_ctrl_value(i), buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for simple value");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set simple value = %u\n", cbor_ctrl_value(i));
                }
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = 0;
                switch (cbor_float_get_width(i)) {
                    case CBOR_FLOAT_16:
                        written = cbor_encode_half(cbor_float_get_float2(i), buf, sizeof(buf));
                        break;
                    case CBOR_FLOAT_32:
                        written = cbor_encode_single(cbor_float_get_float4(i), buf, sizeof(buf));
                        break;
                    case CBOR_FLOAT_64:
                        written = cbor_encode_double(cbor_float_get_float8(i), buf, sizeof(buf));
                        break;
                }

                if (!written) {
                    ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float (%dB)", _pow(2, cbor_float_get_width(i)));
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                ctx->bytes_written += written;
                log("==> set float value = %lf (%dB)\n", cbor_float_get_float(i), _pow(2, cbor_float_get_width(i)));
            }
            break;
        };
    }

    ctx->pointer_found = true;
}

void enc_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map");
        return;
    }

    // If this string is a key

    if (strlen(cur_cc->key) == 0) {
        memset(cur_cc->key, 0, sizeof(cur_cc->key));
        memcpy(cur_cc->key, data, length);

        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);

        if (ctx->erase && (strcmp(ctx->pointer, tmp_pointer) == 0 || cur_cc->should_ignore)) {
            return;
        }

        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_string_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for data item initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    // If this string is a value

    // Check if we need to write a new value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool should_ignore = cur_cc->should_ignore;
    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        struct container_context* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical
        }
    }

    if (should_ignore) {
        return;
    }

    if (strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If the key is not specified by user, it means we need to write the existing value.
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_string_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for string initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    // If we reach this point, it means we need to write the new value.
    enc_value(ctx);
}

void enc_uint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint8(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint16(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint16");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint32(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint32");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint64(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint64");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint8(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint16(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint16");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint32(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint32");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint64(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint64");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_byte_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_bytestring_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for byte string initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    enc_value(ctx);
}

void enc_float2_callback(void* context, float value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_half(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float2");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_float4_callback(void* context, float value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_single(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float4");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_float8_callback(void* context, double value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_double(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_null_callback(void* context) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_null(buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for null");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_undefined_callback(void* context) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_undef(buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for undefined");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_boolean_callback(void* context, bool value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_bool(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for boolean");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_array_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "array is not supported");
    return;
}

void enc_tag_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "optional tag is not supported");
    return;
}

void enc_byte_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
    return;
}

void enc_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
    return;
}

void enc_indef_array_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite array is not supported");
    return;
}

void enc_indef_map_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite map is not supported");
    return;
}

void enc_indef_break_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite break is not supported");
    return;
}

////////////////////////////////////////////////////////////////////////////////

static cbor_item_t* get_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->insert = false;
    h->set = false;
    h->erase = false;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return NULL;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->dec_callbacks), h);
        if (h->pointer_found || h->err != CFT_ERR_OK || strlen(h->insertion_map_pointer) > strlen(ROOT_MAP_POINTER)) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    if (h->err != CFT_ERR_OK) {
        return NULL;
    }

    if (!h->pointer_found) {
        h->err = CFT_ERR_POINTER_NOT_FOUND;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" doesn't exist, but \"%s\" exists\n", h->pointer, h->insertion_map_pointer);
        return NULL;
    }

    return &h->item;
}

static cft_err_t set_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->insert = false;
    h->set = true;
    h->erase = false;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    // First we need to prepare a temp file for storing the new CBOR data
    char* tmp_name = tempnam(NULL, NULL);
    h->fdw = fopen(tmp_name, "wb");
    if (h->fdw == NULL) {
        h->err = CFT_ERR_CREATE_TEMP_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open temp file \"%s\"", tmp_name);
        free(tmp_name);
        return h->err;
    }

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        fclose(h->fdw);
        h->fdw = NULL;
        remove(tmp_name);
        free(tmp_name);
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->enc_callbacks), h);
        if (h->err != CFT_ERR_OK) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    fclose(h->fdw);
    h->fdw = NULL;

    // Delete the original file and rename the temp file
    remove(h->path);
    rename(tmp_name, h->path);
    free(tmp_name);

    if (!h->pointer_found) {
        h->err = CFT_ERR_POINTER_NOT_FOUND;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" doesn't exist\n", h->pointer);
        return h->err;
    }

    return h->err;
}

static cft_err_t insert_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->insert = false;
    h->set = false;
    h->erase = false;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->enc_callbacks), h);
        if ((h->err != CFT_ERR_OK) && (h->err != CFT_ERR_POINTER_NOT_FOUND)) {
            break;
        }

        bytes_read += decode_result.read;

        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    return h->err;
}


static cft_err_t erase_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->insert = false;
    h->set = false;
    h->erase = true;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    // First we need to prepare a temp file for storing the new CBOR data
    char* tmp_name = tempnam(NULL, NULL);
    h->fdw = fopen(tmp_name, "wb");
    if (h->fdw == NULL) {
        h->err = CFT_ERR_CREATE_TEMP_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open temp file \"%s\"", tmp_name);
        free(tmp_name);
        return h->err;
    }

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        fclose(h->fdw);
        h->fdw = NULL;
        remove(tmp_name);
        free(tmp_name);
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->enc_callbacks), h);
        if (h->err != CFT_ERR_OK) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    fclose(h->fdw);
    h->fdw = NULL;

    // Delete the original file and rename the temp file
    remove(h->path);
    rename(tmp_name, h->path);
    free(tmp_name);

    return h->err;
}

uint8_t cft_get_uint8(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_uint(i) || cbor_int_get_width(i) != CBOR_INT_8) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a uint8\n", h->pointer);
        return 0;
    }

    return cbor_get_uint8(i);
}

uint16_t cft_get_uint16(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_uint(i) || cbor_int_get_width(i) != CBOR_INT_16) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a uint16\n", h->pointer);
        return 0;
    }

    return cbor_get_uint16(i);
}

const unsigned char* cft_get_sz(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_string(i)) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a null-terminated string\n", h->pointer);
        return 0;
    }

    return cbor_string_handle(i);
}

cft_err_t cft_set_sz(cft_context_t* h, const char* pointer, const unsigned char* v, unsigned char* old, size_t old_size) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL && h->err != CFT_ERR_POINTER_NOT_FOUND) {
        return h->err;
    }

    if (i != NULL) {
        // the pointer exists, so we need to set the new value.

        log("=> Key already exists, hence setting to a new value ...");

        // If user wants to know the original value, copy the old value to the provided buffer.
        // Returning the old value is a useful feature, since user can undo the modification later.
        if (old != NULL) {
            if (old_size <= i->metadata.string_metadata.length) {
                h->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", i->metadata.string_metadata.length);
                return h->err;
            }

            memset(old, 0, old_size);
            memcpy(old, i->data, i->metadata.string_metadata.length);
        }

        size_t new_size = strlen(v);
        if (new_size >= h->data_size) {
            h->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", new_size);
            return h->err;
        }

        h->item.type = CBOR_TYPE_STRING;
        h->item.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
        h->item.metadata.string_metadata.length = new_size;
        memset(h->item.data, 0, h->data_size);
        memcpy(h->item.data, v, new_size);
        cft_err_t res = set_item(h, pointer);
        if (res != CFT_ERR_OK) {
            return h->err;
        }

        return h->err;
    }

    if (h->err == CFT_ERR_POINTER_NOT_FOUND) {
        // We should insert the key into h->insertion_map_pointer.

        log("=> Key is not present, hence new key and its value ...\n");
        // the pointer exists, so we need to set the new value.

        // First we need to prepare a temp file for storing the new CBOR data
        char* tmp_name = tempnam(NULL, NULL);
        h->fdw = fopen(tmp_name, "wb");
        if (h->fdw == NULL) {
            h->err = CFT_ERR_CREATE_TEMP_FILE_ERROR;
            snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open temp file \"%s\"", tmp_name);
            free(tmp_name);
            return h->err;
        }

        size_t new_size = strlen(v);
        if (new_size >= h->data_size) {
            h->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", new_size);
            fclose(h->fdw);
            remove(tmp_name);
            free(tmp_name);
            return h->err;
        }

        h->item.type = CBOR_TYPE_STRING;
        h->item.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
        h->item.metadata.string_metadata.length = new_size;
        memset(h->item.data, 0, h->data_size);
        memcpy(h->item.data, v, new_size);
        cft_err_t res = insert_item(h, pointer);
        if (res != CFT_ERR_OK) {
            log("=> func: %s, Error(%d) returned\n", __func__, res);
            return h->err;
        }

        // Delete the original file and rename the temp file
        fclose(h->fdw);
        remove(h->path);
        rename(tmp_name, h->path);
        free(tmp_name);
        return h->err;
    }

    return h->err;
}

cft_err_t cft_erase(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL && h->err != CFT_ERR_POINTER_IS_MAP) {
        return h->err;
    }

    char* p = strrchr(pointer, '/');
    if (p == NULL) {
        fprintf(stderr, "Fatal error: cannot find '/' in the pointer \"%s\"\n", pointer);
        return CFT_ERR_POINTER_NOT_FOUND;
    }

    size_t count = p - pointer + 1;
    memset(h->insertion_map_pointer, 0, sizeof(h->insertion_map_pointer));
    memcpy(h->insertion_map_pointer, pointer, count);

    log("------------------------------> insertion_map_pointer: \"%s\"\n", h->insertion_map_pointer);

    cft_err_t res = erase_item(h, pointer);
    if (res != CFT_ERR_OK) {
        fprintf(stderr, "Fatal error: fail to erase existing item \"%s\"\n", pointer);
        return res;
    }

    return h->err;
}

cft_err_t cft_init(cft_context_t* h, const char* path) {
    if (strlen(path) >= sizeof(h->path)) {
        h->err = CFT_ERR_INSUFFICIENT_PATH_BUFFER;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough to store path \"%s\"", path);
        return h->err;
    }

    memset(h, 0, sizeof(cft_context_t));
    strncpy(h->path, path, MAX_PATH_LEN);
    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_END);
    h->content_len = (size_t)ftell(h->fd);
    fclose(h->fd);
    h->fd = NULL;

    h->content_size = MAX_SCAN_BUF_LEN;
    h->content = malloc(h->content_size);
    if (h->content == NULL) {
        h->err = CFT_ERR_ALLOC_BUFFER_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to allocate content buffer");
        return h->err;
    }

    h->dec_callbacks.uint8 = dec_uint8_callback;
    h->dec_callbacks.uint16 = dec_uint16_callback;
    h->dec_callbacks.uint32 = dec_uint32_callback;
    h->dec_callbacks.uint64 = dec_uint64_callback;
    h->dec_callbacks.negint8 = dec_negint8_callback;
    h->dec_callbacks.negint16 = dec_negint16_callback;
    h->dec_callbacks.negint32 = dec_negint32_callback;
    h->dec_callbacks.negint64 = dec_negint64_callback;
    h->dec_callbacks.byte_string = dec_byte_string_callback;
    h->dec_callbacks.byte_string_start = dec_byte_string_start_callback;
    h->dec_callbacks.string = dec_string_callback;
    h->dec_callbacks.string_start = dec_string_start_callback;
    h->dec_callbacks.array_start = dec_array_start_callback;
    h->dec_callbacks.indef_array_start = dec_indef_array_start_callback;
    h->dec_callbacks.map_start = dec_map_start_callback;
    h->dec_callbacks.indef_map_start = dec_indef_map_start_callback;
    h->dec_callbacks.tag = dec_tag_callback;
    h->dec_callbacks.null = dec_null_callback;
    h->dec_callbacks.undefined = dec_undefined_callback;
    h->dec_callbacks.boolean = dec_boolean_callback;
    h->dec_callbacks.float2 = dec_float2_callback;
    h->dec_callbacks.float4 = dec_float4_callback;
    h->dec_callbacks.float8 = dec_float8_callback;
    h->dec_callbacks.indef_break = dec_indef_break_callback;

    h->enc_callbacks.uint8 = enc_uint8_callback;
    h->enc_callbacks.uint16 = enc_uint16_callback;
    h->enc_callbacks.uint32 = enc_uint32_callback;
    h->enc_callbacks.uint64 = enc_uint64_callback;
    h->enc_callbacks.negint8 = enc_negint8_callback;
    h->enc_callbacks.negint16 = enc_negint16_callback;
    h->enc_callbacks.negint32 = enc_negint32_callback;
    h->enc_callbacks.negint64 = enc_negint64_callback;
    h->enc_callbacks.byte_string = enc_byte_string_callback;
    h->enc_callbacks.byte_string_start = enc_byte_string_start_callback;
    h->enc_callbacks.string = enc_string_callback;
    h->enc_callbacks.string_start = enc_string_start_callback;
    h->enc_callbacks.array_start = enc_array_start_callback;
    h->enc_callbacks.indef_array_start = enc_indef_array_start_callback;
    h->enc_callbacks.map_start = enc_map_start_callback;
    h->enc_callbacks.indef_map_start = enc_indef_map_start_callback;
    h->enc_callbacks.tag = enc_tag_callback;
    h->enc_callbacks.null = enc_null_callback;
    h->enc_callbacks.undefined = enc_undefined_callback;
    h->enc_callbacks.boolean = enc_boolean_callback;
    h->enc_callbacks.float2 = enc_float2_callback;
    h->enc_callbacks.float4 = enc_float4_callback;
    h->enc_callbacks.float8 = enc_float8_callback;
    h->enc_callbacks.indef_break = enc_indef_break_callback;

    h->item.data = (uint8_t*)malloc(MAX_DATA_LEN);
    if (h->item.data == NULL) {
        h->err = CFT_ERR_ALLOC_BUFFER_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to allocate buffer");
        return h->err;
    }

    h->data_size = MAX_DATA_LEN;
    h->pointer_found = false;
    h->stack_top = -1;
    h->err = CFT_ERR_OK;
    memset(h->insertion_map_pointer, 0, sizeof(h->insertion_map_pointer));
    strncpy(h->insertion_map_pointer, ROOT_MAP_POINTER, MAX_POINTER_LEN);

    return CFT_ERR_OK;
}

void cft_uninit(cft_context_t* h) {
    free(h->item.data);
    free(h->content);
=======
/*
 * To simplify the implementation, we have the following rules:
 *   1. A key is always a string.
 *   2. Always parse from the beginning of the data.
 *   3. Always write to the flash when modifying the data.
 *   4. Do not support array.
 *   5. Do not support indefinite data (byte string, string, array, map).
 *   6. Do not support optional flags.
 *   7. Do not support fast provisioning. Always prepare provision data offline.
 *   8. Support limited pointer level (configurable).
 */

#include "cft.h"

#include <string.h>

#define log(fmt, ...)                            \
    do {                                         \
        if (ENABLE_LOG)                          \
            fprintf(stderr, fmt, ##__VA_ARGS__); \
    } while (0)

static int _pow(int b, int ex) {
    if (ex == 0)
        return 1;
    int res = b;
    while (--ex > 0)
        res *= b;
    return res;
}

static void push(container_context_t* element, struct container_context stack[], int stackSize, int* top) {
    if (*top == -1) {
        memset(&stack[stackSize - 1], 0, sizeof(container_context_t));
        stack[stackSize - 1].type = element->type;
        stack[stackSize - 1].size = element->size;
        stack[stackSize - 1].current_index = element->current_index;
        stack[stackSize - 1].should_ignore = element->should_ignore;
        strncpy(stack[stackSize - 1].map_pointer, element->map_pointer, MAX_POINTER_LEN);
        memset(stack[stackSize - 1].key, 0, sizeof(stack[stackSize - 1].key));
        *top = stackSize - 1;
    } else if (*top == 0) {
        log("The stack is already full. \n");
    } else {
        memset(&stack[(*top) - 1], 0, sizeof(container_context_t));
        stack[(*top) - 1].type = element->type;
        stack[(*top) - 1].size = element->size;
        stack[(*top) - 1].current_index = element->current_index;
        stack[(*top) - 1].should_ignore = element->should_ignore;
        strncpy(stack[(*top) - 1].map_pointer, element->map_pointer, MAX_POINTER_LEN);
        memset(stack[(*top) - 1].key, 0, sizeof(stack[(*top) - 1].key));
        (*top)--;
    }
}

static void pop(struct container_context stack[], int stackSize, int* top) {
    if (*top == -1) {
        log("The container context stack is empty. \n");
    } else {
        log("container context popped: type=%d, size=%" PRIu64 ", current_index=%d, map_pointer=%s\n", stack[(*top)].type, stack[(*top)].size,
            stack[(*top)].current_index, stack[(*top)].map_pointer);

        // If the element popped was the last element in the stack
        // then set top to -1 to show that the stack is empty
        if ((*top) == stackSize - 1) {
            (*top) = -1;
        } else {
            (*top)++;
        }
    }
}

static container_context_t* get_top(struct container_context stack[], int stackSize, int top) {
    if (top == -1) {
        return NULL;
    }

    return &(stack[top]);
}

static void dec_map_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.map_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.map_metadata.allocated = size;
    ctx->item.metadata.map_metadata.end_ptr = 0;
    ctx->item.type = CBOR_TYPE_MAP;

    struct container_context cc = {0};
    cc.type = CBOR_TYPE_MAP;
    cc.size = size;
    cc.current_index = 0;
    cc.keep_searching = false;

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        strcpy(cc.map_pointer, "/");
        cc.should_ignore = false;
    } else {
        // Check if the specified pointer is a map.
        // If it is a map, then return syntax error, because we should not
        // specify a map. We should specify a key.
        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strcmp(ctx->pointer, tmp_pointer) == 0) {
            ctx->err = CFT_ERR_POINTER_IS_MAP;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "pointer \"%s\" should not be a map", ctx->pointer);
            return;
        }

        strcpy(cc.map_pointer, cur_cc->map_pointer);
        strcat(cc.map_pointer, cur_cc->key);
        strcat(cc.map_pointer, "/");

        // Before we dive into the map, do we really need to check the map content?
        // If the current map should already be ignored, we should ignore the coming map, too.
        // If the current map should not be ignored, we should check the current key.
        // If the current key is not what we are looking for, we should ignore the coming map.
        if (cur_cc->should_ignore) {
            cc.should_ignore = true;
        } else {
            cc.should_ignore = cur_cc->keep_searching ? false : true;
        }
    }

    push(&cc, ctx->stack, MAX_LEVEL, &(ctx->stack_top));

    log("==> map start, size = %" PRIu64 ", map_pointer = %s\n", size, cc.map_pointer);
}

static bool dec_prepare_context_for_value(void* context, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return false;
    }

    if (length > ctx->data_size) {
        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", length);
        return false;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (!cur_cc) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return false;
    }

    memset(ctx->item.data, 0, ctx->data_size);

    // If this item is a key

    if (strlen(cur_cc->key) == 0) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value cannot be a key\n");
        return false;
    }

    // If this item is a value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    if (cur_cc->keep_searching && strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If we are searching /b/f/k, but /b/f is a key for an integer, not a map, it means the pointer specified by user is wrong.
        // It is either the original CBOR data structure is wrong (/b/f should be a map, not an integer),
        // or user is wrong (should not expect /b/f to be a map).
        // We consider this a syntax error - a data type mismatch error.
        ctx->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "wrong data type: \"%s\" should be a map", tmp_pointer);
        return false;
    }

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool keep_searching = cur_cc->keep_searching;
    bool should_ignore = cur_cc->should_ignore;

    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        container_context_t* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical to search the next key in the parent map

            if (parent_cc->keep_searching && !keep_searching) {
                // If we can reach here, it means that the parent key exists in the user pointer, but the current
                // key doesn't exist in the map.
                // This is a very important information, because we know that we need to insert new key/value pair
                // into this map. Store the pointer somewhere so we know we reach this key when we re-parse the data.
                strcpy(ctx->insertion_map_pointer, cur_cc->map_pointer);
            }
        }
    }

    // The values in the current map should be ignored, because the key of the map is not what we're looking for.
    // We don't need this value, because the key of it is not what we're looking for.
    if (should_ignore || !keep_searching) {
        return false;
    }

    return true;
}

static void dec_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    if (length >= ctx->data_size) {
        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for the string (%" PRIu64 " bytes)", length);
        return;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return;
    }

    memset(ctx->item.data, 0, ctx->data_size);

    // If this string is a key

    if (strlen(cur_cc->key) == 0) {
        memset(cur_cc->key, 0, sizeof(cur_cc->key));
        memcpy(cur_cc->key, data, length);

        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strstr(ctx->pointer, tmp_pointer) == ctx->pointer) {
            cur_cc->keep_searching = true;
        } else {
            cur_cc->keep_searching = false;
        }
        return;
    }

    // If this string is a value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    if (cur_cc->keep_searching && strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If we are searching /b/f/k, but /b/f is a key for an integer, not a map, it means the pointer specified by user is wrong.
        // It is either the original CBOR data structure is wrong (/b/f should be a map, not an integer),
        // or user is wrong (should not expect /b/f to be a map).
        // We consider this a syntax error - a data type mismatch error.
        ctx->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "wrong data type: \"%s\" should be a map", tmp_pointer);
        return;
    }

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool keep_searching = cur_cc->keep_searching;
    bool should_ignore = cur_cc->should_ignore;

    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        container_context_t* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical

            if (parent_cc->keep_searching && !keep_searching) {
                // If we can reach here, it means that the parent key exists in the user pointer, but the current
                // key doesn't exist in the map.
                // This is a very important information, because we know that we need to insert new key/value pair
                // into this map. Store the pointer somewhere so we know we reach this key when we re-parse the data.
                strcpy(ctx->insertion_map_pointer, cur_cc->map_pointer);
            }
        }
    }

    // We don't need this value, because it's not what we're looking for
    if (should_ignore || !keep_searching) {
        return;
    }

    // We need this value, because it's what we're looking for
    ctx->item.type = CBOR_TYPE_STRING;
    ctx->item.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.string_metadata.length = length;
    memcpy(ctx->item.data, data, length);
    ctx->pointer_found = true;
    log("==> string (value) = %s\n", (char*)ctx->item.data);
}

static void dec_uint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint8_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_8;
    memcpy(ctx->item.data, &value, sizeof(uint8_t));
    ctx->pointer_found = true;
    log("==> uint8 = %u\n", *((uint8_t*)ctx->item.data));
}

static void dec_uint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint16_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_16;
    memcpy(ctx->item.data, &value, sizeof(uint16_t));
    ctx->pointer_found = true;
    log("==> uint16 = %u\n", *((uint16_t*)ctx->item.data));
}

static void dec_uint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint32_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_32;
    memcpy(ctx->item.data, &value, sizeof(uint32_t));
    ctx->pointer_found = true;
    log("==> uint32 = %u\n", *((uint32_t*)ctx->item.data));
}

static void dec_uint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint64_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_UINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_64;
    memcpy(ctx->item.data, &value, sizeof(uint64_t));
    ctx->pointer_found = true;
    log("==> uint64 = %" PRIu64 "\n", *((uint64_t*)ctx->item.data));
}

static void dec_negint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint8_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_8;
    memcpy(ctx->item.data, &value, sizeof(uint8_t));
    ctx->pointer_found = true;
    log("==> negint8 = -%u\n", *((uint8_t*)ctx->item.data) + 1);
}

static void dec_negint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint16_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_16;
    memcpy(ctx->item.data, &value, sizeof(uint16_t));
    ctx->pointer_found = true;
    log("==> negint16 = -%u\n", *((uint16_t*)ctx->item.data) + 1);
}

static void dec_negint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint32_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_32;
    memcpy(ctx->item.data, &value, sizeof(uint32_t));
    ctx->pointer_found = true;
    log("==> negint32 = -%u\n", *((uint32_t*)ctx->item.data) + 1);
}

static void dec_negint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(uint64_t))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_NEGINT;
    ctx->item.metadata.int_metadata.width = CBOR_INT_64;
    memcpy(ctx->item.data, &value, sizeof(uint64_t));
    ctx->pointer_found = true;
    log("==> negint64 = -%" PRIu64 "\n", *((uint64_t*)ctx->item.data) + 1);
}

static void dec_byte_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, length)) {
        return;
    }

    ctx->item.type = CBOR_TYPE_BYTESTRING;
    ctx->item.metadata.bytestring_metadata.type = _CBOR_METADATA_DEFINITE;
    ctx->item.metadata.bytestring_metadata.length = length;
    memcpy(ctx->item.data, data, length);
    ctx->pointer_found = true;

    log("==> bytes =");
    for (int i = 0; i < length; i++) {
        log(" %x", data[i]);
    }
    log("\n");
}

static void dec_float2_callback(void* context, float value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_16;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float2 = %f\n", *((float*)ctx->item.data));
}

static void dec_float4_callback(void* context, float value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_32;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float4 = %f\n", *((float*)ctx->item.data));
}

static void dec_float8_callback(void* context, double value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(float))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_64;
    memcpy(ctx->item.data, &value, sizeof(float));
    ctx->pointer_found = true;
    log("==> float8 = %f\n", *((double*)ctx->item.data));
}

static void dec_null_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = CBOR_CTRL_NULL;
    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    log("==> null\n");
}

static void dec_undefined_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = CBOR_CTRL_UNDEF;
    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    log("==> undefined\n");
}

static void dec_boolean_callback(void* context, bool value) {
    cft_context_t* ctx = context;
    if (!dec_prepare_context_for_value(ctx, sizeof(bool))) {
        return;
    }

    ctx->item.type = CBOR_TYPE_FLOAT_CTRL;
    ctx->item.metadata.float_ctrl_metadata.width = CBOR_FLOAT_0;
    ctx->item.metadata.float_ctrl_metadata.ctrl = value ? CBOR_CTRL_TRUE : CBOR_CTRL_FALSE;
    memcpy(ctx->item.data, &value, sizeof(bool));
    ctx->pointer_found = true;
    log("==> %s\n", *((bool*)ctx->item.data) ? "true" : "false");
}

static void dec_array_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "array is not supported");
    return;
}

static void dec_tag_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "optional tag is not supported");
    return;
}

static void dec_byte_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
    return;
}

static void dec_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
    return;
}

static void dec_indef_array_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite array is not supported");
    return;
}

static void dec_indef_map_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite map is not supported");
    return;
}

static void dec_indef_break_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->pointer_found || ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite break is not supported");
    return;
}

////////////////////////////////////////////////////////////////////////////////

void enc_map_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    struct container_context cc = {0};
    cc.type = CBOR_TYPE_MAP;
    cc.size = size;
    cc.current_index = 0;
    cc.keep_searching = false;

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        strcpy(cc.map_pointer, "/");
        cc.should_ignore = false;
    } else {
        // Check if the specified pointer is a map.
        // If it is a map, then return syntax error, because we should not
        // specify a map. We should specify a key.
        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);
        if (strcmp(ctx->pointer, tmp_pointer) == 0) {
            if (!ctx->erase) {
                ctx->err = CFT_ERR_POINTER_IS_MAP;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "pointer \"%s\" should not be a map", ctx->pointer);
                return;
            }

            cc.should_ignore = true;
        }

        strcpy(cc.map_pointer, cur_cc->map_pointer);
        strcat(cc.map_pointer, cur_cc->key);
        strcat(cc.map_pointer, "/");

        // Before we dive into the map, do we really need to encode and write the map content?
        // If the current map should already be ignored, we should ignore the coming map, too.
        // If the current map should not be ignored, we should check the current key.
        // If the current key is not what we are looking for, we should ignore the coming map.
        if (cur_cc->should_ignore) {
            cc.should_ignore = true;
        }
    }

    // Notice that, when we plan to remove an item from the map, the size of the map pushed to
    // the container context stack should be the original value, or the last element in the map
    // won't see the correct container context, because it will be popped out just before we see
    // the last element.
    push(&cc, ctx->stack, MAX_LEVEL, &(ctx->stack_top));

    if (cc.should_ignore) {
        return;    
    }

    if (ctx->erase && strcmp(cc.map_pointer, ctx->insertion_map_pointer) == 0) {
        // Before erasing an item, we will first search the item. If the item exists,
        // we will store its parent pointer in the insertion_map_pointer variable, and
        // start to look for the item from the beginning of the cbor data again.
        // If the current map pointer is the insertion_map_pointer, it means we have
        // reached the parent map of the item that we want to delete. In this case, we
        // should decrease the parent map size by one, and then encode the parent map
        // starting byte.
        cc.size--;
    }

    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
    size_t written = cbor_encode_map_start(cc.size, buf, sizeof(buf));
    fwrite(buf, written, 1, ctx->fdw);
    ctx->bytes_written += written;
    log("==> map start, size = %" PRIu64 ", map_pointer = %s\n", cc.size, cc.map_pointer);
}

static bool enc_prepare_context_for_value(void* context, bool* write_new_value) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return false;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (!cur_cc) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map\n");
        return false;
    }

    // If this item is a key

    if (strlen(cur_cc->key) == 0) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value cannot be a key\n");
        return false;
    }

    // If this item is a value

    // Check if we need to write a new value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);
    *write_new_value = strcmp(ctx->pointer, tmp_pointer) == 0 ? true : false;

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool should_ignore = cur_cc->should_ignore;
    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        struct container_context* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical to search the next key in the parent map
        }
    }

    return should_ignore ? false : true;
}

static void enc_value(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK || ctx->erase) {
        return;
    }

    cbor_item_t* i = &ctx->item;
    switch (cbor_typeof(i)) {
        case CBOR_TYPE_UINT: {
            unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
            size_t written = 0;
            switch (cbor_int_get_width(i)) {
                case CBOR_INT_8: {
                    written = cbor_encode_uint8(cbor_get_uint8(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_16: {
                    written = cbor_encode_uint16(cbor_get_uint16(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_32: {
                    written = cbor_encode_uint16(cbor_get_uint32(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_64: {
                    written = cbor_encode_uint64(cbor_get_uint64(i), buf, sizeof(buf));
                    break;
                }
            }

            if (!written) {
                ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint (%dB)", _pow(2, cbor_int_get_width(i)));
                return;
            }

            fwrite(buf, written, 1, ctx->fdw);
            ctx->bytes_written += written;
            log("==> set uint (%dB) value = %" PRIu64 "\n", _pow(2, cbor_int_get_width(i)), cbor_get_int(i));
            break;
        };
        case CBOR_TYPE_NEGINT: {
            unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
            size_t written = 0;
            switch (cbor_int_get_width(i)) {
                case CBOR_INT_8: {
                    written = cbor_encode_negint8(cbor_get_uint8(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_16: {
                    written = cbor_encode_negint16(cbor_get_uint16(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_32: {
                    written = cbor_encode_negint16(cbor_get_uint32(i), buf, sizeof(buf));
                    break;
                }
                case CBOR_INT_64: {
                    written = cbor_encode_negint64(cbor_get_uint64(i), buf, sizeof(buf));
                    break;
                }
            }

            if (!written) {
                ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint (%dB)", _pow(2, cbor_int_get_width(i)));
                return;
            }

            fwrite(buf, written, 1, ctx->fdw);
            ctx->bytes_written += written;
            log("==> set negint (%dB) value = -%" PRIu64 "\n", _pow(2, cbor_int_get_width(i)), cbor_get_int(i) + 1);
            break;
        };
        case CBOR_TYPE_BYTESTRING: {
            if (cbor_bytestring_is_indefinite(i)) {
                ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
                return;
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = cbor_encode_bytestring_start(cbor_bytestring_length(i), buf, sizeof(buf));
                if (written == 0) {
                    ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for byte string initial bytes");
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                fwrite(cbor_bytestring_handle(i), cbor_bytestring_length(i), 1, ctx->fdw);
                ctx->bytes_written += written;
                ctx->bytes_written += cbor_bytestring_length(i);
                log("==> set byte string value (%" PRIu64 "B)\n", cbor_bytestring_length(i));
            }
            break;
        };
        case CBOR_TYPE_STRING: {
            if (cbor_string_is_indefinite(i)) {
                ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
                snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
                return;
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = cbor_encode_string_start(cbor_string_length(i), buf, sizeof(buf));
                if (written == 0) {
                    ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for string initial bytes");
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                fwrite(cbor_string_handle(i), cbor_string_length(i), 1, ctx->fdw);
                ctx->bytes_written += written;
                ctx->bytes_written += cbor_string_length(i);
                log("==> set string value = %s\n", cbor_string_handle(i));
            }
            break;
        };
        case CBOR_TYPE_FLOAT_CTRL: {
            if (cbor_float_ctrl_is_ctrl(i)) {
                if (cbor_is_bool(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_bool(cbor_get_bool(i), buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for boolean");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set boolean value = %s\n", cbor_get_bool(i) ? "true" : "false");
                } else if (cbor_is_undef(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_undef(buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for undefined");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set undefined value\n");
                } else if (cbor_is_null(i)) {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_null(buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for null");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set null value\n");
                } else {
                    unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                    size_t written = cbor_encode_ctrl(cbor_ctrl_value(i), buf, sizeof(buf));
                    if (!written) {
                        ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for simple value");
                        return;
                    }

                    fwrite(buf, written, 1, ctx->fdw);
                    ctx->bytes_written += written;
                    log("==> set simple value = %u\n", cbor_ctrl_value(i));
                }
            } else {
                unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
                size_t written = 0;
                switch (cbor_float_get_width(i)) {
                    case CBOR_FLOAT_16:
                        written = cbor_encode_half(cbor_float_get_float2(i), buf, sizeof(buf));
                        break;
                    case CBOR_FLOAT_32:
                        written = cbor_encode_single(cbor_float_get_float4(i), buf, sizeof(buf));
                        break;
                    case CBOR_FLOAT_64:
                        written = cbor_encode_double(cbor_float_get_float8(i), buf, sizeof(buf));
                        break;
                }

                if (!written) {
                    ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
                    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float (%dB)", _pow(2, cbor_float_get_width(i)));
                    return;
                }

                fwrite(buf, written, 1, ctx->fdw);
                ctx->bytes_written += written;
                log("==> set float value = %lf (%dB)\n", cbor_float_get_float(i), _pow(2, cbor_float_get_width(i)));
            }
            break;
        };
    }

    ctx->pointer_found = true;
}

void enc_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    container_context_t* cur_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
    if (cur_cc == NULL) {
        ctx->err = CFT_ERR_MALFORMATED_DATA;
        snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "the value is not inside a map");
        return;
    }

    // If this string is a key

    if (strlen(cur_cc->key) == 0) {
        memset(cur_cc->key, 0, sizeof(cur_cc->key));
        memcpy(cur_cc->key, data, length);

        char tmp_pointer[MAX_POINTER_LEN] = {0};
        strcpy(tmp_pointer, cur_cc->map_pointer);
        strcat(tmp_pointer, cur_cc->key);

        if (ctx->erase && (strcmp(ctx->pointer, tmp_pointer) == 0 || cur_cc->should_ignore)) {
            return;
        }

        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_string_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for data item initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    // If this string is a value

    // Check if we need to write a new value
    char tmp_pointer[MAX_POINTER_LEN] = {0};
    strcpy(tmp_pointer, cur_cc->map_pointer);
    strcat(tmp_pointer, cur_cc->key);

    // Because this is a value, we need to clear the key so that the next time we see a string, we will know it is a key.
    memset(cur_cc->key, 0, sizeof(cur_cc->key));

    bool should_ignore = cur_cc->should_ignore;
    cur_cc->current_index++;
    if (cur_cc->current_index >= cur_cc->size) {
        // This is the last value in the map, so we're going to pop up the container context
        pop(ctx->stack, MAX_LEVEL, &(ctx->stack_top));

        // Get the parent container context of the current value
        struct container_context* parent_cc = get_top(ctx->stack, MAX_LEVEL, ctx->stack_top);
        if (parent_cc) {
            memset(parent_cc->key, 0, sizeof(parent_cc->key));  // critical
        }
    }

    if (should_ignore) {
        return;
    }

    if (strcmp(ctx->pointer, tmp_pointer) != 0) {
        // If the key is not specified by user, it means we need to write the existing value.
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_string_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for string initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    // If we reach this point, it means we need to write the new value.
    enc_value(ctx);
}

void enc_uint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint8(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint16(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint16");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint32(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint32");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_uint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_uint64(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for uint64");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint8_callback(void* context, uint8_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint8(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint16_callback(void* context, uint16_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint16(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint16");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint32_callback(void* context, uint32_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint32(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint32");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_negint64_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_negint64(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for negint64");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_byte_string_callback(void* context, cbor_data data, size_t length) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_bytestring_start(length, buf, sizeof(buf));
        if (written == 0) {
            ctx->err = CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for byte string initial bytes");
            return;
        }

        fwrite(buf, written, 1, ctx->fdw);
        fwrite(data, length, 1, ctx->fdw);
        ctx->bytes_written += written;
        ctx->bytes_written += length;
        return;
    }

    enc_value(ctx);
}

void enc_float2_callback(void* context, float value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_half(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float2");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_float4_callback(void* context, float value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_single(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float4");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_float8_callback(void* context, double value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_double(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for float8");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_null_callback(void* context) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_null(buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for null");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_undefined_callback(void* context) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_undef(buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for undefined");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_boolean_callback(void* context, bool value) {
    cft_context_t* ctx = context;
    bool write_new_value;
    if (!enc_prepare_context_for_value(ctx, &write_new_value)) {
        return;
    }

    if (!write_new_value) {
        unsigned char buf[MAX_INIT_BYTES_LEN] = {0};
        size_t written = cbor_encode_bool(value, buf, sizeof(buf));
        if (!written) {
            ctx->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for boolean");
            return;
        }
        fwrite(buf, written, 1, ctx->fdw);
        ctx->bytes_written += written;
        return;
    }

    enc_value(ctx);
}

void enc_array_start_callback(void* context, size_t size) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "array is not supported");
    return;
}

void enc_tag_callback(void* context, uint64_t value) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "optional tag is not supported");
    return;
}

void enc_byte_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite byte string is not supported");
    return;
}

void enc_string_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite string is not supported");
    return;
}

void enc_indef_array_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite array is not supported");
    return;
}

void enc_indef_map_start_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite map is not supported");
    return;
}

void enc_indef_break_callback(void* context) {
    cft_context_t* ctx = context;
    if (ctx->err != CFT_ERR_OK) {
        return;
    }

    ctx->err = CFT_ERR_CBOR_TYPE_NOT_ALLOWED;
    snprintf(ctx->err_msg, MAX_ERR_MSG_LEN, "indefinite break is not supported");
    return;
}

////////////////////////////////////////////////////////////////////////////////

static cbor_item_t* get_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->erase = false;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return NULL;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->dec_callbacks), h);
        if (h->pointer_found || h->err != CFT_ERR_OK || strlen(h->insertion_map_pointer) > strlen(ROOT_MAP_POINTER)) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    if (h->err != CFT_ERR_OK) {
        return NULL;
    }

    if (!h->pointer_found) {
        h->err = CFT_ERR_POINTER_NOT_FOUND;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" doesn't exist, but \"%s\" exists\n", h->pointer, h->insertion_map_pointer);
        return NULL;
    }

    return &h->item;
}

static cft_err_t set_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->erase = false;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    // First we need to prepare a temp file for storing the new CBOR data
    char* tmp_name = tempnam(NULL, NULL);
    h->fdw = fopen(tmp_name, "wb");
    if (h->fdw == NULL) {
        h->err = CFT_ERR_CREATE_TEMP_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open temp file \"%s\"", tmp_name);
        free(tmp_name);
        return h->err;
    }

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        fclose(h->fdw);
        h->fdw = NULL;
        remove(tmp_name);
        free(tmp_name);
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->enc_callbacks), h);
        if (h->err != CFT_ERR_OK) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    fclose(h->fdw);
    h->fdw = NULL;

    // Delete the original file and rename the temp file
    remove(h->path);
    rename(tmp_name, h->path);
    free(tmp_name);

    if (!h->pointer_found) {
        h->err = CFT_ERR_POINTER_NOT_FOUND;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" doesn't exist\n", h->pointer);
        return h->err;
    }

    return h->err;
}

static cft_err_t erase_item(cft_context_t* h, const char* pointer) {
    memset(h->pointer, 0, sizeof(h->pointer));
    strncpy(h->pointer, pointer, strlen(pointer));
    h->stack_top = -1;
    h->pointer_found = false;
    h->erase = true;
    h->err = CFT_ERR_OK;
    h->bytes_written = 0;

    // First we need to prepare a temp file for storing the new CBOR data
    char* tmp_name = tempnam(NULL, NULL);
    h->fdw = fopen(tmp_name, "wb");
    if (h->fdw == NULL) {
        h->err = CFT_ERR_CREATE_TEMP_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open temp file \"%s\"", tmp_name);
        free(tmp_name);
        return h->err;
    }

    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        fclose(h->fdw);
        h->fdw = NULL;
        remove(tmp_name);
        free(tmp_name);
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_SET);
    fread(h->content, h->content_size, 1, h->fd);

    size_t bytes_read = 0;
    struct cbor_decoder_result decode_result;
    while (bytes_read < h->content_len) {
        decode_result = cbor_stream_decode(h->content, h->content_size, &(h->enc_callbacks), h);
        if (h->err != CFT_ERR_OK) {
            break;
        }

        bytes_read += decode_result.read;
        fseek(h->fd, bytes_read, SEEK_SET);
        fread(h->content, h->content_size, 1, h->fd);
    }

    fclose(h->fd);
    h->fd = NULL;

    fclose(h->fdw);
    h->fdw = NULL;

    // Delete the original file and rename the temp file
    remove(h->path);
    rename(tmp_name, h->path);
    free(tmp_name);

    return h->err;
}

uint8_t cft_get_uint8(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_uint(i) || cbor_int_get_width(i) != CBOR_INT_8) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a uint8\n", h->pointer);
        return 0;
    }

    return cbor_get_uint8(i);
}

uint16_t cft_get_uint16(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_uint(i) || cbor_int_get_width(i) != CBOR_INT_16) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a uint16\n", h->pointer);
        return 0;
    }

    return cbor_get_uint16(i);
}

const unsigned char* cft_get_sz(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL) {
        return 0;
    }

    if (!cbor_isa_string(i)) {
#if ENABLE_LOG == 1
        cbor_describe(i, stdout);
#endif

        h->err = CFT_ERR_WRONG_DATA_TYPE;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "\"%s\" should be a null-terminated string\n", h->pointer);
        return 0;
    }

    return cbor_string_handle(i);
}

cft_err_t cft_set_sz(cft_context_t* h, const char* pointer, const unsigned char* v, unsigned char* old, size_t old_size) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL && h->err != CFT_ERR_POINTER_NOT_FOUND) {
        return h->err;
    }

    if (i != NULL) {
        // the pointer exists, so we need to set the new value.

        // If user wants to know the original value, copy the old value to the provided buffer.
        // Returning the old value is a useful feature, since user can undo the modification later.
        if (old != NULL) {
            if (old_size <= i->metadata.string_metadata.length) {
                h->err = CFT_ERR_INSUFFICIENT_BUFFER;
                snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", i->metadata.string_metadata.length);
                return h->err;
            }

            memset(old, 0, old_size);
            memcpy(old, i->data, i->metadata.string_metadata.length);
        }

        size_t new_size = strlen(v);
        if (new_size >= h->data_size) {
            h->err = CFT_ERR_INSUFFICIENT_BUFFER;
            snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough for value (%" PRIu64 " bytes)", new_size);
            return h->err;
        }

        h->item.type = CBOR_TYPE_STRING;
        h->item.metadata.string_metadata.type = _CBOR_METADATA_DEFINITE;
        h->item.metadata.string_metadata.length = new_size;
        memset(h->item.data, 0, h->data_size);
        memcpy(h->item.data, v, new_size);
        cft_err_t res = set_item(h, pointer);
        if (res != CFT_ERR_OK) {
            return h->err;
        }

        return h->err;
    }

    if (h->err == CFT_ERR_POINTER_NOT_FOUND) {
        // We should insert the key into h->insertion_map_pointer.
        // However, since we haven't implemented the insert_item() function, return error for now.
        return h->err;
    }

    return h->err;
}

cft_err_t cft_erase(cft_context_t* h, const char* pointer) {
    cbor_item_t* i = get_item(h, pointer);
    if (i == NULL && h->err != CFT_ERR_POINTER_IS_MAP) {
        return h->err;
    }

    char* p = strrchr(pointer, '/');
    if (p == NULL) {
        fprintf(stderr, "Fatal error: cannot find '/' in the pointer \"%s\"\n", pointer);
        return CFT_ERR_POINTER_NOT_FOUND;
    }

    size_t count = p - pointer + 1;
    memset(h->insertion_map_pointer, 0, sizeof(h->insertion_map_pointer));
    memcpy(h->insertion_map_pointer, pointer, count);

    log("------------------------------> insertion_map_pointer: \"%s\"\n", h->insertion_map_pointer);

    cft_err_t res = erase_item(h, pointer);
    if (res != CFT_ERR_OK) {
        fprintf(stderr, "Fatal error: fail to erase existing item \"%s\"\n", pointer);
        return res;
    }

    return h->err;
}

cft_err_t cft_init(cft_context_t* h, const char* path) {
    if (strlen(path) >= sizeof(h->path)) {
        h->err = CFT_ERR_INSUFFICIENT_PATH_BUFFER;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "buffer is not large enough to store path \"%s\"", path);
        return h->err;
    }
    
    memset(h, 0, sizeof(cft_context_t));
    strncpy(h->path, path, MAX_PATH_LEN);    
    h->fd = fopen(h->path, "rb");
    if (h->fd == NULL) {
        h->err = CFT_ERR_OPEN_FILE_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to open path \"%s\"", h->path);
        return h->err;
    }

    fseek(h->fd, 0, SEEK_END);
    h->content_len = (size_t)ftell(h->fd);
    fclose(h->fd);
    h->fd = NULL;

    h->content_size = MAX_SCAN_BUF_LEN;
    h->content = malloc(h->content_size);
    if (h->content == NULL) {
        h->err = CFT_ERR_ALLOC_BUFFER_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to allocate content buffer");
        return h->err;
    }

    h->dec_callbacks.uint8 = dec_uint8_callback;
    h->dec_callbacks.uint16 = dec_uint16_callback;
    h->dec_callbacks.uint32 = dec_uint32_callback;
    h->dec_callbacks.uint64 = dec_uint64_callback;
    h->dec_callbacks.negint8 = dec_negint8_callback;
    h->dec_callbacks.negint16 = dec_negint16_callback;
    h->dec_callbacks.negint32 = dec_negint32_callback;
    h->dec_callbacks.negint64 = dec_negint64_callback;
    h->dec_callbacks.byte_string = dec_byte_string_callback;
    h->dec_callbacks.byte_string_start = dec_byte_string_start_callback;
    h->dec_callbacks.string = dec_string_callback;
    h->dec_callbacks.string_start = dec_string_start_callback;
    h->dec_callbacks.array_start = dec_array_start_callback;
    h->dec_callbacks.indef_array_start = dec_indef_array_start_callback;
    h->dec_callbacks.map_start = dec_map_start_callback;
    h->dec_callbacks.indef_map_start = dec_indef_map_start_callback;
    h->dec_callbacks.tag = dec_tag_callback;
    h->dec_callbacks.null = dec_null_callback;
    h->dec_callbacks.undefined = dec_undefined_callback;
    h->dec_callbacks.boolean = dec_boolean_callback;
    h->dec_callbacks.float2 = dec_float2_callback;
    h->dec_callbacks.float4 = dec_float4_callback;
    h->dec_callbacks.float8 = dec_float8_callback;
    h->dec_callbacks.indef_break = dec_indef_break_callback;

    h->enc_callbacks.uint8 = enc_uint8_callback;
    h->enc_callbacks.uint16 = enc_uint16_callback;
    h->enc_callbacks.uint32 = enc_uint32_callback;
    h->enc_callbacks.uint64 = enc_uint64_callback;
    h->enc_callbacks.negint8 = enc_negint8_callback;
    h->enc_callbacks.negint16 = enc_negint16_callback;
    h->enc_callbacks.negint32 = enc_negint32_callback;
    h->enc_callbacks.negint64 = enc_negint64_callback;
    h->enc_callbacks.byte_string = enc_byte_string_callback;
    h->enc_callbacks.byte_string_start = enc_byte_string_start_callback;
    h->enc_callbacks.string = enc_string_callback;
    h->enc_callbacks.string_start = enc_string_start_callback;
    h->enc_callbacks.array_start = enc_array_start_callback;
    h->enc_callbacks.indef_array_start = enc_indef_array_start_callback;
    h->enc_callbacks.map_start = enc_map_start_callback;
    h->enc_callbacks.indef_map_start = enc_indef_map_start_callback;
    h->enc_callbacks.tag = enc_tag_callback;
    h->enc_callbacks.null = enc_null_callback;
    h->enc_callbacks.undefined = enc_undefined_callback;
    h->enc_callbacks.boolean = enc_boolean_callback;
    h->enc_callbacks.float2 = enc_float2_callback;
    h->enc_callbacks.float4 = enc_float4_callback;
    h->enc_callbacks.float8 = enc_float8_callback;
    h->enc_callbacks.indef_break = enc_indef_break_callback;

    h->item.data = (uint8_t*)malloc(MAX_DATA_LEN);
    if (h->item.data == NULL) {
        h->err = CFT_ERR_ALLOC_BUFFER_ERROR;
        snprintf(h->err_msg, MAX_ERR_MSG_LEN, "fail to allocate buffer");
        return h->err;
    }

    h->data_size = MAX_DATA_LEN;
    h->pointer_found = false;
    h->stack_top = -1;
    h->err = CFT_ERR_OK;
    memset(h->insertion_map_pointer, 0, sizeof(h->insertion_map_pointer));
    strncpy(h->insertion_map_pointer, ROOT_MAP_POINTER, MAX_POINTER_LEN);

    return CFT_ERR_OK;
}

void cft_uninit(cft_context_t* h) {
    free(h->item.data);
    free(h->content);
}