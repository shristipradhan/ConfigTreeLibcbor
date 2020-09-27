#ifndef CFT_H
#define CFT_H

#include <stdio.h>
#include <inttypes.h>

#include "cbor.h"

#define MAX_LEVEL          16
#define MAX_POINTER_LEN    256
#define MAX_ERR_MSG_LEN    128
#define MAX_DATA_LEN       1024
#define MAX_SCAN_BUF_LEN   1024
#define MAX_INIT_BYTES_LEN 8
#define MAX_PATH_LEN       256
#define ENABLE_LOG         1
#define ROOT_MAP_POINTER "/"

typedef enum cft_err {
    CFT_ERR_OK,
    CFT_ERR_POINTER_NOT_FOUND,
    CFT_ERR_WRONG_DATA_TYPE,
    CFT_ERR_INSUFFICIENT_BUFFER,
    CFT_ERR_INSUFFICIENT_INIT_BYTES_BUFFER,
    CFT_ERR_INSUFFICIENT_PATH_BUFFER,
    CFT_ERR_ALLOC_BUFFER_ERROR,
    CFT_ERR_CBOR_TYPE_NOT_ALLOWED,
    CFT_ERR_MALFORMATED_DATA,
    CFT_ERR_POINTER_IS_MAP,
    CFT_ERR_CREATE_TEMP_FILE_ERROR,
    CFT_ERR_OPEN_FILE_ERROR
} cft_err_t;

typedef struct container_context {
    cbor_type type;
    size_t size;
    int current_index;
    char key[MAX_POINTER_LEN + 1];
    bool keep_searching;
    bool should_ignore;
    char map_pointer[MAX_POINTER_LEN + 1];
} container_context_t;

typedef struct cft_context {
    cft_err_t err;                                    ///< Error code
    char err_msg[MAX_ERR_MSG_LEN + 1];                ///< Error message
    cbor_item_t item;                                 ///< CBOR item found
    size_t data_size;                                 ///< Size of the buffer used to hold the value
    char pointer[MAX_POINTER_LEN + 1];                ///< JSON Pointer of the key we want to search
    bool pointer_found;                               ///< Indicate whether the key is found or not
    char insertion_map_pointer[MAX_POINTER_LEN + 1];  ///< JSON Pointer of the map where we can insert the key
    container_context_t stack[MAX_LEVEL];             ///< Stack of the container context
    int stack_top;                                    ///< Top of the container context stack
    struct cbor_callbacks dec_callbacks;              ///< Callbacks for decoding
    struct cbor_callbacks enc_callbacks;              ///< Callbacks for encoding
    uint8_t* content;                                 ///< Buffer for holding partial CBOR data
    size_t content_size;                              ///< Size of the buffer holding partial CBOR data
    size_t content_len;                               ///< Total length of the CBOR data
    FILE* fd;                                         ///< CBOR data file descriptor for reading data
    FILE* fdw;                                        ///< CBOR data file descriptor for writing data
    size_t bytes_written;                             ///< Bytes that have been written to fdw
    char path[MAX_PATH_LEN + 1];                      ///< CBOR data file path
    bool insert;
    bool set;
    bool erase;                                       ///< Indicate whether we need to erase the pointer
} cft_context_t;

cft_err_t cft_init(cft_context_t* h, const char* path);
void cft_uninit(cft_context_t* h);
uint8_t cft_get_uint8(cft_context_t* h, const char* pointer);
uint16_t cft_get_uint16(cft_context_t* h, const char* pointer);
const unsigned char* cft_get_sz(cft_context_t* h, const char* pointer);
cft_err_t cft_set_sz(cft_context_t* h, const char* pointer, const unsigned char* v, unsigned char* old, size_t old_size);
cft_err_t cft_erase(cft_context_t* h, const char* pointer);

#endif
