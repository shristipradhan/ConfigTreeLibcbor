#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#include "cft.h"

int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("Usage: streaming_parser [input file]\n");
        return 1;
    }

    const char* path = argv[1];
    const char* pointer = argv[2];

    cft_context_t h = {0};
    cft_err_t res = cft_init(&h, path);
    if (res != CFT_ERR_OK) {
        printf("error: fail to initialize the cft library\n");
        return 1;
    }
    
    const unsigned char* v_sz = cft_get_sz(&h, pointer);
    if (h.err != CFT_ERR_OK) {
        printf("error(%d): %s\n", h.err, h.err_msg);
        return 1;
    }
    printf("%s = %s (%" PRIu64 " bytes)\n", pointer, v_sz, strlen(v_sz));

    cft_uninit(&h);
    return 0;
}
