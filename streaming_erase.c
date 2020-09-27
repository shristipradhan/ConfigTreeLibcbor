#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "cft.h"

int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("Usage: steaming_erase <input file> <pointer>\n");
        return 1;
    }

    const char* path = argv[1];
    const char* pointer = argv[2];
//    const char* value = argv[3];

    cft_context_t h = {0};
    cft_err_t res = cft_init(&h, path);
    if (res != CFT_ERR_OK) {
        printf("error: fail to initialize the cft library\n");
        return 1;
    }

    res = cft_erase(&h, pointer);
    if (h.err != CFT_ERR_OK) {
        printf("error(%d): %s\n", h.err, h.err_msg);
        return 1;
    }

    cft_uninit(&h);
    return 0;
}
