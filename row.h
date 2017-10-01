//
// Created by Tomochika Hara on 2017/10/02.
//

#pragma once
#include <stdint.h>

const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} row_t;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(row_t, id);
const uint32_t USERNAME_SIZE = size_of_attribute(row_t, username);
const uint32_t EMAIL_SIZE = size_of_attribute(row_t, email);

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;