//
// Created by Tomochika Hara on 2017/12/09.
//

#pragma once

const uint32_t TABLE_MAX_PAGES = 100;

typedef struct {
    int file_descriptor;
    off_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} pager_t;

typedef struct {
    pager_t* pager;
    uint32_t root_page_num;
} table_t;

typedef struct {
    table_t* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;  // Indicates a position one past the last element
} cursor_t;