//
// Created by Tomochika Hara on 2017/12/09.
//

#pragma once

#include "node.h"
#include "btree.h"
#include "values.h"

cursor_t* table_find(table_t* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}

cursor_t* table_start(table_t* table) {
    cursor_t* cur = table_find(table, 0);

    void* node = get_page(table->pager, cur->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cur->end_of_table = (num_cells == 0);
    return cur;
}

table_t* db_open(const char* filename) {
    pager_t* pager = pager_open(filename);

    table_t* table = malloc(sizeof(table_t));
    table->pager = pager;

    if (pager->num_pages == 0) {
        // New database file. Initialze page 0 as leaf node.
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
    return table;
}

void db_close(table_t* table) {
    pager_t* pager = table->pager;

    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Erro closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
}