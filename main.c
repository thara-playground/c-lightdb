#include <fcntl.h>
#include <mhash.h>

#include "btree.h"

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} input_buffer_t;

input_buffer_t* new_input_buffer() {
    input_buffer_t* bf = malloc(sizeof(input_buffer_t));
    bf->buffer = NULL;
    bf->buffer_length = 0;
    bf->input_length = 0;
    return bf;
}

void print_prompt() {
    printf("db > ");
}

void read_input(input_buffer_t* input) {
    ssize_t bytes_read = getline(&(input->buffer), &(input->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input->input_length = bytes_read - 1; // Ignore trailing newline
    input->buffer[bytes_read - 1] = 0;
}

void print_row(row_t* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } statement_type_t;
typedef struct {
    statement_type_t type;
    row_t row_to_insert;
} statement_t;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
} prepare_result_t;

prepare_result_t prepare_insert(input_buffer_t* input, statement_t* st) {
    st->type = STATEMENT_INSERT;
    char* keyword = strtok(input->buffer, " ");
    char* id_str = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_str == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_str);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (COLUMN_USERNAME_SIZE < strlen(username)) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (COLUMN_EMAIL_SIZE < strlen(email)) {
        return PREPARE_STRING_TOO_LONG;
    }

    st->row_to_insert.id = (uint32_t)id;
    strcpy(st->row_to_insert.username, username);
    strcpy(st->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

prepare_result_t prepare_statement(input_buffer_t* input, statement_t* st) {
    if (strncmp(input->buffer, "insert", 6) == 0) {
        return prepare_insert(input, st);
    }
    if (strncmp(input->buffer, "select", 6) == 0) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void serialize_row(row_t* src, void* dest) {
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void* src, row_t* dest) {
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

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

void* get_page(pager_t* pager, uint32_t page_num);

cursor_t* leaf_node_find(table_t* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    cursor_t* cur = malloc(sizeof(cursor_t));
    cur->table = table;
    cur->page_num = page_num;

    // Binary Search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;

    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cur->cell_num = index;
            return cur;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cur->cell_num = min_index;
    return cur;
}

cursor_t* internal_node_find(table_t* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t min_index = 0;
    uint32_t max_index = num_keys;  // there is one more child than than key

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key <= key_to_right) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    uint32_t child_num = *internal_node_child(node, min_index);
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }
}

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

void* get_page(pager_t* pager, uint32_t page_num) {
    if (TABLE_MAX_PAGES < page_num) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = (uint32_t)(pager->file_length / PAGE_SIZE);

        // We might save a partial page at the end of the file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

void* cursor_value(cursor_t* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

// cursor_advance
void cursor_next(cursor_t* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(page))) {
        // Advance to next leaf node
        uint32_t next_page_num = *leaf_node_next_leaf(page);
        if (next_page_num == 0) {
            // This was rightmost leaf
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

uint32_t get_unused_page_num(pager_t* pager) { return pager->num_pages; }

void create_new_root(table_t* table, uint32_t right_child_page_num) {
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);

    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}

void leaf_node_split_and_insert(cursor_t* cursor, uint32_t key, row_t* value) {
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    for (int32_t i = LEAF_NODE_MAX_CELLS; 0 <= i; i--) {
        void* dest_node;
        if (LEAF_NODE_LEFT_SPLIT_COUNT <= i) {
            dest_node = new_node;
        } else {
            dest_node = old_node;
        }
        uint32_t index = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* dest = leaf_node_cell(dest_node, index);

        if (i == cursor->cell_num) {
            serialize_row(value, leaf_node_value(dest_node, index));
            *leaf_node_key(dest_node, index) = key;
        } else if (cursor->cell_num < i) {
            memcpy(dest, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(dest, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        printf("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}

void leaf_node_insert(cursor_t* cursor, uint32_t key, row_t* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (LEAF_NODE_MAX_CELLS <= num_cells) {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i = num_cells; cursor->cell_num < i; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, EXECUTE_DUPLICATE_KEY } execute_result_t;

execute_result_t execute_insert(statement_t* st, table_t* table) {
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));

    row_t* row = &(st->row_to_insert);
    uint32_t key = row->id;
    cursor_t* cur = table_find(table, key);

    if (cur->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cur->cell_num);
        if (key_at_index == key) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cur, row->id, row);

    free(cur);
    return EXECUTE_SUCCESS;
}

execute_result_t execute_select(statement_t* st, table_t* table) {
    cursor_t* cur = table_start(table);
    row_t row;
    while (!(cur->end_of_table)) {
        deserialize_row(cursor_value(cur), &row);
        print_row(&row);
        cursor_next(cur);
    }
    free(cur);
    return EXECUTE_SUCCESS;
}

pager_t* pager_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    pager_t* pager = malloc(sizeof(pager_t));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (uint32_t) (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
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

void pager_flush(pager_t* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
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

execute_result_t execute_statement(statement_t* st, table_t* table) {
    switch (st->type) {
    case (STATEMENT_INSERT):
        return execute_insert(st, table);
    case (STATEMENT_SELECT):
        return execute_select(st, table);
    }
}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void print_tree(pager_t* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);

    switch (get_node_type(node)) {
        case NODE_LEAF:
        {
            uint32_t num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            printf("- leaf (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                printf("- %d\n", *leaf_node_key(node, i));
            }
            break;
        }
        case NODE_INTERNAL:
        {
            uint32_t num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            printf("- internal (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                uint32_t child = *internal_node_child(node, i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level);
                printf("- key %d\n", *internal_node_key(node, i));
            }
            uint32_t child = *internal_node_right_child(node);
            print_tree(pager, child, indentation_level + 1);
            break;
        }
    }
}

typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND } meta_command_result_t;

meta_command_result_t do_meta_command(input_buffer_t* input, table_t* table) {
    if (strcmp(input->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    table_t* table = db_open(filename);

    input_buffer_t* input = new_input_buffer();

    while (true) {
        print_prompt();
        read_input(input);

        if (input->buffer[0] == '.') {
            switch (do_meta_command(input, table)) {
            case (META_COMMAND_SUCCESS):
                continue;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecognized command '%s'\n", input->buffer);
                continue;
            }
        }

        statement_t st;
        switch (prepare_statement(input, &st)) {
        case (PREPARE_SUCCESS):
            break;
        case (PREPARE_NEGATIVE_ID):
            printf("ID must be positive.\n");
            continue;
        case (PREPARE_STRING_TOO_LONG):
            printf("String is too long.\n");
            continue;
        case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            break;
        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n", input->buffer);
            continue;
        }

        switch (execute_statement(&st, table)) {
        case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;
        case (EXECUTE_DUPLICATE_KEY):
            printf("Error: Duplicate key.\n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("Error: Table full.\n");
            break;
        }
    }

    return 0;
}
