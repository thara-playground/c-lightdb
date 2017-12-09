#include <fcntl.h>
#include <mhash.h>

#include "node.h"
#include "table.h"

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
