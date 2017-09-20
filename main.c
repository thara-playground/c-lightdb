#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

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

void print_prompt() { printf("db > "); }

void read_input(input_buffer_t* input) {
    ssize_t bytes_read = getline(&(input->buffer), &(input->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input->input_length = bytes_read - 1;   // Ignore trailing newline
    input->buffer[bytes_read - 1] = 0;
}

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} meta_command_result_t;

meta_command_result_t do_meta_command(input_buffer_t* input) {
    if (strcmp(input->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

typedef struct {
    uint32_t id;
    uint8_t username[COLUMN_USERNAME_SIZE];
    uint8_t email[COLUMN_EMAIL_SIZE];
} row_t;

void print_row(row_t* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } statement_type_t ;
typedef struct {
    statement_type_t type;
    row_t row_to_insert;
} statement_t;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR } prepare_result_t;

prepare_result_t prepare_statement(input_buffer_t* input, statement_t* st) {
    if (strncmp(input->buffer, "insert", 6) == 0) {
        st->type = STATEMENT_INSERT;
        int assigned = sscanf(input->buffer, "insert %d %s %s", &(st->row_to_insert.id), st->row_to_insert.username,  st->row_to_insert.email);
        if (assigned != 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strncmp(input->buffer, "select", 6) == 0) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}


#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(row_t, id);
const uint32_t USERNAME_SIZE = size_of_attribute(row_t, username);
const uint32_t EMAIL_SIZE = size_of_attribute(row_t, email);

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

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

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    void* pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
} table_t;

void* row_slot(table_t* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if (!page) {
        // Allocate memory only when wh try to access page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} execute_result_t;

execute_result_t execute_insert(statement_t* st, table_t* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    row_t* row = &(st->row_to_insert);
    serialize_row(row, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

execute_result_t execute_select(statement_t* st, table_t* table) {
    row_t row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

table_t* new_table() {
    table_t* table = malloc(sizeof(table_t));
    table->num_rows = 0;
    return table;
}


execute_result_t execute_statement(statement_t* st, table_t* table) {
    switch (st->type) {
        case (STATEMENT_INSERT) :
            return execute_insert(st, table);
        case (STATEMENT_SELECT) :
            return execute_select(st, table);
    }
}

int main(int argc, char* argv[]) {
    table_t* table = new_table();

    input_buffer_t* input = new_input_buffer();

    while (true) {
        print_prompt();
        read_input(input);

        if (input->buffer[0] == '.') {
            switch (do_meta_command(input)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input->buffer);
                    continue;
            }
        }

        statement_t st;
        switch (prepare_statement(input, &st)) {
            case (PREPARE_SUCCESS) :
                break;
            case (PREPARE_SYNTAX_ERROR) :
                printf("Syntax error. Could not parse statement.\n");
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT) :
                printf("Unrecognized keyword at start of '%s'.\n",  input->buffer);
                continue;
        }

        switch (execute_statement(&st, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }

    return 0;
}