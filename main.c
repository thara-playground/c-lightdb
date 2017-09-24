#include <fcntl.h>
#include <mhash.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
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

const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} row_t;

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
    int file_descriptor;
    off_t file_length;
    void* pages[TABLE_MAX_PAGES];
} pager_t;

typedef struct {
    pager_t* pager;
    uint32_t num_rows;
} table_t;

typedef struct {
    table_t* table;
    uint32_t row_num;
    bool end_of_table;  // Indicates a position one past the last element
} cursor_t;

cursor_t* table_start(table_t* table) {
    cursor_t* cur = malloc(sizeof(cursor_t));
    cur->table = table;
    cur->row_num = 0;
    cur->end_of_table = (table->num_rows == 0);
    return cur;
}

cursor_t* table_end(table_t* table) {
    cursor_t* cur = malloc(sizeof(cursor_t));
    cur->table = table;
    cur->row_num = table->num_rows;
    cur->end_of_table = true;
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
    }

    return pager->pages[page_num];
}

void* cursor_value(cursor_t* cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void cursor_next(cursor_t* cursor) {
    cursor->row_num += 1;
    if (cursor->table->num_rows <= cursor->row_num) {
        cursor->end_of_table = true;
    }
}

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } execute_result_t;

execute_result_t execute_insert(statement_t* st, table_t* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    row_t* row = &(st->row_to_insert);
    cursor_t* cur = table_end(table);
    serialize_row(row, cursor_value(cur));
    table->num_rows += 1;
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

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

table_t* db_open(const char* filename) {
    pager_t* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    table_t* table = malloc(sizeof(table_t));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
}

void pager_flush(pager_t* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(table_t* table) {
    pager_t* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // There may be a partial page to write to the end of the file.
    // This should not be needed after we switch to a B-tree.
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (0 < num_additional_rows) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
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

typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND } meta_command_result_t;

meta_command_result_t do_meta_command(input_buffer_t* input, table_t* table) {
    if (strcmp(input->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
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
        case (EXECUTE_TABLE_FULL):
            printf("Error: Table full.\n");
            break;
        }
    }

    return 0;
}
