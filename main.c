#include <stdio.h>
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

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } statement_type_t ;
typedef struct {
    statement_type_t type;
} statement_t;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT } prepare_result_t;

prepare_result_t prepare_statement(input_buffer_t* input, statement_t* st) {
    if (strncmp(input->buffer, "insert", 6) == 0) {
        st->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strncmp(input->buffer, "select", 6) == 0) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(statement_t* st) {
    switch (st->type) {
        case (STATEMENT_INSERT) :
            printf("This is where we would do an insert.\n");
            break;
        case (STATEMENT_SELECT) :
            printf("This is where we would do an select.\n");
            break;
    }
}

int main(int argc, char* argv[]) {
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
            case (PREPARE_UNRECOGNIZED_STATEMENT) :
                printf("Unrecognized keyword at start of '%s'.\n",  input->buffer);
                continue;
        }

        execute_statement(&st);
        printf("Executed.\n");
    }

    return 0;
}