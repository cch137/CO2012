#ifndef DB_INTERACTION_H
#define DB_INTERACTION_H

#define OK "OK"

#include "types.h"

DBRequest *create_request(db_action_t action);

DBReply *create_reply();

void add_request_arg(DBRequest *request, DBObj *arg);

DBRequest *reset_request(DBRequest *request, db_action_t action);

void free_request(DBRequest *request);

void free_reply(DBReply *reply);

DBObj *print_dbobj(DBObj *obj);

DBReply *print_reply(DBReply *reply);

DBReply *reply_error(DBReply *reply, const char *message);

DBReply *reply_data(DBReply *reply, DBObj *data);

char *get_string_arg(DBListNode *curr_node);
db_uint_t get_uint_arg(DBListNode *curr_node);
db_int_t get_int_arg(DBListNode *curr_node);

DBObj *arg_string_to_uint(DBObj *obj);

DBObj *arg_string_to_int(DBObj *obj);

#endif