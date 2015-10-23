#ifndef _query
#define _query

typedef struct QueryArg_{
  char *field_name;
  char *before;
  char *after;
  struct QueryArg_ *nxt;
} QueryArg;

#endif
