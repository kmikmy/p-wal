#include "ARIES.h"
#include <map>

PageBufferEntry page_table[PAGE_N];
std::map<uint32_t, uint32_t> dirty_page_table;
char *ARIES_HOME;

