#ifndef _debug
#define _debug

#define NNN       do {fprintf(stderr, "%ld %16s %4d %16s\n", (long int)pthread_self(), __FILE__, __LINE__, __func__); fflush(stderr);} while (0)
#define ERR       do {perror("ERROR"); NNN; exit(1);} while (0)
#define PERR(val) do {perror(val); NNN; exit(1);} while (0)

#endif
