#include <gem5/m5ops.h>
#include <stdio.h>

int
main(void)
{
    puts("TRACE_STATIC: start");
    fflush(stdout);
    m5_exit(0);
    puts("TRACE_STATIC: after_m5_exit");
    fflush(stdout);
    return 0;
}
