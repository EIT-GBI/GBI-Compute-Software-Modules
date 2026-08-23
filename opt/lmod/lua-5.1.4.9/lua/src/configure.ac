dnl Process this file with autoconf to produce a configure script.
dnl This is for lua
AC_INIT(Makefile.in)
AC_CONFIG_HEADER(config.h:config.h.in)
AC_PROG_CC
AC_CHECK_HEADERS(readline/readline.h)
AC_CHECK_LIB(ncurses, cbreak, , , )
AC_CHECK_LIB(tinfo, tputs, , , -lncurses)
AC_CHECK_LIB(readline,rl_insert, , ,-lncurses)
AC_CHECK_LIB(history,history_get, , ,-lncurses)

AC_OUTPUT(Makefile:Makefile.in, [date > stamp-h])
rm -f conftest*

