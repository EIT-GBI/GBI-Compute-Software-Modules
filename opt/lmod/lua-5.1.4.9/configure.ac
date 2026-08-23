dnl ######################################################################
dnl $Id: configure.ac,v 1.3 2004-01-21 22:13:29 mclay Exp $
dnl ######################################################################
dnl Process this file with autoconf to produce a configure script.
dnl This is for MGF

AC_INIT(Makefile.in)
AC_PREFIX_DEFAULT(/usr/local)
AC_CONFIG_SUBDIRS(lua/src)
AC_SUBST(STATIC)
AC_ARG_WITH(static,
    AC_HELP_STRING([--with-static=ans],[link system libraries statically [[NO]]]),
    STATIC="$withval"
    AC_MSG_RESULT([STATIC=$with_static])
    AC_DEFINE_UNQUOTED(STATIC, "$with_static")dnl
    ,
    withval="NO"
    AC_MSG_RESULT([STATIC=$withval])
    STATIC="$withval"
    AC_DEFINE_UNQUOTED(STATIC, "$withval"))dnl
AC_PROG_CC
AC_OUTPUT(makefile:Makefile.in)

