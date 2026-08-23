LIBNAME = lpeg
LUADIR = /usr/include/lua5.1/

COPT = -O2 -DNDEBUG

CWARNS = -Wall -Wextra -pedantic \
        -Waggregate-return \
	-Wbad-function-cast \
        -Wcast-align \
        -Wcast-qual \
	-Wdeclaration-after-statement \
	-Wdisabled-optimization \
        -Wmissing-prototypes \
        -Wnested-externs \
        -Wpointer-arith \
        -Wshadow \
	-Wsign-compare \
	-Wstrict-prototypes \
	-Wundef \
        -Wwrite-strings \
	#  -Wunreachable-code \


CC = gcc
OS := $(shell uname -s)
ifeq ($(OS),Darwin)
  CEXTRA   := 
  ENV      := env MACOSX_DEPLOYMENT_TARGET=10.4
  DLLFLAGS := -bundle -undefined dynamic_lookup
else
  CEXTRA   := -fpic
  ENV      := 
  DLLFLAGS := -shared -fpic
endif

CFLAGS := $(CEXTRA) $(CWARNS) $(COPT) -ansi -I$(LUADIR)

lpeg.so: lpeg.o
	$(ENV) $(CC) $(DLLFLAGS) lpeg.o -o lpeg.so

lpeg.o:		makefile lpeg.c lpeg.h

test: test.lua re.lua lpeg.so
	test.lua

clean:
	$(RM) lpeg.so lpeg.o
