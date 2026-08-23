# Local LMod Deployment

Had to make the following changes to LMod's Lua install:
```diff
diff --git a/opt/lmod/lua-5.1.4.9/loadsys/Makefile b/opt/lmod/lua-5.1.4.9/loadsys/Makefile
index 840e830..b8a9337 100644
--- a/opt/lmod/lua-5.1.4.9/loadsys/Makefile
+++ b/opt/lmod/lua-5.1.4.9/loadsys/Makefile
@@ -6,9 +6,10 @@ OBJ      := $(patsubst %.c, %.o, $(SRC))
 CFLAGS   := -fPIC -I $(LUAINC)

 ifeq ($(OS),macosx)
-  LIB_OPTION= -bundle -undefined dynamic_lookup #for MacOS X
+  LIB_OPTION = -bundle -undefined dynamic_lookup #for MacOS X
+  CFLAGS += -DLUA_INCLUDE_STRING_H
 else
-  LIB_OPTION= -shared -Wl,-soname,$(SONAMEV) #for Linux
+  LIB_OPTION = -shared -Wl,-soname,$(SONAMEV) #for Linux
 endif

 all:  $(LIBRARY) $(SONAMEV) $(SONAME)
diff --git a/opt/lmod/lua-5.1.4.9/loadsys/unx_sys.c b/opt/lmod/lua-5.1.4.9/loadsys/unx_sys.c
index f9a44cc..95ee8bb 100644
--- a/opt/lmod/lua-5.1.4.9/loadsys/unx_sys.c
+++ b/opt/lmod/lua-5.1.4.9/loadsys/unx_sys.c
@@ -8,6 +8,9 @@
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <sys/time.h>
+#ifdef LUA_INCLUDE_STRING_H
+#include <string.h>
+#endif

 #define MYNAME   "sys"
 #define MYVERSION      MYNAME " library for " LUA_VERSION " / Jan 2007"
```

in order to include `string.h` on mac builds.
