#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#ifdef LUA_INCLUDE_STRING_H
#include <string.h>
#endif

#define MYNAME   "sys"
#define MYVERSION	MYNAME " library for " LUA_VERSION " / Jan 2007"
#define DIRSIZE 255

static int dir_iter(lua_State* L);

static int unx_dir(lua_State* L)
{
  const char* path = luaL_checkstring(L, 1);

  /* Create a user datum to store a DIR address */
  DIR** d = (DIR** )lua_newuserdata(L, sizeof(DIR*));

  /* Set its metatable */
  luaL_getmetatable(L, "sys.dir");
  lua_setmetatable(L, -2);

  /* try to open the given directory */
  *d = opendir(path);
  if (*d == NULL)
    /* error openning directory */
    luaL_error(L, "Can not open %s: %s", path, strerror(errno));

  /* create and return the iterator function
     (its sole upvalue, the directory userdatum,
     is already on the top of the stack ) */
  lua_pushcclosure(L, dir_iter, 1);
  return 1;
}

static int dir_iter(lua_State* L)
{
  DIR* d = *(DIR **)lua_touserdata(L, lua_upvalueindex(1));
  struct dirent* entry;
  if ((entry = readdir(d)) != NULL)
    {
      lua_pushstring(L, entry->d_name);
      return 1;
    }
  else
    return 0;
}
  
static int unx_dir_gc(lua_State* L)
{
  DIR* d = *(DIR**) lua_touserdata(L, 1);
  if (d) closedir(d);
  return 0;
}

static int unx_chdir(lua_State* L)
{
  const char* fn   = luaL_checkstring(L, 1);
  int         iret = chdir(fn);
  lua_pushboolean(L, iret);
  return 1;
}

static int unx_isdir(lua_State* L)
{
  const char* path = luaL_checkstring(L, 1);
  struct stat statbuf;
  int         iret;

  stat(path, &statbuf);
  iret = (S_ISDIR(statbuf.st_mode) == 0) ?  0 : 1;
  lua_pushboolean(L, iret);
  return 1;
}

static int unx_isfile(lua_State* L)
{
  const char* path = luaL_checkstring(L, 1);
  struct stat statbuf;
  int         iret;

  stat(path, &statbuf);
  iret = S_ISREG(statbuf.st_mode);
  lua_pushboolean(L, iret);
  return 1;
}


static int unx_cwd(lua_State* L)
{
  int   sz   = DIRSIZE;
  char* ptr;

  while (1)
    {
      ptr = (char *) malloc(sz);
      if (ptr == 0)
	luaL_error(L,"insufficent memory");
      if (getcwd(ptr,sz-1) != 0) break;
      if (errno != ERANGE)
	luaL_error(L,"unable to get current directory, errno: %s", strerror(errno));
      free(ptr);
      sz = 2*sz;
    }

  lua_pushstring(L, ptr);
  free(ptr);
  return 1;
}

static int unx_gettimeofday(lua_State* L)
{
  double t1;
  struct timezone z;
  struct timeval  tv;
  gettimeofday(&tv, &z);
  t1 = tv.tv_sec + tv.tv_usec * 1.0e-6;
  lua_pushnumber(L, t1);
  return 1;
}

static const luaL_reg syslib[] = {
  { "cwd",    	    unx_cwd},
  { "dir",    	    unx_dir},
  { "chdir",  	    unx_chdir},
  { "isdir",  	    unx_isdir},
  { "isfile", 	    unx_isfile},
  { "gettimeofday", unx_gettimeofday},
  { NULL, NULL}
};

LUALIB_API int luaopen_sys (lua_State *L)
{
	luaL_openlib(L, MYNAME, syslib, 0);
	lua_pushliteral(L,"version");		/** version */
	lua_pushliteral(L,MYVERSION);
	lua_settable(L,-3);
	return 1;
}
