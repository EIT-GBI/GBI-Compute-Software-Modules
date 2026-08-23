include config.$(OS)

OBJS= src/clocks.o src/core_profiler.o src/function_meter.o src/stack.o src/lua50_profiler.o


$(PROFILER_OUTPUT): $(OBJS)
	mkdir -p bin 
	export MACOSX_DEPLOYMENT_TARGET="10.5"; $(CC) $(CFLAGS) $(LIB_OPTION) -o $(PROFILER_OUTPUT) $(OBJS)

clean:
	rm -f $(PROFILER_OUTPUT) src/*.o

install:
	install -m 0755 $(PROFILER_OUTPUT) $(LUA_LIB)
	install -m 0755 src/analyzer/summary.lua $(LUA_BIN)