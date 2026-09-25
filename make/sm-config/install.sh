module load cc/${CC_MODULE}

# zig's cache stays inside the staging directory (cleaned up with it) rather
# than landing in ~/.cache
export ZIG_GLOBAL_CACHE_DIR=$(pwd)/zig-cache

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${VERSION}.tar.gz"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1

cd downloaded

# GNU make is the one program that cannot assume a make: its tarball ships
# build.sh for exactly this case. Configure as usual (CC/AR/RANLIB come from
# the cc module's environment), compile with the script, then let the fresh
# binary install itself -- which also means the build host has to satisfy the
# cc module's glibc floor, since the binary targets that, not the host.
#
# --without-guile: configure would otherwise pick up a host libguile, if one
#   is around, and link against it -- and the binary would stop being portable
#   across the fleet. --disable-nls keeps a host libintl out for the same
#   reason.
./configure --prefix="${INSTALL_PREFIX}" --without-guile --disable-nls
sh build.sh
./make install

#______________________________________________________________________________
# Smoke test: the installed make runs, and drives the cc module's compiler
# through a real Makefile
#
"${INSTALL_PREFIX}/bin/make" --version | head -n 1

mkdir -p smoke
cat > smoke/hello.c <<'SRC'
#include <stdio.h>
int main(void) { printf("hello from make\n"); return 0; }
SRC
# recipe lines need tabs, hence printf rather than a heredoc
printf 'all: hello\n\t./hello\n\nhello: hello.c\n\t$(CC) -O2 -o $@ $<\n' > smoke/Makefile
# (capture, then grep: a `| grep -q` would close the pipe on the first match
# and turn make's next write into a SIGPIPE failure)
"${INSTALL_PREFIX}/bin/make" -C smoke > smoke/run.log 2>&1 || { cat smoke/run.log; exit 1; }
grep -q '^hello from make$' smoke/run.log
echo "make: smoke test passed"
#------------------------------------------------------------------------------
