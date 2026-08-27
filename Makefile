# We need to get the absolute path of the makefile (in order to find
# entrypoint.sh) => this is the equivalent of:
# ```
# $(dirname "$(realpath "$source")")
# ```
# in GNUMake
MKFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

MODULE_PATH ?= $(MKFILE_DIR)/usr
EP_ARG      := -m $(MODULE_PATH)

VARIANT ?= gnu

VARIANTS := gnu musl
# Check for allowed variants
ifneq ($(filter $(VARIANT),$(VARIANTS)),$(VARIANT))
    $(error VARIANT must be one of: '$(VARIANTS)')
endif

ifeq ($(VARIANT),musl)
	EP_ARG += -g
endif

ML_INIT_FILE ?= $(MKFILE_DIR)/opt/lmod/lmod/init
ML_INIT      ?= source $(MKFILE_DIR)/opt/share/env.sh
RUN_CMD       = $(MKFILE_DIR)/run.sh
BUILD_CMD     = opt/bin/build.sh
RENDER_CMD    = opt/bin/render.sh
CHECK_CMD     = opt/bin/check_versions.nu

# `check` is a nushell script -- override NU to point at an interpreter that is
# not on PATH (e.g. NU=$(MODULE_PATH)/nu/<version>/nu)
NU ?= nu
CHECK_ARGS := -m $(MODULE_PATH) --variant $(VARIANT)
ifeq ($(MODE),build)
CHECK_ARGS += --mode build
else
CHECK_ARGS += --mode default
endif

TARGETS := rust eza bat nu fish neovim uv zig ncdu parallel-tar
# Heavyweight / opt-in targets: valid for `clean` and `make <target>`, but
# deliberately not walked by `all`
AUX_TARGETS := cmake llvm zig-bootstrap

# Build steps
.PHONY: all install update bootstrap check clean realclean $(TARGETS) $(AUX_TARGETS)

# Guard against incorrect targets
ifneq ($(filter $(TARGET),$(TARGETS) $(AUX_TARGETS)),$(TARGET))
    $(error TARGET must be one of: '$(TARGETS) $(AUX_TARGETS)')
endif

NULL :=
help:
	$(info  ----------------- install local modules ---------------------    )
	$(info Sometimes Spack is just too much of a headache -- also how do you )
	$(info use spack without a local python? -- anyway, this is a collection )
	$(info of bash a lua scripts to generate a bare-bones set of LMod        )
	$(info modules                                                           )
	$(info                                                                   )
	$(info Environment variables:                                            )
	$(info ├── VARIANT [must be one of: '$(VARIANTS)']                       )
	$(info │      └── Specify which glibc variant to use                     )
	$(info ├── MODULE_PATH [can be any valid path]                           )
	$(info │      └── Specify where to install local modules to              )
	$(info ├── ML_INIT_FILE [default: $(MKFILE_DIR)/opt/lmod/lmod/init/bash] )
	$(info │      └── Path of the LMod init file, use ML_INIT= to stop lmod initialization )
	$(info └── MODE [default '']                                             )
	$(info $(NULL)       ├── If MODE=build, this will build the module from source )
	$(info $(NULL)       └── WARNING: VARIANT=musl is not permitted with MODE=build )
	$(info                                                                   )
	$(info Available make targets that generate modules:                     )
	$(info ├── rust [the Rust compiler]                                      )
	$(info │    ├── default mode installs a rustup-managed toolchain         )
	$(info │    └── MODE=build requires 'llvm MODE=build' + a python3        )
	$(info ├── eza [ls but better]                                           )
	$(info │    └── MODE=build requires 'rust'                               )
	$(info ├── bat [cat but better]                                          )
	$(info │    └── MODE=build requires 'rust'                               )
	$(info ├── nu [a completely new way to think of a shell]                 )
	$(info │    └── MODE=build requires 'rust'                               )
	$(info ├── fish [the friendly interactive shell]                         )
	$(info │    └── MODE=build reuqires 'rust' and 'cmake'                   )
	$(info ├── neovim [the neovim editor]                                    )
	$(info │    └── MODE=build has no dependencies                           )
	$(info ├── uv [a better way to manage python]                            )
	$(info │    └── If MODE=build requires 'rust'                            )
	$(info ├── zig [the zig language + a drop-in C/C++ cross compiler]       )
	$(info │    └── MODE=build requires 'cmake' + 'llvm MODE=build' [~2h]    )
	$(info ├── ncdu [du, but with a text-mode user interface]                )
	$(info │    ├── binary releases are linux-only, use MODE=build elsewhere )
	$(info │    └── MODE=build requires 'zig'                                )
	$(info └── parallel-tar [multi-threaded archival tools]                  )
	$(info $(NULL)     └── MODE=build requires 'rust'                        )
	$(info                                                                   )
	$(info Opt-in targets [NOT built by 'all', ask for them by name]:        )
	$(info ├── cmake [the cmake build system]                                )
	$(info │    └── MODE=build has no dependencies                           )
	$(info ├── llvm [clang/lld + the LLVM development libraries]             )
	$(info │    ├── default mode downloads a >1GB upstream release           )
	$(info │    │    └── NOTE: an LTO build, so it CANNOT compile zig         )
	$(info │    └── MODE=build requires 'cmake' [~15min]                     )
	$(info │         └── needed by 'zig MODE=build' and 'rust MODE=build'    )
	$(info └── zig-bootstrap [zig via bootstrap.c, installs as zig/<ver>-bootstrap] )
	$(info $(NULL)     ├── source build only, needs nothing but a C compiler [~36min] )
	$(info $(NULL)     └── for porting, provenance and compiler hacking -- stops at )
	$(info $(NULL)          zig's intermediate 'zig2', so not a usable toolchain )
	$(info                                                                   )
	$(info Auxilliary make targets:                                          )
	$(info ├── help [print this help prompt]                                 )
	$(info ├── check [does the installer need re-running?]                    )
	$(info │    ├── compares settings.toml versions against MODULE_PATH       )
	$(info │    ├── honours MODE and VARIANT; set TARGET to check one module  )
	$(info │    └── exits non-zero if anything is missing/partial/stale       )
	$(info ├── realclean [deletes ALL installed modules]                     )
	$(info │    └── must set MODULE_PATH to the location to be cleaned       )
	$(info ├── clean [clean module specified by TARGET]                      )
	$(info │    ├── must set MODULE_PATH to the location of the target module)
	$(info │    └── must set TARGET to the name of the module to be cleaned  )
	$(info ├── bootstrap [bootstraps a Lua and LMod install to /opt/lmod]    )
	$(info └── update [updates this project's dependencies]                  )


#______________________________________________________________________________
# Rule to build all targets, note: since we define the install rules the way we
# do, we can't use use `all: $(TARGETS)` and instead need to call `make`
# recursively. Note: environment variables are inherited (apparently)
#
ifeq ($(MODE),build)
all: NTARGETS := eza bat nu neovim ncdu parallel-tar
all:
	$(warn Building all targets using MODE=build! Omitting all targes that don't allow for build. Installing rust and zig in default mode -- both can be built from source, but only against the opt-in llvm module, and they are needed by many builds.)
	$(MAKE) rust MODE=
	$(MAKE) zig MODE=
	@for target in $(NTARGETS); do \
		$(MAKE) $$target;         \
	done
else
all:
	@for target in $(TARGETS); do \
		$(MAKE) $$target;         \
	done
endif
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Check whether the installed tree still matches the recipes -- i.e. whether
# the installer has to be re-run. Exits non-zero if it does, so this can gate
# a build.
#
check:
	@command -v $(NU) >/dev/null 2>&1 || {                                     \
	    echo "check needs nushell: install it ('make nu') and 'module load nu',"; \
	    echo "or point NU at an interpreter, e.g. make check NU=/path/to/nu";  \
	    exit 1;                                                               \
	}
	@$(NU) $(MKFILE_DIR)/$(CHECK_CMD) --prefix $(MKFILE_DIR) $(CHECK_ARGS) $(TARGET)
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Rules to clean installed targets
#
realclean:
	$(info Running realclean => deleting entire install)
	rm -rf $(MODULE_PATH)
	rm -rf $(MKFILE_DIR)/opt/share/*
	rm -rf $(MKFILE_DIR)/opt/bin/lua
	rm -rf $(MKFILE_DIR)/opt/bin/lua-static
	rm -rf $(MKFILE_DIR)/opt/bin/luac

clean:
	$(info Running clean on target: '$(TARGET)')
	rm -rf $(MODULE_PATH)/$(TARGET)
	rm -rf $(MODULE_PATH)/modules/$(TARGET)
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Update all sources
#
update:
	$(MKFILE_DIR)/opt/update_bin.sh
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Bootstrap LMOD dependencies and install
#
bootstrap:
	$(RUN_CMD) opt/lmod/bootstrap.sh v1.5.0 $(MODULE_PATH)/modules $(ML_INIT_FILE)
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Main rule to install targets -- this rule is invoked by the install rules
# below
#
ifeq ($(MODE),build)
install: $(BUILD_DEPS)
	$(info Running build-mode install for '$(TARGET)' with dependencies: '$(BUILD_DEPS)')
	bash -c "$(ML_INIT); $(RUN_CMD) $(BUILD_CMD) -b $(EP_ARG) $(RENDER_CMD) $(TARGET)"
else
install:
	$(info Running default-mode install for '$(TARGET)' with dependencies: '$(BUILD_DEPS)')
	bash -c "$(ML_INIT); $(RUN_CMD) $(BUILD_CMD) $(EP_ARG) $(RENDER_CMD) $(TARGET)"
endif
#------------------------------------------------------------------------------


#______________________________________________________________________________
# CMAKE Module
#
ifeq ($(MODE),build)
cmake: BUILD_DEPS=
endif
cmake: TARGET=cmake
cmake: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# RUST Module
#
# Default mode installs a rustup-managed toolchain. MODE=build compiles the
# official source tarball against the `llvm` module -- which, as for zig, has
# to be the source-built one (`make llvm MODE=build`).
#
ifeq ($(MODE),build)
rust: BUILD_DEPS=$(MODULE_PATH)/modules/llvm
endif
rust: TARGET=rust
rust: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# EZA Module
#
ifeq ($(MODE),build)
eza: BUILD_DEPS=$(MODULE_PATH)/modules/rust
endif
eza: TARGET=eza
eza: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# BAT Module
#
ifeq ($(MODE),build)
bat: BUILD_DEPS=$(MODULE_PATH)/modules/rust
endif
bat: TARGET=bat
bat: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# NU Module
#
ifeq ($(MODE),build)
nu: BUILD_DEPS=$(MODULE_PATH)/modules/rust
endif
nu: TARGET=nu
nu: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# FISH Module
#
ifeq ($(MODE),build)
fish: BUILD_DEPS="$(MODULE_PATH)/modules/rust $(MODULE_PATH)/modules/cmake"
endif
fish: TARGET=fish
fish: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# NEOVIM Module
#
ifeq ($(MODE),build)
neovim: BUILD_DEPS=$(MODULE_PATH)/modules/cmake
endif
neovim: TARGET=neovim
neovim: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# UV Module
#
ifeq ($(MODE),build)
uv: BUILD_DEPS=$(MODULE_PATH)/modules/rust
endif
uv: TARGET=uv
uv: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# ZIG Module
#
# MODE=build is the full CMake build against the `llvm` module. For a build
# with no dependencies at all (but a reduced compiler) see `zig-bootstrap`.
#
ifeq ($(MODE),build)
zig: BUILD_DEPS="$(MODULE_PATH)/modules/cmake $(MODULE_PATH)/modules/llvm"
endif
zig: TARGET=zig
zig: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# NCDU Module
#
ifeq ($(MODE),build)
ncdu: BUILD_DEPS=$(MODULE_PATH)/modules/zig
endif
ncdu: TARGET=ncdu
ncdu: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# PARALLEL-TAR Module
#
# Not on crates.io, so MODE=build installs straight from the GitHub repo.
#
ifeq ($(MODE),build)
parallel-tar: BUILD_DEPS=$(MODULE_PATH)/modules/rust
endif
parallel-tar: TARGET=parallel-tar
parallel-tar: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# LLVM Module -- needed to build zig from source
#
ifeq ($(MODE),build)
llvm: BUILD_DEPS=$(MODULE_PATH)/modules/cmake
endif
llvm: TARGET=llvm
llvm: install
#------------------------------------------------------------------------------

#______________________________________________________________________________
# ZIG-BOOTSTRAP Module -- installs as `zig/<version>-bootstrap`
#
# This is zig's own bootstrap.c path: the only dependency is a C compiler, but
# the resulting compiler is missing LLVM-backed features. Source build only.
#
ifeq ($(MODE),build)
zig-bootstrap: BUILD_DEPS=
zig-bootstrap: TARGET=zig-bootstrap
zig-bootstrap: install
else
zig-bootstrap:
	$(error zig-bootstrap is a source build, use MODE=build)
endif
#------------------------------------------------------------------------------
