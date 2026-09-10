# We need to get the absolute path of the makefile (in order to find
# entrypoint.sh) => this is the equivalent of:
# ```
# $(dirname "$(realpath "$source")")
# ```
# in GNUMake
MKFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

GBI_MODULE_PATH ?= $(MKFILE_DIR)/usr
# An empty GBI_MODULE_PATH (e.g. an unset variable in a wrapper script) would
# turn the clean/realclean deletions into absolute paths like '/modules'
ifeq ($(strip $(GBI_MODULE_PATH)),)
    $(error GBI_MODULE_PATH must not be empty)
endif
EP_ARG          := -m $(GBI_MODULE_PATH)

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
HELP_CMD      = opt/bin/help.sh

# `check` is a nushell script -- override NU to point at an interpreter that is
# not on PATH (e.g. NU=$(GBI_MODULE_PATH)/nu/<version>/nu)
NU ?= nu
CHECK_ARGS := -m $(GBI_MODULE_PATH) --variant $(VARIANT)
ifeq ($(MODE),build)
CHECK_ARGS += --mode build
else
CHECK_ARGS += --mode default
endif

#______________________________________________________________________________
# Target discovery
# A make target is any directory in the project root that contains an
# sm-config/ (default mode) and/or sm-config-build/ (MODE=build) recipe.
# Nothing is registered here by hand: drop a new recipe directory into the repo
# and it becomes a target, is walked by `all`, and shows up in `make help`.
# Per-recipe metadata (all optional except settings.toml):
#
#   <name>/sm-config/settings.toml        -> `make <name>` works
#   <name>/sm-config-build/settings.toml  -> `make <name> MODE=build` works
#   <name>/sm-opt-in                      -> `make all` skips it (opt-in)
#   <name>/sm-help                        -> description shown by `make help`
#                                            (line 1: summary; rest: notes)
#
DEFAULT_TARGETS := $(sort $(notdir $(patsubst %/sm-config/settings.toml,%,\
                       $(wildcard $(MKFILE_DIR)*/sm-config/settings.toml))))
BUILD_TARGETS   := $(sort $(notdir $(patsubst %/sm-config-build/settings.toml,%,\
                       $(wildcard $(MKFILE_DIR)*/sm-config-build/settings.toml))))
ALL_RECIPES     := $(sort $(DEFAULT_TARGETS) $(BUILD_TARGETS))

# Heavyweight / opt-in targets (marked by an sm-opt-in file in the recipe):
# valid for `clean` and `make <target>`, but deliberately not walked by `all`
AUX_TARGETS := $(filter $(ALL_RECIPES),$(sort $(notdir $(patsubst %/sm-opt-in,%,\
                   $(wildcard $(MKFILE_DIR)*/sm-opt-in)))))
TARGETS     := $(filter-out $(AUX_TARGETS),$(ALL_RECIPES))

# Build-mode module dependencies of a target, read from the `module load`
# lines of its install script -- the recipe itself is the source of truth, so
# this cannot drift. Version suffixes (e.g. zig/0.16.0) are stripped.
sm_deps = $(sort $(foreach w,\
              $(shell sed -n 's,^[[:space:]]*module load[[:space:]]*,,p' \
                  $(MKFILE_DIR)$(1)/sm-config-build/install.sh 2>/dev/null),\
              $(firstword $(subst /, ,$(w)))))

# Build steps
.PHONY: all install update bootstrap check clean realclean help $(ALL_RECIPES)

# Guard against incorrect targets
ifneq ($(filter $(TARGET),$(ALL_RECIPES)),$(TARGET))
    $(error TARGET must be one of: '$(ALL_RECIPES)')
endif

#______________________________________________________________________________
# Help is rendered by a helper script (same pattern as build/render): it walks
# the discovered recipes and prints each target's summary (from sm-help), its
# versions (from settings.toml) and its build deps (from install.sh)
#
help:
	@MODE="$(MODE)" VARIANT="$(VARIANT)" VARIANTS="$(VARIANTS)"    \
	    GBI_MODULE_PATH="$(GBI_MODULE_PATH)" TARGETS="$(TARGETS)"          \
	    AUX_TARGETS="$(AUX_TARGETS)" $(RUN_CMD) $(HELP_CMD)
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Rule to build all targets, note: since we define the install rules the way we
# do, we can't use use `all: $(TARGETS)` and instead need to call `make`
# recursively. Note: environment variables are inherited (apparently)
#
ifeq ($(MODE),build)
# `all` first installs the toolchain targets below in default mode -- both can
# be built from source, but only against the opt-in llvm module, and they are
# needed by many builds -- and then builds every non-opt-in target whose
# `module load` dependencies those toolchains cover. Anything with other deps
# (e.g. on the opt-in cmake) is skipped and reported.
ALL_PROVIDES := $(filter rust zig,$(DEFAULT_TARGETS))
NTARGETS := $(strip $(foreach t,\
                $(filter-out $(ALL_PROVIDES),$(filter $(BUILD_TARGETS),$(TARGETS))),\
                $(if $(filter-out $(ALL_PROVIDES),$(call sm_deps,$(t))),,$(t))))
NSKIPPED := $(filter-out $(ALL_PROVIDES) $(NTARGETS),$(TARGETS))
all:
	$(warning Building all targets using MODE=build! Installing '$(ALL_PROVIDES)' in default mode first, then building: '$(NTARGETS)'. Omitting targets whose build dependencies 'all' does not provide: '$(NSKIPPED)')
	@for target in $(ALL_PROVIDES); do \
		$(MAKE) $$target MODE= ;      \
	done
	@for target in $(NTARGETS); do \
		$(MAKE) $$target;          \
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
# Rules to clean installed targets. Deletions are scoped to what the recipes
# actually install under GBI_MODULE_PATH -- the per-recipe dirs and the
# modules/ tree -- never GBI_MODULE_PATH itself: on shared deployments the
# prefix holds more than the install (it can even contain this checkout).
#
realclean:
	$(info Running realclean => deleting all installed modules and modulefiles)
	rm -rf $(addprefix $(GBI_MODULE_PATH)/,$(ALL_RECIPES) modules)
	rm -rf $(MKFILE_DIR)/opt/share/*
	rm -rf $(MKFILE_DIR)/opt/bin/lua
	rm -rf $(MKFILE_DIR)/opt/bin/lua-static
	rm -rf $(MKFILE_DIR)/opt/bin/luac

clean:
ifeq ($(strip $(TARGET)),)
	$(error clean needs a target: make clean TARGET=<recipe> -- use realclean to delete every installed module)
endif
	$(info Running clean on target: '$(TARGET)')
	rm -rf $(GBI_MODULE_PATH)/$(TARGET)
	rm -rf $(GBI_MODULE_PATH)/modules/$(TARGET)
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
	$(RUN_CMD) opt/lmod/bootstrap.sh v1.5.0 $(GBI_MODULE_PATH)/modules $(ML_INIT_FILE)
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Main rule to install targets -- this rule is invoked by the generated
# per-module rules below
#
ifeq ($(MODE),build)
# BUILD_DEPS is informational: the `module load` calls in the recipe's own
# install.sh (where these names are read from) are what actually enforces them
BUILD_DEPS = $(call sm_deps,$(TARGET))
install:
	$(info Running build-mode install for '$(TARGET)' with dependencies: '$(BUILD_DEPS)')
	bash -c "$(ML_INIT); $(RUN_CMD) $(BUILD_CMD) -b $(EP_ARG) $(RENDER_CMD) $(TARGET)"
else
install:
	$(info Running default-mode install for '$(TARGET)')
	bash -c "$(ML_INIT); $(RUN_CMD) $(BUILD_CMD) $(EP_ARG) $(RENDER_CMD) $(TARGET)"
endif
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Per-module rules, generated for every discovered recipe. A target whose
# recipe does not exist in the current MODE gets an error stub instead: e.g.
# zig-bootstrap is a source-only build, so without MODE=build it errors out.
#
ifeq ($(MODE),build)
RUNNABLE      := $(BUILD_TARGETS)
MISSING_DIR   := sm-config-build
NO_RECIPE_MSG := does not support MODE=build
else
RUNNABLE      := $(DEFAULT_TARGETS)
MISSING_DIR   := sm-config
NO_RECIPE_MSG := is a source build, use MODE=build
endif

define RECIPE_template
$(1): TARGET=$(1)
$(1): install
endef

define NO_RECIPE_template
$(1):
	$$(error '$(1)' $(NO_RECIPE_MSG) ['$(1)/$(MISSING_DIR)/' does not exist])
endef

$(foreach t,$(RUNNABLE),$(eval $(call RECIPE_template,$(t))))
$(foreach t,$(filter-out $(RUNNABLE),$(ALL_RECIPES)),$(eval $(call NO_RECIPE_template,$(t))))
#------------------------------------------------------------------------------
