#!/usr/bin/env nu

# Compare the versions each recipe declares in `settings.toml` against what is
# actually installed in the site layout described by `local_settings.toml`, and
# say whether the installer has to be re-run.
#
# For every version in `[install].versions` two artefacts must exist:
#
#   * the modulefile      <modules>/<name>/<version>[-<variant>].lua
#   * the install tree    <[install].destination>, with {SITE_DESTINATION},
#                         {INSTALL_VERSION}, {INSTALL_VERSION_VARIANT} and
#                         {STAGE_DIR} expanded the way simple-modules does
#
# `<name>`, `<variant>`, `<modules>` and {SITE_DESTINATION} come from
# `local_settings.toml` ({SM_ROOT} := the install root), with `variant`
# overridden by `musl` in default mode, exactly as opt/bin/render.sh does.
#
# Statuses:
#   ok        both artefacts present, and no recipe file is newer
#   missing   neither present  => never installed
#   partial   only one present => interrupted or half-cleaned install
#   stale     present, but a recipe file was modified afterwards
#   rolling   present and current, but the version is a moving target
#             (stable/nightly/master/...) so presence proves nothing
#
# Exit code is 0 when nothing needs doing, 1 when any version is
# missing/partial/stale — so it can gate a `make` run.

const ROLLING = ["stable", "nightly", "beta", "master", "main", "latest", "trunk"]
const RECIPE_DIRS = ["sm-config", "sm-config-build"]
const NEEDS_INSTALL = ["missing", "partial", "stale"]

# Expand {BRACED} placeholders. Longest key first, so that {INSTALL_VERSION}
# cannot eat the head of {INSTALL_VERSION_VARIANT}.
def substitute [template: string, map: record] {
    mut out = $template
    let keys = (
        $map | columns | wrap key
        | insert len {|r| $r.key | str length}
        | sort-by len --reverse
        | get key
    )
    for key in $keys {
        let val = ($map | get $key)
        if $val != null {
            $out = ($out | str replace --all ("{" + $key + "}") ($val | into string))
        }
    }
    $out
}

# Newest mtime of any file in a recipe directory.
def recipe-mtime [config_dir: string] {
    let times = (ls $config_dir | where type == file | get modified)
    if ($times | is-empty) { null } else { $times | math max }
}

def path-mtime [target: string] {
    if ($target | path exists) { ls -D $target | get 0.modified } else { null }
}

# One row per (target, mode) recipe directory found under the repo root.
def discover [root: string] {
    glob ($root | path join "*" "sm-config*" "settings.toml")
    | each {|p|
        let config_dir = ($p | path dirname)
        {
            target: ($config_dir | path dirname | path basename)
            recipe: ($config_dir | path basename)
            config_dir: $config_dir
        }
    }
    | where recipe in $RECIPE_DIRS
    | insert mode {|r| if $r.recipe == "sm-config-build" { "build" } else { "default" }}
    | sort-by target mode
}

# Expand one recipe into one row per declared version.
def expand [recipe: record, sm_root: string, libc: string, ignore_mtime: bool] {
    let settings = (open ($recipe.config_dir | path join "settings.toml"))
    let site_file = ($recipe.config_dir | path join "local_settings.toml")
    let site = (if ($site_file | path exists) { open $site_file } else { {} })

    let name = ($site.name? | default $recipe.target)
    let site_dest = (substitute ($site.destination? | default "{SM_ROOT}/{SITE_NAME}") {
        SM_ROOT: $sm_root
        SITE_NAME: $name
    })
    let modules_dir = (substitute ($site.modules? | default "{SM_ROOT}/modules") {
        SM_ROOT: $sm_root
    })

    # render.sh passes --variant=musl for default-mode recipes only, and that
    # CLI value overrides whatever local_settings.toml says
    let variant = if ($recipe.mode == "default") and ($libc == "musl") {
        "musl"
    } else {
        $site.variant?
    }

    let dest_tpl = ($settings.install?.destination? | default "{SITE_DESTINATION}/{INSTALL_VERSION_VARIANT}")
    let staged = ($settings.install?.stage? | default false)
    let versions = ($settings.install?.versions? | default [])
    let mtime = (recipe-mtime $recipe.config_dir)

    $versions | each {|version|
        let tag = if $variant == null { $version } else { $"($version)-($variant)" }
        let stage_dir = if $variant == null {
            $"($name)-($version)"
        } else {
            $"($name)-($variant)-($version)"
        }
        let install_dir = (substitute $dest_tpl {
            SITE_DESTINATION: $site_dest
            SITE_NAME: $name
            SITE_VARIANT: $variant
            INSTALL_VERSION: $version
            INSTALL_VERSION_VARIANT: $tag
            STAGE_DIR: (if $staged { $stage_dir } else { null })
        })
        let module_file = ($modules_dir | path join $name $"($tag).lua")

        let has_module = ($module_file | path exists)
        let has_install = ($install_dir | path exists)
        let module_mtime = (path-mtime $module_file)
        let comparable = ($has_module and ($mtime != null) and ($module_mtime != null))
        let stale = ((not $ignore_mtime) and $comparable and ($mtime > $module_mtime))

        let status = if (not $has_module) and (not $has_install) {
            "missing"
        } else if (not $has_module) or (not $has_install) {
            "partial"
        } else if $stale {
            "stale"
        } else if ($version in $ROLLING) {
            "rolling"
        } else {
            "ok"
        }

        {
            target: $recipe.target
            mode: $recipe.mode
            name: $name
            version: $version
            variant: $variant
            tag: $tag
            status: $status
            module_ok: $has_module
            install_ok: $has_install
            module_file: $module_file
            install_dir: $install_dir
            modules_dir: $modules_dir
            recipe_mtime: $mtime
            module_mtime: $module_mtime
        }
    }
}

# Modulefiles present on disk whose tag no recipe of that name produces --
# i.e. left over from a settings.toml that has since been changed. `recipes`
# must be ALL recipes, so that e.g. zig-bootstrap's tags do not look orphaned
# while checking zig; `names` restricts what is reported. Module names with no
# recipe at all (rust's version-independent `rustup`) are never considered.
def orphans [recipes: list, names: list, sm_root: string, ignore_mtime: bool] {
    let expected = (
        $recipes
        | each {|r|
            let gnu = (expand $r $sm_root "gnu" $ignore_mtime)
            let musl = (expand $r $sm_root "musl" $ignore_mtime)
            [$gnu $musl] | flatten
        }
        | flatten
    )
    if ($expected | is-empty) { return [] }

    $expected | where name in $names | select name modules_dir | uniq | each {|group|
        let dir = ($group.modules_dir | path join $group.name)
        if not ($dir | path exists) { return [] }
        glob ($dir | path join "*.lua") | each {|f|
            {name: $group.name, tag: ($f | path parse | get stem), module_file: $f}
        }
    }
    | flatten
    | where {|found|
        $expected | where {|e| ($e.name == $found.name) and ($e.tag == $found.tag)} | is-empty
    }
}

def main [
    ...targets: string          # recipe directories to check (default: all of them)
    --module-path (-m): string  # install root (SM_ROOT); default: $__MODULE_PATH__ or <repo>/usr
    --prefix: string            # repo root; default: $__PREFIX__ or derived from this script
    --mode: string = "default"  # default | build | all
    --variant: string = "gnu"   # gnu | musl -- applies to default-mode recipes
    --ignore-mtime              # do not report installs older than their recipe as stale
    --json                      # emit a JSON report instead of tables
    --quiet (-q)                # print nothing, only set the exit code
] {
    if $mode not-in ["default", "build", "all"] {
        error make {msg: $"--mode must be one of default|build|all, got '($mode)'"}
    }
    if $variant not-in ["gnu", "musl"] {
        error make {msg: $"--variant must be one of gnu|musl, got '($variant)'"}
    }
    if ($mode == "build") and ($variant == "musl") {
        error make {msg: "--variant=musl cannot be combined with --mode=build"}
    }

    let root = if $prefix != null {
        $prefix
    } else if ("__PREFIX__" in $env) {
        $env.__PREFIX__
    } else {
        $env.CURRENT_FILE | path dirname | path dirname | path dirname
    }
    let sm_root = if $module_path != null {
        $module_path
    } else if ("__MODULE_PATH__" in $env) {
        $env.__MODULE_PATH__
    } else {
        $root | path join "usr"
    }

    let all_recipes = (discover $root)
    if ($all_recipes | is-empty) {
        error make {msg: $"no sm-config recipes found under ($root)"}
    }

    if ($targets | is-not-empty) {
        let known = ($all_recipes | get target | uniq)
        let unknown = ($targets | where {|t| $t not-in $known})
        if ($unknown | is-not-empty) {
            error make {msg: $"unknown target\(s): ($unknown | str join ', ') -- known: ($known | str join ', ')"}
        }
    }

    let recipes = ($all_recipes | where {|r|
        let mode_ok = (($mode == "all") or ($r.mode == $mode))
        let target_ok = (($targets | is-empty) or ($r.target in $targets))
        $mode_ok and $target_ok
    })

    let rows = ($recipes | each {|r| expand $r $sm_root $variant $ignore_mtime} | flatten)
    let leftovers = (orphans $all_recipes ($rows | get name | uniq) $sm_root $ignore_mtime)
    let needs = ($rows | where status in $NEEDS_INSTALL)

    let commands = ($needs | each {|r|
        let mode_arg = if $r.mode == "build" { " MODE=build" } else { "" }
        let variant_arg = if ($r.mode == "default") and ($variant == "musl") { " VARIANT=musl" } else { "" }
        $"make ($r.target)($mode_arg)($variant_arg) MODULE_PATH=($sm_root)"
    } | uniq)

    if $json {
        if not $quiet {
            print (
                {
                    root: $root
                    module_path: $sm_root
                    mode: $mode
                    variant: $variant
                    needs_install: ($needs | is-not-empty)
                    versions: $rows
                    orphans: $leftovers
                    commands: $commands
                } | to json
            )
        }
    } else if not $quiet {
        print $"repo root:    ($root)"
        print $"install root: ($sm_root)"
        print $"mode:         ($mode)   variant: ($variant)"
        print ""
        print ($rows | select target mode name version tag status module_ok install_ok)

        if ($leftovers | is-not-empty) {
            print "installed modulefiles no recipe declares any more:"
            print ($leftovers | select name tag module_file)
        }

        if ($needs | is-empty) {
            print $"nothing to do: all ($rows | length) declared version\(s) are installed and current"
        } else {
            print $"the installer has to be re-run for ($needs | length) of ($rows | length) declared version\(s):"
            for row in $needs {
                print $"  ($row.status)  ($row.name)/($row.tag)  \(($row.target), mode=($row.mode))"
            }
            print ""
            for cmd in $commands { print $"  ($cmd)" }
        }
    }

    if ($needs | is-not-empty) { exit 1 }
}
