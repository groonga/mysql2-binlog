#!/usr/bin/env ruby

require "mkmf"

require "pkg-config"
require "native-package-installer"

def gcc?
  CONFIG["GCC"] == "yes"
end

def disable_optimization_build_flag(flags)
  if gcc?
    RbConfig.expand(flags.dup).gsub(/(^|\s)?-O\d(\s|$)?/, '\\1-O0\\2')
  else
    flags
  end
end

def enable_debug_build_flag(flags)
  if gcc?
    expanded_flags = RbConfig.expand(flags.dup)
    debug_option_pattern = /(^|\s)-g(?:gdb)?\d?(\s|$)/
    if debug_option_pattern =~ expanded_flags
      expanded_flags.gsub(debug_option_pattern, '\\1-ggdb3\\2')
    else
      flags + " -ggdb3"
    end
  else
    flags
  end
end

checking_for(checking_message("--enable-debug-build option")) do
  enable_debug_build = enable_config("debug-build", false)
  if enable_debug_build
    $CFLAGS = disable_optimization_build_flag($CFLAGS)
    $CFLAGS = enable_debug_build_flag($CFLAGS)

    $CXXFLAGS = disable_optimization_build_flag($CXXFLAGS)
    $CXXFLAGS = enable_debug_build_flag($CXXFLAGS)
  end
  enable_debug_build
end

spec = Gem::Specification.find_by_name("mysql2")
source_dir = File.join(spec.full_gem_path, "ext", "mysql2")
$INCFLAGS += " -I#{source_dir}"

unless PKGConfig.have_package("libmariadb")
  unless NativePackageInstaller.install(debian: "libmariadb-dev",
                                        homebrew: "mariadb-connector-c",
                                        msys2: "libmariadbclient",
                                        redhat: "mariadb-connector-c-devel")
    exit(false)
  end
  unless PKGConfig.have_package("libmariadb")
    exit(false)
  end
end

create_makefile("mysql2_replication")
