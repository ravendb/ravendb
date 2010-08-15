# ****************************************************************************
#
# Copyright (c) Microsoft Corporation. 
#
# This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
# copy of the license can be found in the License.html file at the root of this distribution. If 
# you cannot locate the  Apache License, Version 2.0, please send an email to 
# ironruby@microsoft.com. By using this source code in any fashion, you are agreeing to be bound 
# by the terms of the Apache License, Version 2.0.
#
# You must not remove this notice, or any other, from this software.
#
#
# ****************************************************************************


module Config
  CONFIG = {}
  version_components = RUBY_VERSION.split('.')
  abort("Could not parse RUBY_VERSION") unless version_components.size == 3
  CONFIG["MAJOR"], CONFIG["MINOR"], CONFIG["TEENY"] = version_components
  CONFIG["PATCHLEVEL"] = "0"
  CONFIG["EXEEXT"] = ".exe"
  # This value is used by libraries to spawn new processes to run Ruby scripts. Hence it needs to match the ir.exe name
  CONFIG["ruby_install_name"] = "ir"
  CONFIG["RUBY_INSTALL_NAME"] = "ir"
  CONFIG["RUBY_SO_NAME"] = "msvcrt-ruby18"
  CONFIG["SHELL"] = "$(COMSPEC)"
  CONFIG["BUILD_FILE_SEPARATOR"] = "\\"
  CONFIG["PATH_SEPARATOR"] = ";"
  CONFIG["CFLAGS"] = "-MD -Zi -O2b2xg- -G6"
  CONFIG["CPPFLAGS"] = ""
  CONFIG["CXXFLAGS"] = ""
  CONFIG["FFLAGS"] = ""
  CONFIG["LDFLAGS"] = ""
  CONFIG["LIBS"] = "oldnames.lib user32.lib advapi32.lib ws2_32.lib "
  
  # Set up paths
  if ENV["DLR_ROOT"] then
    # This is a dev environment. See http://wiki.github.com/ironruby/ironruby
    TOPDIR = File.expand_path(ENV["DLR_BIN"] || System::IO::Path.get_directory_name(
      System::Reflection::Assembly.get_executing_assembly.code_base
    ).gsub('file:\\', ''))
    CONFIG["bindir"] = TOPDIR
    CONFIG["libdir"] = File.expand_path("External.LCA_RESTRICTED/Languages/Ruby/redist-libs", ENV["DLR_ROOT"])
  else
    TOPDIR = File.expand_path("../..", File.dirname(__FILE__))
    CONFIG["bindir"] = TOPDIR + "/bin"
    CONFIG["libdir"] = TOPDIR + "/lib"
  end
  DESTDIR = TOPDIR && TOPDIR[/\A[a-z]:/i] || '' unless defined? DESTDIR
  CONFIG["DESTDIR"] = DESTDIR
  CONFIG["prefix"] = (TOPDIR || DESTDIR + "")
  CONFIG["exec_prefix"] = "$(prefix)"
  CONFIG["sbindir"] = "$(exec_prefix)/sbin"
  CONFIG["libexecdir"] = "$(exec_prefix)/libexec"
  CONFIG["datadir"] = "$(prefix)/share"
  CONFIG["sysconfdir"] = "$(prefix)/etc"
  CONFIG["sharedstatedir"] = "$(DESTDIR)/etc"
  CONFIG["localstatedir"] = "$(DESTDIR)/var"
  CONFIG["includedir"] = "$(prefix)/include"
  CONFIG["infodir"] = "$(prefix)/info"
  CONFIG["mandir"] = "$(prefix)/man"
  CONFIG["rubylibdir"] = "$(libdir)/ruby/$(ruby_version)"
  CONFIG["sitedir"] = "$(libdir)/ruby/site_ruby"

  CONFIG["oldincludedir"] = "/usr/include"
  cpu_and_os = RUBY_PLATFORM.split('-')
  abort("Could not parse RUBY_PLATFORM") if cpu_and_os.size != 2
  CONFIG["host_cpu"] = (cpu_and_os[0] == "i386") ? "i686" : cpu_and_os[0]
  CONFIG["host_os"] = cpu_and_os[1]
  clr_version = "#{System::Environment.Version.Major}.#{System::Environment.Version.Minor}"
  CONFIG["target"] = "dotnet#{clr_version}"
  CONFIG["arch"] = "universal-#{CONFIG["target"]}"
  CONFIG["build"] = CONFIG["arch"] # Not strictly true. For example, while running a .NET 2.0 version of IronRuby on .NET 4
  CONFIG["target_alias"] = CONFIG["target"]
  CONFIG["target_cpu"] = cpu_and_os[0]
  CONFIG["target_vendor"] = "pc"
  CONFIG["target_os"] = CONFIG["host_os"]
  CONFIG["CC"] = "cl -nologo"
  CONFIG["CPP"] = "cl -nologo -E"
  CONFIG["YACC"] = "byacc"
  CONFIG["RANLIB"] = ""
  CONFIG["AR"] = "lib -nologo"
  CONFIG["ARFLAGS"] = "-machine:x86 -out:"
  CONFIG["LN_S"] = ""
  CONFIG["SET_MAKE"] = ""
  CONFIG["CP"] = "copy > nul"
  CONFIG["ALLOCA"] = ""
  CONFIG["DEFAULT_KCODE"] = ""
  CONFIG["OBJEXT"] = "obj"
  CONFIG["XCFLAGS"] = "-DRUBY_EXPORT -I. -IC:/develop/win/ruby/ruby-1.8.6 -IC:/develop/win/ruby/ruby-1.8.6/missing"
  CONFIG["XLDFLAGS"] = "-stack:0x2000000"
  CONFIG["DLDFLAGS"] = "-link -incremental:no -debug -opt:ref -opt:icf -dll $(LIBPATH) -def:$(DEFFILE) -implib:$(*F:.so=)-$(arch).lib -pdb:$(*F:.so=)-$(arch).pdb"
  CONFIG["ARCH_FLAG"] = ""
  CONFIG["STATIC"] = ""
  CONFIG["CCDLFLAGS"] = ""
  CONFIG["LDSHARED"] = "cl -nologo -LD"
  CONFIG["DLEXT"] = "so"
  CONFIG["DLEXT2"] = "dll"
  CONFIG["LIBEXT"] = "lib"
  CONFIG["STRIP"] = ""
  CONFIG["EXTSTATIC"] = ""
  CONFIG["setup"] = "Setup"
  CONFIG["MINIRUBY"] = ".\\miniruby.exe "
  CONFIG["PREP"] = "miniruby.exe"
  CONFIG["RUNRUBY"] = ".\\ruby.exe \"C:/develop/win/ruby/ruby-1.8.6/runruby.rb\" --extout=\".ext\" --"
  CONFIG["EXTOUT"] = ".ext"
  CONFIG["ARCHFILE"] = ""
  CONFIG["RDOCTARGET"] = "install-nodoc"
  CONFIG["LIBRUBY_LDSHARED"] = "cl -nologo -LD"
  CONFIG["LIBRUBY_DLDFLAGS"] = " -def:msvcrt-ruby18.def"
  CONFIG["rubyw_install_name"] = "rubyw"
  CONFIG["RUBYW_INSTALL_NAME"] = "rubyw"
  CONFIG["LIBRUBY_A"] = "$(RUBY_SO_NAME)-static.lib"
  CONFIG["LIBRUBY_SO"] = "$(RUBY_SO_NAME).dll"
  CONFIG["LIBRUBY_ALIASES"] = ""
  CONFIG["LIBRUBY"] = "$(RUBY_SO_NAME).lib"
  CONFIG["LIBRUBYARG"] = "$(LIBRUBYARG_SHARED)"
  CONFIG["LIBRUBYARG_STATIC"] = "$(LIBRUBY_A)"
  CONFIG["LIBRUBYARG_SHARED"] = "$(LIBRUBY)"
  CONFIG["SOLIBS"] = ""
  CONFIG["DLDLIBS"] = ""
  CONFIG["ENABLE_SHARED"] = "yes"
  CONFIG["OUTFLAG"] = "-Fe"
  CONFIG["CPPOUTFILE"] = "-P"
  CONFIG["LIBPATHFLAG"] = " -libpath:\"%s\""
  CONFIG["RPATHFLAG"] = ""
  CONFIG["LIBARG"] = "%s.lib"
  CONFIG["LINK_SO"] = "$(LDSHARED) -Fe$(@) $(OBJS) $(LIBS) $(LOCAL_LIBS) $(DLDFLAGS)"
  CONFIG["COMPILE_C"] = "$(CC) $(INCFLAGS) $(CFLAGS) $(CPPFLAGS) -c -Tc$(<:\\=/)"
  CONFIG["COMPILE_CXX"] = "$(CXX) $(INCFLAGS) $(CXXFLAGS) $(CPPFLAGS) -c -Tp$(<:\\=/)"
  CONFIG["COMPILE_RULES"] = "{$(srcdir)}.%s{}.%s: {$(topdir)}.%s{}.%s: {$(hdrdir)}.%s{}.%s: .%s.%s:"
  CONFIG["RULE_SUBST"] = "{.;$(srcdir);$(topdir);$(hdrdir)}%s"
  CONFIG["TRY_LINK"] = "$(CC) -Feconftest $(INCFLAGS) -I$(hdrdir) $(CPPFLAGS) $(CFLAGS) $(src) $(LOCAL_LIBS) $(LIBS) -link $(LDFLAGS) $(LIBPATH) $(XLDFLAGS)"
  CONFIG["COMMON_LIBS"] = "m"
  CONFIG["COMMON_MACROS"] = "WIN32_LEAN_AND_MEAN"
  CONFIG["COMMON_HEADERS"] = "winsock2.h windows.h"
  CONFIG["DISTCLEANFILES"] = "vc*.pdb"
  CONFIG["EXPORT_PREFIX"] = " "
  CONFIG["configure_args"] = "--with-make-prog=nmake --enable-shared --with-winsock2"
  CONFIG["ruby_version"] = "$(MAJOR).$(MINOR)"
  CONFIG["archdir"] = "$(rubylibdir)/$(arch)"
  CONFIG["sitelibdir"] = "$(sitedir)/$(ruby_version)"
  CONFIG["sitearchdir"] = "$(sitelibdir)/$(sitearch)"
  CONFIG["topdir"] = File.dirname(__FILE__)
  MAKEFILE_CONFIG = {}
  CONFIG.each{|k,v| puts k if v.nil?; MAKEFILE_CONFIG[k] = v.dup}
  def Config::expand(val, config = CONFIG)
    val.gsub!(/\$\$|\$\(([^()]+)\)|\$\{([^{}]+)\}/) do |var|
      if !(v = $1 || $2)
	'$'
      elsif key = config[v = v[/\A[^:]+(?=(?::(.*?)=(.*))?\z)/]]
	pat, sub = $1, $2
	config[v] = false
	Config::expand(key, config)
	config[v] = key
	key = key.gsub(/#{Regexp.quote(pat)}(?=\s|\z)/n) {sub} if pat
	key
      else
	var
      end
    end
    val
  end
  CONFIG.each_value do |val|
    Config::expand(val)
  end
end
RbConfig = Config # compatibility for ruby-1.9
CROSS_COMPILING = nil unless defined? CROSS_COMPILING
