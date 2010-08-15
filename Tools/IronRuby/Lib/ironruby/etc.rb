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

module Etc

  Struct.new('Passwd', :name, :passwd, :uid, :gid, :gecos, :dir, :shell)
  Struct.new('Group', :name, :passwd, :gid, :mem)

  class << self
    platform = System::PlatformID
    case System::Environment.OSVersion.Platform
    when platform.Win32S, platform.WinCE, platform.Win32Windows, platform.Win32NT:
      def getlogin
        ENV['USERNAME']
      end
    
      def endgrent(*args)
        nil
      end

      [:endpwent, :getgrent, :getgrgid, :getgrnam, :getpwent, 
       :getpwnam, :getpwuid, :group, :passwd, :setgrent,
       :setpwent].each do |method|
        alias_method method, :endgrent
      end
    else
      load_assembly 'Mono.Posix'
 
      def endgrent
        Mono::Unix::Native::Syscall.endgrent
        nil
      end

      def endpwent
        Mono::Unix::Native::Syscall.endpwent
        nil
      end

      def getgrent
        result = Mono::Unix::Native::Syscall.getgrent
        to_group(result)
      end

      def getgrgid(p1)
        result = Mono::Unix::Native::Syscall.getgrgid(p1)
        to_group(result)
      end

      def getgrnam(p1)
        result = Mono::Unix::Native::Syscall.getgrnam(p1.to_clr_string)
        to_group(result)
      end

      def getlogin
        result = Mono::Unix::Native::Syscall.getlogin
        String.new(result)
      end

      def getpwent
        result = Mono::Unix::Native::Syscall.getpwent
        to_passwd(result)
      end

      def getpwnam(p1)
        result = Mono::Unix::Native::Syscall.getpwnam(p1.to_clr_string)
        to_passwd(result)
      end

      def getpwuid(p1)
        result = Mono::Unix::Native::Syscall.getpwuid(p1)
        to_passwd(result)
      end

     def group
        if block_given? then
          # Reset groups
          setgrent

          # Get the first group, then loop until the last, yielding the current group.
          grp = getgrent
          until grp.nil?
            yield grp unless grp.nil?
            grp = getgrent
          end

          # Reset groups.
          setgrent
        end
        getgrent
      end

      def passwd
        if block_given? then
          # Reset passwd
          setpwent

          # Get the first passwd, then loop until the last, yielding the current passwd.
          pw = getpwent
          until pw.nil?
            yield pw 
            pw = getpwent
          end

          # Reset passwd.
          setpwent
        end
        getpwent
      end

      def setgrent
        Mono::Unix::Native::Syscall.setgrent
        nil
      end

      def setpwent
        Mono::Unix::Native::Syscall.setpwent
        nil
      end

      private

      def from_clr_array(clr_array)
        # to_a converts a System::Array to a Ruby Array,
        # but doesn't convert the internals to Ruby types.
        Array.new(clr_array.length){ |i| clr_array[i].to_s }
      end

      def to_group(result)
        Struct::Group.new(result.gr_name.to_s, result.gr_passwd.to_s, Fixnum.induced_from(result.gr_gid), from_clr_array(result.gr_mem)) unless result.nil?
      end

      def to_passwd(result)
        Struct::Passwd.new(result.pw_name.to_s, result.pw_passwd.to_s, Fixnum.induced_from(result.pw_uid), Fixnum.induced_from(result.pw_gid), result.pw_gecos.to_s, result.pw_dir.to_s, result.pw_shell.to_s) unless result.nil?
      end

    end
  end
end

