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



class ReplBufferStream
  def write_to_repl(str)
    GuiTutorial::Window.current.begin_invoke do
      $repl.output_buffer.write str
    end
  end

  def puts(*args)
    args.each do |arg| 
      write_to_repl arg.to_s
      write_to_repl "\n"
    end
  end
  
  def print (*args)
    args.each {|arg| write_to_repl arg.to_s }
  end

  def warn *args
    puts *args
  end

  def write(*args)
    print *args
  end
end

# minitest uses Signal.list, which does not exist in Silverlight. So fake it
class Signal
  def self.list() Hash.new end
end

def run_tests()
  $LOAD_PATH << "./Libs/minitest-1.4.2/lib"
  $0 = __FILE__ # minitest expects this to be non-nil
  require "minitest/spec"
  MiniTest::Unit.output = ReplBufferStream.new
  require "test/test_console"
  orig_stdout, orig_stderr = $stdout, $stderr
  $stdout = ReplBufferStream.new
  $stderr = ReplBufferStream.new
  MiniTest::Unit.new.run(ARGV)
  $stdout, $stderr = orig_stdout, orig_stderr
end

Thread.new { run_tests }
