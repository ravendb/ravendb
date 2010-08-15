#####################################################################################
#
#  Copyright (c) Microsoft Corporation. All rights reserved.
#
# This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
# copy of the license can be found in the License.html file at the root of this distribution. If 
# you cannot locate the  Apache License, Version 2.0, please send an email to 
# ironpy@microsoft.com. By using this source code in any fashion, you are agreeing to be bound 
# by the terms of the Apache License, Version 2.0.
#
# You must not remove this notice, or any other, from this software.
#
#
#####################################################################################

require 'mscorlib'
require 'System.Management.Automation'
include System
include System::Management::Automation
include System::Management::Automation::Host
include System::Management::Automation::Runspaces

$runspace = RunspaceFactory.create_runspace
$runspace.open
$intrinsics = $runspace.session_state_proxy.get_variable("ExecutionContext")

class Object
  def pop_arg_hash(args)
    result = args.last.is_a?(Hash) ? args.pop : {}
    return result, args
  end
end

module PowerShell
=begin
  Utility function converts a string, name, to lowercase.
  Also replaces hyphens with underscores.
=end
  def translate(name)
    name.downcase.gsub(/-/,"_")
  end

=begin
   Utility function converts arg (of type string, PSObject, or
   ShellOutput) to type string.
=end
  def fix_arg(arg)
    case arg
    when String
      $intrinsics.invoke_command.expand_string(arg)
    when PSObject
      arg
    when ShellOutput
      arg.data
    else
      arg
    end
  end

=begin
  Used to actually invoke a powershell command
=end
  def invoke_command(cmd_name, input = nil, *args)
    arg_hash, args = pop_arg_hash(args)
    cmd = Command.new(cmd_name)
    args.each {|arg| cmd.parameters.add(CommandParameter.new(nil, fix_arg(arg)))}
    arg_hash.each {|key, value| cmd.parameters.add(CommandParameter.new(key.to_s, fix_arg(value)))}
    
    #Create a pipeline to run the command within and invoke
    #the command.
    pipeline = $runspace.create_pipeline
    pipeline.commands.add(cmd)

    ret = if input
            pipeline.invoke(fix_arg(input))
          else
            pipeline.invoke
          end
   
    #return the output of the command formatted special
    #using the ShellOutput class
    ShellOutput.new(ret)
  end
end

=begin
    Instances of this class are like pseudo PowerShell
    shells. That is, this class essentially has a method for
    each PowerShell command available.
=end
class Shell
  def initialize(data)
    @data = data
  end
  
  def method_missing(sym, *args, &block)
    if @data[sym.to_s]
      return @data[sym.to_s].call(*args, &block)
    end
    raise NoMethodError(sym, *args, &block)
  end
end

=begin
  Wrapper class for shell commands
=end
class ShellCommand
  include PowerShell
  attr_accessor :input
  def initialize(name, input=nil)
    @name = name
    @input = input
  end

  def call(*args)
    invoke_command(@name, @input, *args)
  end

  def to_s
    "#<ShellCommand #{@name}>"
  end
end

class ShellOutput
  attr_accessor :data
  def initialize(data)
    @data = data
  end
  
  def self.[]=(name, value)
    define_method(name) do |*args| 
      input, value.input = value.input, self
      result = value.call(*args)
      value.input = input
      result
    end
  end

  def each
    @data.each {|el| yield el}
  end

  def length
    @data.count
  end

  def inspect
    return "" if @data.count.zero?
    out_string(:Width => (System::Console.BufferWidth-1))[0].to_s.strip
  end
  alias_method :to_s, :inspect

  def [](key)
    @data[key]
  end
end

class PSObject
  def method_missing(sym, *args, &block)
    member = members[sym.to_s]
    if member
      result = member.value
      if result.is_a? PSMethod
        result = result.invoke(*args)
      end
      result
    else
      raise NoMethodError.new(sym, *args, &block)
    end
  end

  def inspect
    members["ToString"].invoke
  end
  alias_method :to_s, :inspect
end

class Program
  include PowerShell

  def initialize
    @cmds = {}
    invoke_command('get-command').each do |cmdlet| 
      @cmds[translate(cmdlet.name)] = ShellCommand.new cmdlet.name  
    end
    
    invoke_command('get-alias').each do |al|
      cmd_name = translate(al.get_ReferencedCommand.name)
      if @cmds.include? cmd_name
        @cmds[translate(al.name)] = @cmds[cmd_name]
      end
    end

    $shell = Shell.new @cmds
    @cmds.each {|key, value| ShellOutput[key] = value }
  end
end

Program.new

if __FILE__ != $0
  puts <<EOL
Run '$shell.instance_variables' to get a list of available PowerShell commands!
In general, IronRuby PowerShell commands are accessed using the form:
  $shell.get_process("cmd")
EOL
end
