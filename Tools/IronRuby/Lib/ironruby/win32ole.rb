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

class WIN32OLE
  # TODO - Instead of wrapping the RCWs (runtime-callable-wrapper), we should just
  # return them directly (and make WIN32OLE an alias for System.__ComObject). We currently 
  # wrap them so that we can cheaply customize the behavior of the resulting object 
  # (for eg, to convert the arguments).
  # However, all the customizations should be moved into the IronRuby runtime
  # itself and made to work directly against RCWs. This will allow better cross-language
  # inteorp of passing RCW arguments, better perf since it will avail of call-site
  # caching, and object identity.
  
  def initialize(arg)
    if arg.respond_to? :to_str
      if guid? arg
        type = System::Type.GetTypeFromCLSID arg
      else
        type = System::Type.GetTypeFromProgID arg
      end
      @com_object = System::Activator.create_instance type
    else
      @com_object = arg
    end
  end
  
  attr :com_object # The wrapped RCW
  
  def [](name)
    method_missing name
  end
  
  def []=(name, value)
    method_missing "#{name}=", value
  end
  
  # Used to set indexed property
  def setproperty(name, *args)
    method_missing "#{name}=", *args
  end
  
  def each(&b)
    # Reflection on COM objects does not show IEnumerable as a supported interface, but
    # casting to IEnumerable works (if the COM object supports enumeration). Hence,
    # we create a wrapper object which does the cast
    load_assembly "Microsoft.Dynamic"
    strongly_typed_enumerable = Microsoft::Scripting::Utils::EnumerableWrapper.new(@com_object)
    result = strongly_typed_enumerable.each(&b)
    (result == strongly_typed_enumerable) ? self : result
  end
  
  def method_missing(name, *args)
    if args.size == 1 and args[0] and args[0].kind_of? Hash
      raise NotImplementedError, "Named arguments not supported"
    end
    
    converted_args = ruby_to_com_interop_types(args)
    
    begin
      result = @com_object.send(name, *converted_args)
    rescue => e
      # Make sure the method name is in the exception message (useful for COMException)
      raise e if e.message =~ /#{name}/
      raise e.class, e.message + " while calling #{name}"
    end
    
    convert_return_value result
  end
  
  def self.const_load(ole_object, mod = WIN32OLE)
    raise NotImplementedError, "type library name not supported" if ole_object.respond_to? :to_str
    
    load_assembly "Microsoft.Dynamic"
    tlb = Microsoft::Scripting::ComInterop::ComTypeLibDesc.CreateFromObject(ole_object.com_object)
    constants = Hash.new
    tlb.TypeLibDesc.GetMemberNames.each do |tlb_entry_name|
      tlb_entry_value = tlb.TypeLibDesc.GetTypeLibObjectDesc tlb_entry_name
      if tlb_entry_value.kind_of? Microsoft::Scripting::ComInterop::ComTypeEnumDesc
        tlb_entry_value.GetMemberNames.each do |enum_name|
          enum_value = tlb_entry_value.GetValue enum_name
          enum_name = enum_name.to_str
          enum_name = enum_name[0...1].upcase + enum_name[1..-1]
          constants[enum_name] = enum_value
          mod.const_set enum_name, enum_value
        end
      end
    end
    mod.const_set "CONSTANTS", constants
  end
  
  def ole_method_help(method_name)
    raise NotImplementedError
  end

  def ole_obj_help
    raise NotImplementedError
  end

  #
  # Private methods
  #
  
  private
  
  def guid?(str)
    /[{]?[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}[}]?|[0-9A-F]{32}/ =~ str
  end

  def ruby_to_com_interop_type(arg)
    case arg
    when String
      arg.to_str
    when Array
      element_type = ruby_to_com_interop_type(arg[0]).class
      converted_elements = ruby_to_com_interop_types arg
      System::Array[element_type].new converted_elements
    when WIN32OLE
      arg.com_object
    else
      arg
    end
  end
  
  def ruby_to_com_interop_types(args)
    args.map { |arg| ruby_to_com_interop_type arg }
  end
  
  def convert_return_value(result)
    case result
    when nil
      result
    when System::String
      result.to_str
    when System::Decimal
      result.to_s
    when System::DBNull
      nil
    else
      if System::Runtime::InteropServices::Marshal.is_com_object result
        WIN32OLE.new result
      else
        result
      end
    end
  end
  
end

class WIN32OLE_EVENT
  def initialize(ole_object, out_interface_name)
    # Events are not supported even outside of win32ole
    raise NotImplementedError, "Events are not supported on COM objects"
    @ole_object = ole_object
  end
  
  def on_event(method_name = nil, &b)
    event = @ole_object.com_object.send method_name.to_sym
    event.add b
  end
end
