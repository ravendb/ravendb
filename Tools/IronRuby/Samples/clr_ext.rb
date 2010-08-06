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

#TODO: wpf.rb helpers from Tutorial?
#TODO: split into multiple files
#TODO: any other helpers?
module CLR
  module Array
    def to_clr_array(*type)
      result = self.map {|el| yield el} if block_given?
      System::Array[*type].new(result)
    end
  end

  module TypedObject
    def bind(type)
      @type = type
    end

    def method_missing(name, *args, &blk)
      if respond_to? :clr_member
        clr_member(@type, name).call(*args, &blk)
      else
        raise NoMethodError.new(name, *args, &blk)
      end
    end
  end

  module Kernel
    def as(type)
      raise TypeError.new("#{self.class.name} does not inherit from or include #{type.name}") unless self.kind_of?(type)
      class << self
        include TypedObject
      end
      self.bind(type)
      self
    end
  end

  module Events
    #Thank you Ivan :)
    def self.included(base)  
      base.extend ClassMethods 
    end 
    module ClassMethods 
      def attr_event(*names) 
        names.each do |name|
          nms = name.to_s   
          nm = nms.respond_to?(:underscore) ? nms.underscore : nms.gsub(/::/, '/').     
            gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2').  
            gsub(/([a-z\d])([A-Z])/, '\1_\2').      
            tr("-", "_").      
            downcase     
          kn = nms.respond_to?(:classify) ? nms.classify : nms.gsub(/\/(.?)/) { "::" + $1.upcase }.gsub(/(^|_)(.)/) { $2.upcase }   
          add_add_handler nm, kn     
          add_remove_handler nm, kn  
          add_event_trigger nm   
        end   
      end
      private     
      def add_add_handler(underscore, klass)     
        self.send :define_method :"add_#{klass}" do |arg|  
          vr = instance_variable_get :"@__#{underscore}_handlers__"    
          vr ||= []    
          vr << arg 
          instance_variable_set :"@__#{underscore}_handlers__", vr    
        end  
      end   
      def add_remove_handler(underscore, klass)    
        self.send :define_method :"remove_#{klass}" do |arg|   
          vr = instance_variable_get :"@__#{underscore}_handlers__"     
          vr ||= []     
          vr.delete arg    
          instance_variable_set :"@__#{underscore}_handlers__", vr 
        end 
      end   
      def add_event_trigger(underscore)    
        self.send :define_method :"raise_#{underscore}" do |sender, arg|  
          vr = instance_variable_get :"@__#{underscore}_handlers__"     
          return unless vr   
          vr.each do |ev|    
            ev.invoke self, arg if ev.respond_to? :invoke      
            ev.call self, arg if ev.respond_to? :call        
          end  
        end 
      end 
    end
  end
end

class Array
  include CLR::Array
end

module Kernel
  include CLR::Kernel
end

class Object
  include Kernel
end
