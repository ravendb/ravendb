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

SILVERLIGHT = !System::Type.get_type('System.Windows.Browser.HtmlPage, System.Windows.Browser, Version=2.0.5.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e').nil? unless defined? SILVERLIGHT
MOONLIGHT   = !System::Type.get_type('Mono.MoonException, System.Windows, Version=2.0.5.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e').nil? unless defined? MOONLIGHT

if not SILVERLIGHT
  # Reference the WPF assemblies
  require 'system.xml, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089' 
  require 'PresentationFramework, Version=3.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35'
  require 'PresentationCore, Version=3.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35'
  require 'windowsbase, Version=3.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35'
else
  require 'System.Xml, Version=2.0.5.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e'
end

class System::Windows::FrameworkElement
  # Monkey-patch FrameworkElement to allow window.ChildName instead of window.FindName("ChildName")
  # If FindName doesn't yield an object, it tried the Resources collection (for things like Storyboards)
  # TODO - Make window.child_name work as well
  def method_missing name, *args
    obj   = find_name(name.to_s.to_clr_string) 
    obj ||= self.resources[name.to_s.to_clr_string]
    obj || super
  end

  def hide!
    self.visibility = System::Windows::Visibility.hidden
  end

  def collapse!
    self.visibility = System::Windows::Visibility.collapsed
  end

  def show!
    self.visibility = System::Windows::Visibility.visible
  end

  # * Makes the element with a name of "element_name" as visible if "value" is not nil (or false)
  #   and invokes the given block
  # * Makes the element as collapsed if "value" is nil (or false)
  # 
  # Example:
  #   my_window.set_or_collapse(:label, some_message) { |element, value| element.text = value }
  #
  def set_or_collapse(element_name, value)
    obj = send(element_name)
    if obj && value
      yield obj, value
      obj.show!
    else
      obj.collapse!
    end
  end
  
  def content=(value)
    send "#{respond_to?(:Content) ? :Content : :Text}=", value
  end
end

if not SILVERLIGHT
  class System::Windows::Controls::RichTextBox
    def document=(value)
      smflow = value.kind_of?(Wpf::SimpleMarkupFlow) ? value : Wpf::SimpleMarkupFlow.new(value)
      self.Document = smflow.flow
    end
  end
end

class System::Windows::Controls::TextBox
  def document=(value)
    if SILVERLIGHT
      self.Text = ""
      smflow = value.kind_of?(Wpf::SimpleMarkupFlow) ? value : Wpf::SimpleMarkupFlow.new(value)
      smflow.flow.each {|i| self.inlines.add i }
      self.TextWrapping = TextWrapping.Wrap # TODO : Move this to XAML
    else
      self.Text = value.flow
    end
  end
end

class System::Windows::Controls::TextBlock
  def document=(value)
    if SILVERLIGHT
      self.Text = ""
      smflow = value.kind_of?(Wpf::SimpleMarkupFlow) ? value : Wpf::SimpleMarkupFlow.new(value)
      smflow.flow.each {|i| self.inlines.add i }
      self.TextWrapping = TextWrapping.Wrap # TODO : Move this to XAML
    else
      self.Text = value.flow
    end
  end
end

if SILVERLIGHT
  class System::Windows::Controls::ScrollViewer
    def scroll_to_top
      scroll_to_vertical_offset(0)
    end

    def scroll_to_bottom
      scroll_to_vertical_offset(actual_height + scrollable_height)
    end
  end
end

class System::Windows::Markup::XamlReader
  class << self
    alias raw_load load unless method_defined? :raw_load
  end

  def self.load(xaml)
    obj = if SILVERLIGHT
      self.Load(xaml)
    else
      return raw_load(xaml) unless xaml.respond_to? :to_clr_string
    
      self.Load(
        System::Xml::XmlReader.create(
          System::IO::StringReader.new(xaml.to_clr_string)))
    end
    yield obj if block_given?
    obj
  end

  def self.erb_load(xaml, b, &block)
    require 'erb'
    self.load(ERB.new(xaml).result(b).to_s, &block)
  end
end

class Module
  # methods - array of method names to redirect to another method with a varying method name.
  # This is useful when a window has a varying array of sub-controls. A single method name
  # can be used to invoke a function on, say, the last sub-control in the array, without
  # having to know the exact current size of the array.
  #
  # Example:
  #  class Example
  #    attr_accessor :idx
  #    def current_text() @idx.to_s end
  #    def show_text_1() @textbox1.show! end
  #    def show_text_2() @textbox2.show! end
  #    delegate_methods [:show_text], :append => :current_text
  #  end
  #
  #  c = Example.new
  #  c.idx = 2
  #  c.show_text # => calls show_text_2
  def delegate_methods(methods, opts = {})
    raise TypeError, "methods should be an array" unless methods.kind_of?(Array)
    this = self
    opts[:to]      ||= self
    opts[:prepend]   = opts[:prepend] ? "#{opts[:prepend]}_" : ''
    opts[:append]    = if opts[:append]
                         append = opts[:append]
                         lambda{|this| "_#{this::send(append)}" }
                       else
                         lambda{|this| '' }
                       end

    methods.each do |method|
      define_method(method.to_s.to_sym) do
        send(opts[:to]).send "#{opts[:prepend]}#{method}#{opts[:append][self]}"
      end
    end
  end
end

if SILVERLIGHT
  class System::Windows::DependencyObject
    def begin_invoke &block
      require 'System.Core, Version=2.0.5.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e'
      dispatch_callback = System::Action[0].new block
      self.dispatcher.begin_invoke dispatch_callback
    end
  end
else
  class System::Windows::Threading::DispatcherObject
    def begin_invoke &block
      require "system.core, Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
      dispatch_callback = System::Action[0].new block
      self.dispatcher.begin_invoke System::Windows::Threading::DispatcherPriority.Normal, dispatch_callback
    end
  end
end

module Wpf
  include System::Windows
  include System::Windows::Documents
  include System::Windows::Controls
  include System::Windows::Input
  include System::Windows::Markup
  include System::Windows::Media

  def self.load_xaml_file(filename)
    f = System::IO::FileStream.new filename, System::IO::FileMode.open, System::IO::FileAccess.read
    begin
      element = XamlReader.load f
    ensure
      f.close
    end
    element
  end
  
  # Returns an array with all the children, or invokes the block (if given) for each child
  # Note that it also includes content (which could be just strings).
  def self.walk(tree, &b)
    if not block_given?
      result = []
      walk(tree) { |child| result << child }
      return result
    end

    yield tree

    if tree.respond_to? :Children
      tree.Children.each { |child| walk child, &b }
    elsif tree.respond_to? :Child
      walk tree.Child, &b
    elsif tree.respond_to? :Content
      walk tree.Content, &b
    end
  end

  # If you constructed your treeview with XAML, you should
  # use this XAML snippet instead to auto-expand items:
  #
  # <TreeView.ItemContainerStyle>
  #   <Style>
  #     <Setter Property="TreeViewItem.IsExpanded" Value="True"/>
  #     <Style.Triggers>
  #       <DataTrigger Binding="{Binding Type}" Value="menu">
  #         <Setter Property="TreeViewItem.IsSelected" Value="True"/>
  #       </DataTrigger>
  #     </Style.Triggers>
  #   </Style>
  # </TreeView.ItemContainerStyle>
  #
  # If your treeview was constructed with code, use this method
  def self.select_tree_view_item(tree_view, item)
    return false unless self and item

    childNode = tree_view.ItemContainerGenerator.ContainerFromItem item
    if childNode
      childNode.focus
      childNode.IsSelected = true
      # TODO - BringIntoView ?
      return true
    end

    if tree_view.Items.Count > 0
      tree_view.Items.each do |childItem|
        childControl = tree_view.ItemContainerGenerator.ContainerFromItem(childItem)
        return false if not childControl

        # If tree node is not loaded, its sub-nodes will be nil. Force them to be loaded
        old_is_expanded = childControl.is_expanded
        childControl.is_expanded = true
        childControl.update_layout

        if select_tree_view_item childControl, item
          return true
        else
          childControl.is_expanded = old_is_expanded
        end
      end
    end

    false
  end

  def self.create_sta_thread &block
    ts = System::Threading::ThreadStart.new &block
    t = Thread.clr_new ts
    t.ApartmentState = System::Threading::ApartmentState.STA
    t.Start
  end

  # Some setup is needed to use WPF from an interactive session console (like iirb). This is because
  # WPF needs to do message pumping on a thread, iirb also requires a thread to read user input,
  # and all commands that interact with UI need to be executed on the message pump thread.
  def self.interact
    raise NotImplementedError, "Wpf.interact is not implemented yet"
    def CallBack(function, priority = DispatcherPriority.Normal)
      Application.Current.Dispatcher.BeginInvoke(priority, System::Action[].new(function))
    end
    
    def CallBack1(function, arg0, priority = DispatcherPriority.Normal)
       Application.Current.Dispatcher.BeginInvoke(priority, System::Action[arg0.class].new(function), arg0)
    end
    
    dispatcher = nil    
    message_pump_started = System::Threading::AutoResetEvent.new false

    create_sta_thread do
      app = Application.new
      app.startup do
        dispatcher = Dispatcher.FromThread System::Threading::Thread.current_thread
        message_pump_started.set
      end
      begin
        app.run
      ensure
        IronRuby::Ruby.SetCommandDispatcher(None) # This is a non-existent method that will need to be implemented
      end
    end
    
    message_pump_started.wait_one
    
    def dispatch_console_command(console_command)
      if console_command
        dispatcher.invoke DispatcherPriority.Normal, console_command
      end
    end
    
    IronRuby::Ruby.SetCommandDispatcher dispatch_console_command # This is a non-existent method that will need to be implemented
  end

  # Converts text in RDoc simple markup format 
  class SimpleMarkupFlow
    include System::Windows
    include System::Windows::Documents
        
    def initialize(text)
      require 'rdoc/markup/simple_markup'
      require 'rdoc/markup/simple_markup/inline'
  
      if not @markupParser
        @markupParser = SM::SimpleMarkup.new
        
        # external hyperlinks
        @markupParser.add_special(/((link:|https?:|mailto:|ftp:|www\.)\S+\w)/, :HYPERLINK)
  
        # and links of the form  <text>[<url>]
        @markupParser.add_special(/(((\{.*?\})|\b\S+?)\[\S+?\.\S+?\])/, :TIDYLINK)
        # @markupParser.add_special(/\b(\S+?\[\S+?\.\S+?\])/, :TIDYLINK)
      end
      
      begin
        @markupParser.convert(text, self)
      rescue Exception => e
        puts "#{e} while converting: #{text[0..50]}"
        raise e
      end
    end

    # Returns an array of Inline object on Silverlight, or a WPF FlowDocument object otherwise
    attr_accessor :flow

    def add_paragraph(item, bold = true, font_family = nil)
      if item.respond_to? :to_str
        item = SimpleMarkupFlow.create_run item.to_str, bold, font_family
      end

      if SILVERLIGHT
        if item.respond_to? :to_ary
          @flow += item
        else
          @flow << item
        end
        @flow << LineBreak.new
      else
        para = Paragraph.new
        if item.respond_to? :to_ary
          items.each {|i| para.inlines.add i }
        else
          para.inlines.add item
        end
        @flow.blocks.add para
      end
    end
    
    def self.create_run(text, bold = false, font_family = nil, font_style = nil)
      run = Run.new
      run.text = text
      run.font_family = FontFamily.new font_family if font_family 
      run.font_weight = FontWeights.Bold if bold
      run.font_style = font_style if font_style
      run
    end
    
    def start_accepting
      @@bold_mask = SM::Attribute.bitmap_for :BOLD
      @@italics_mask = SM::Attribute.bitmap_for :EM
      @@tt_mask = SM::Attribute.bitmap_for :TT
      @@hyperlink_mask = SM::Attribute.bitmap_for :HYPERLINK
      @@tidylink_mask = SM::Attribute.bitmap_for :TIDYLINK

      @flow = SILVERLIGHT ? [] : FlowDocument.new
      @attributes = []
    end
    
    def end_accepting
      @flow
    end

    def accept_paragraph(am, fragment)
      inlines = convert_flow(am.flow(fragment.txt))
      if SILVERLIGHT
        @flow += inlines
        @flow << LineBreak.new
      else
        paragraph = Paragraph.new
        inlines.each {|i| paragraph.inlines.add i }
        @flow.blocks.add paragraph
      end
    end

    def convert_flow(flow)
      inlines = []
      active_attribute = nil

      flow.each do |item|
        case item
        when String
        
          run = Run.new
          run.Text = item
          
          case active_attribute
          when @@bold_mask
            run.font_weight = FontWeights.Bold
            @attributes.clear
          when @@italics_mask
            run.font_style = FontStyles.Italic
          when @@tt_mask
            run.font_weight = FontWeights.Bold
            run.font_family = FontFamily.new "Consolas"
          when nil
          else
            raise "unexpected active_attribute: #{active_attribute}"
          end
          
          inlines << run
            
        when SM::AttrChanger
          on_mask = item.turn_on
          active_attribute = on_mask if not on_mask.zero?
          off_mask = item.turn_off
          if not off_mask.zero?
            raise NotImplementedError.new("mismatched attribute #{SM::Attribute.as_string(off_mask)} with active_attribute=#{SM::Attribute.as_string(active_attribute)}") if off_mask != active_attribute
            active_attribute = nil
          end

        when SM::Special
          convert_special(item, inlines)

        else
          raise "Unknown flow element: #{item.inspect}"
        end
      end
    
      raise "mismatch" if active_attribute
      
      inlines
    end

    def accept_verbatim(am, fragment)
      add_paragraph fragment.txt, true, "Consolas"
    end

    def accept_list_start(am, fragment)
      @list = System::Windows::Documents::List.new if not SILVERLIGHT
    end

    def accept_list_end(am, fragment)      
      @flow.blocks.add @list if not SILVERLIGHT
    end

    def accept_list_item(am, fragment)
      inlines = convert_flow(am.flow(fragment.txt))
      if SILVERLIGHT
        run = SimpleMarkupFlow.create_run "o  ", true
        inlines.unshift run
        add_paragraph inlines
      else
        paragraph = Paragraph.new
        inlines.each {|i| paragraph.inlines.add i }
        list_item = ListItem.new paragraph
        @list.list_items.add list_item
      end
    end

    def accept_blank_line(am, fragment)
      @flow << LineBreak.new if SILVERLIGHT
    end

    def accept_rule(am, fragment)
      raise NotImplementedError, "accept_rule: #{fragment.to_s}"
    end
    
    def convert_special(special, inlines)
      handled = false
      SM::Attribute.each_name_of(special.type) do |name|
        method_name = "handle_special_#{name}"
        return send(method_name, special, inlines) if self.respond_to? method_name
      end
      raise "Unhandled special: #{special}"
    end

    def handle_special_HYPERLINK(special, inlines)
      run = SimpleMarkupFlow.create_run special.text
      if SILVERLIGHT
        inlines << run
      else
        inlines << Hyperlink.new(run)
      end
    end

    def handle_special_TIDYLINK(special, inlines)
      text = special.text
      # text =~ /(\S+)\[(.*?)\]/
      unless text =~ /\{(.*?)\}\[(.*?)\]/ or text =~ /(\S+)\[(.*?)\]/ 
        handle_special_HYPERLINK(special, inlines)
        return
      end

      label = $1
      url   = $2

      if SILVERLIGHT
        run = SimpleMarkupFlow.create_run "#{label} (#{url})"
        inlines << run
      else
        run = SimpleMarkupFlow.create_run label
        hyperlink = Hyperlink.new run
        hyperlink.NavigateUri = System::Uri.new url
        inlines << hyperlink
      end
    end
  end
end

