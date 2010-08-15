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

require "stringio"
require "pathname"
require "erb"
require "rdoc/markup/simple_markup"
require "rdoc/markup/simple_markup/to_html"
$: << File.expand_path(File.dirname(__FILE__) + "/app")
require "tutorial"

# Utility class to convert from RDoc SimpleMarkup text to WPF FlowDocument.
# It adds hyperlinking functinality to SM::ToHtml similar to Generators::HyperlinkHtml,
# and also adds stylesheet annotations for <p> tags

class ToStyledHtml < SM::ToHtml
    def initialize paragraph_tag = "<p>"
      @paragraph_tag = paragraph_tag
      super()
    end
    
    def annotate tag
      if tag == "<p>" then @paragraph_tag else tag end
    end
    
    def gen_url(url, text)
      if url =~ /([A-Za-z]+):(.*)/
        type = $1
        path = $2
      else
        type = "http"
        path = url
        url  = "http://#{url}"
      end

      if (type == "http") && 
          url =~ /\.(gif|png|jpg|jpeg|bmp)$/

        "<img src=\"#{url}\" />"
      else
        "<a href=\"#{url}\">#{text.sub(%r{^#{type}:/*}, '')}</a>"
      end
    end

    def handle_special_HYPERLINK(special)
      url = special.text
      gen_url(url, url)
    end

    def handle_special_TIDYLINK(special)
        text = special.text
#       unless text =~ /(\S+)\[(.*?)\]/
        unless text =~ /\{(.*?)\}\[(.*?)\]/ or text =~ /(\S+)\[(.*?)\]/ 
            handle_special_HYPERLINK(special, paragraph)
            return
        end
        label = $1
        url   = $2
      
      gen_url(url, label)
    end
        
    def self.convert text, paragraph_tag = '<p class="Body">'
        if not text then return "" end
        
        if not @markupParser
            @markupParser = SM::SimpleMarkup.new
            # external hyperlinks
            @markupParser.add_special(/((link:|https?:|mailto:|ftp:|www\.)\S+\w)/, :HYPERLINK)

            # and links of the form  <text>[<url>]
            @markupParser.add_special(/(((\{.*?\})|\b\S+?)\[\S+?\.\S+?\])/, :TIDYLINK)
            # @markupParser.add_special(/\b(\S+?\[\S+?\.\S+?\])/, :TIDYLINK)
        end
        
        begin
          @markupParser.convert(text, ToStyledHtml.new(paragraph_tag))
        rescue Exception => e
            puts "Error while converting:\n#{text}"
            raise e
        end
    end
end

class HtmlTutorial
    attr :tutorial
    
    def initialize(tutorial = nil)
        if tutorial
            @tutorial = tutorial
        else
            @tutorial = Tutorial.get_tutorial
        end

        @context = Tutorial::ReplContext.new
    end

    @@rhtml = %q{
      <head>
        <title><%= @tutorial.name %></title>
        <style type="text/css">
        <%= css_file_contents %>
        </style>
      </head>
      
      <body>
        <h1><%= @tutorial.name %></h1>
        <p class="Body">
          <%= ToStyledHtml.convert(@tutorial.introduction) %>
        </p>
        
        <h2>Table of Contents</h2>
          <ul>
          <% @tutorial.sections.each do |section| %>
            <li>
            <%= section.name %>
            <ul>
            <% section.chapters.each do |chapter| %>
              <li><%= chapter.name %></li>
            <% end %>            
            </ul>
            </li>
          <% end %>
          </ul>
              
        <% @tutorial.sections.each do |section| %>
          <h2><%= section.name %></h2>
          <p class="Body"><%= ToStyledHtml.convert(section.introduction) %></p>
          <% section.chapters.each do |chapter| %>
            <h3><%= chapter.name %></h3>
            <p class="Body"><%= ToStyledHtml.convert(chapter.introduction) %></p>
            <% chapter.tasks.each do |task| %>
              <% next if not task.should_run? @context.bind %>
              <% task.setup.call(@context.bind) if task.setup %>
              <p class="Body"><%= ToStyledHtml.convert(task.description) %></p>
              <p class="Code-Highlighted">
              <% if task.code.respond_to?(:to_ary) %>
                <% task.code.to_ary.each do |code| %>
                  <b>
                  >>> <%= code %>
                  </b>
                  <br>
                  <% result = @context.interact code %>
                  <%= "#{result.output.gsub(/\n/, '<br>')}<br>" if not result.output.empty? %>
                  <%= "#{result.error.inspect.gsub(/\n/, '<br>')}<br>" if result.error %>
                  <%= "=> #{result.result.inspect.gsub(/\n/, '<br>')}<br>" if not result.error %>
                  <br>                
                <% end %>
              <% else %>
                <b>
                >>> <%= task.code %>
                </b>
                <br>
                  <% result = @context.interact task.code %>
                  <%= "#{result.output.gsub(/\n/, '<br>')}<br>" if not result.output.empty? %>
                  <%= "#{result.error.inspect.gsub(/\n/, '<br>')}<br>" if result.error %>
                  <%= "=> #{result.result.inspect.gsub(/\n/, '<br>')}<br>" if not result.error %>
              <% end %>
              </p>
            <% end %>
          <% end %>
        <% end %>
      </body>
    }
    
    @@erb = ERB.new(@@rhtml)

    def generate_html html_file_name = nil
      css_file = File.expand_path(File.dirname(__FILE__) + "/css") + "/Tutorial.css"
      warn "Missing file #{css_file}" if not File.exist? css_file

      if html_file_name # can be nil if generating html in memory
        html_file_path = Pathname.new(File.dirname(html_file_name))
      end

      css_file_contents = File.read(css_file)

      @@erb.result(binding)
    end
    
    def generate_file 
      dir = File.dirname(@tutorial.file)
      base_name = File.basename(@tutorial.file, ".rb")
      html_file_name = File.expand_path(base_name + ".generated.html", dir)

      html = generate_html html_file_name

      open(html_file_name, "w+") { |file| file << html }
      
      puts "Generated #{html_file_name}"
      html_file_name
    end
    
    def ensure_generated
      # TODO - Need to check timestamp or hash to see if the file really needs to be generated
      generate_file
    end
end

if $0 == __FILE__
  Tutorial.all.values.each{|t| HtmlTutorial.new(t).ensure_generated }
end
