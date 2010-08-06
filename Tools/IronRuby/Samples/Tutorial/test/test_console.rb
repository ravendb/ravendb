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

require 'stringio'

SILVERLIGHT = false unless defined?(SILVERLIGHT)

if not SILVERLIGHT
  $: << File.expand_path(File.dirname(__FILE__) + '/..')
  $: << File.expand_path(File.dirname(__FILE__) + '/../app')
end

require 'tutorial'
require 'console_tutorial'

if not SILVERLIGHT
  require 'rubygems'
  require 'minitest/spec'
  require 'fileutils'
  require 'html_tutorial'

  MiniTest::Unit.autorun # tests will run using at_exit
  
  TUTORIAL_ROOT = File.expand_path("..", File.dirname(File.expand_path(__FILE__, FileUtils.pwd)))
end

class MiniTest::Unit::TestCase
  def self.test_order
    :not_random
  end
end

def get_standard_tutorial(file_name)
  path = "app/Tutorials/#{file_name}"
  path = File.expand_path(path, TUTORIAL_ROOT) if not SILVERLIGHT     
  Tutorial.get_tutorial path
end

describe "ReplContext" do
  before(:each) do
    @context = Tutorial::ReplContext.new
  end
  
  it "works with single-line input" do
    assert_equal @context.interact("2+2").result, 4
  end

  it "works with multi-line code" do
    code = ["if true", "101", "else", "102", "end"].join("\n")
    assert_equal @context.interact(code).result, 101
  end

  it "works with multi-line input" do
    result = nil
    ["if true", "101", "else", "102", "end"].each {|i| result = @context.interact i }
    assert_equal result.result, 101
  end

  it "can be reset" do
    ["if true", "101", "else"].each {|i| @context.interact i }
    @context.reset_input
    assert_equal @context.interact("2+2").result, 4
  end
end

describe "ConsoleTutorial" do 
  before(:each) do
    @in = StringIO.new
    @out = StringIO.new
    tutorial = get_standard_tutorial('tryruby_tutorial.rb')
    @app = ConsoleTutorial.new tutorial, @in, @out
  end
  
  it "should early out" do
    @in.string = ["0"].join("\n")
    @app.run
    assert_match @out.string, /Bye!/
  end

  it 'should chose a section' do
    @in.string = ["1", "0", "0"].join("\n")
    @app.run
    assert_match @out.string, /Bye!/
  end
end

# Helper module to programatically create a new spec methods for each task in each chapter
module TutorialTests 
  
  def self.format_interaction_result code, result
    "code = #{code.inspect} #{result}"
  end
  
  def self.create_tests testcase, tutorial_file
    tutorial = get_standard_tutorial tutorial_file
    context = Tutorial::ReplContext.new
    tutorial.sections.each_index do |s|
      section = tutorial.sections[s]
      section.chapters.each_index do |c|
        chapter = section.chapters[c]
        test_name = "#{section.name} - #{chapter.name}"
        
        testcase.it(test_name) { TutorialTests.run_test self, context, chapter }
      end
    end
  end
  
  def self.assert_task_success(spec, task, code, result, success=true)
    res = TutorialTests.format_interaction_result(code, result)
    if success
      spec.assert(task.success?(result, true), res)
    else
      spec.assert(!task.success?(result), res)
    end
  end

  def self.run_test spec, context, chapter
    chapter.tasks.each do |task| 
      if not task.should_run? context.bind 
        return
      end
      task.setup.call(context.bind) if task.setup
      task.test_hook.call(:before, spec, context.bind) if task.test_hook
      result = context.interact "" # Ensure that the user can try unrelated code snippets without moving to the next task
      if task.code.respond_to? :to_ary
        task.code.each do |code|
          assert_task_success spec, task, "before : #{code}", result, false
          result = context.interact code
        end
        assert_task_success spec, task, task.code.last, result
      else
        assert_task_success spec, task, "before : #{task.code_string}", result, false
        result = context.interact task.code_string
        assert_task_success spec, task, task.code_string, result
      end
      task.test_hook.call(:after, spec, context.bind) if task.test_hook
    end
  end
end

describe "IronRubyTutorial" do
  TutorialTests.create_tests self, 'ironruby_tutorial.rb' if defined? RUBY_ENGINE
end

describe "HostingTutorial" do
  TutorialTests.create_tests self, 'hosting_tutorial.rb' if defined? RUBY_ENGINE
end

describe "TryRubyTutorial" do
  TutorialTests.create_tests self, 'tryruby_tutorial.rb'
end

if not SILVERLIGHT
  describe "HtmlGeneratorTests" do
    it "basically works" do
      tutorial = get_standard_tutorial('tryruby_tutorial.rb')
      html_tutorial = HtmlTutorial.new tutorial
      html = html_tutorial.generate_html
      assert_match %r{<h2>Table of Contents</h2>}, html
    end
  end
end
