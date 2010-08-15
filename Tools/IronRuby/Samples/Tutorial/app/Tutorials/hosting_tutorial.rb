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

require "tutorial"

module HostingTutorial
  def self.ensure_script_method
    task(:run_unless => lambda { |bind| bind.script('2+2') == 4 },
         :body => %{
             This chapter uses the method +script+ that was defined in the first chapter. Enter the
             following commands, or go through the first chapter first.
         },
         :code => [
            'engine = IronRuby.create_engine',
            '$engine = engine',
            'def script(script_code) $engine.execute(script_code) end']
         ) { |iar| iar.bind.script('2+2') == 4 }
  end
  
  class RedirectingOutputStream < System::IO::Stream
    def initialize
      @encoding = System::Text::UTF8Encoding.new
      super
    end
    
    def can_seek
     false
    end
    
    def can_read
     false
    end
  
    def can_write
     true
    end
    
    # TODO - This does not deal with any encoding issues
    def write(buffer, offset, count)
      # Do the actual write. Note that this will automatically honor $stdout redirection 
      # of the ScriptEngine of the tutorial application.
      print @encoding.get_string(buffer, offset, count)
    end
  end
end

load_assembly "Microsoft.Scripting"

module Microsoft
  module Scripting
    module Hosting
      class ScriptEngine
        def redirect_output
          stream = HostingTutorial::RedirectingOutputStream.new
          @encoding ||= System::Text::UTF8Encoding.new
          self.runtime.io.set_output(stream, @encoding)
        end
      end
    end
  end
end

# All strings use the RDoc syntax documented at 
# http://www.ruby-doc.org/stdlib/libdoc/rdoc/rdoc/index.html

tutorial "IronRuby Hosting tutorial" do

    legal %{
        Information in this document is subject to change without notice. The example companies,
        organizations, products, people, and events depicted herein are fictitious. No association with any
        real company, organization, product, person or event is intended or should be inferred. Complying with
        all applicable copyright laws is the responsibility of the user. Without limiting the rights under
        copyright, no part of this document may be reproduced, stored in or introduced into a retrieval
        system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording,
        or otherwise), or for any purpose, without the express written permission of Microsoft Corporation.

        Microsoft may have patents, patent applications, trademarked, copyrights, or other intellectual
        property rights covering subject matter in this document. Except as expressly provided in any written
        license agreement from Microsoft, the furnishing of this document does not give you any license to
        these patents, trademarks, copyrights, or other intellectual property.

        (c) Microsoft Corporation. All rights reserved.

        Microsoft, MS-DOS, MS, Windows, Windows NT, MSDN, Active Directory, BizTalk, SQL Server, SharePoint,
        Outlook, PowerPoint, FrontPage, Visual Basic, Visual C++, Visual J++, Visual InterDev, Visual
        SourceSafe, Visual C#, Visual J#,  and Visual Studio are either registered trademarks or trademarks of
        Microsoft Corporation in the U.S.A. and/or other countries.

        Other product and company names herein may be the trademarks of their respective owners
    }
    
    introduction %{
        One of the top DLR features is common hosting support for all languages implemented on the DLR. The 
        primary goal is supporting .NET applications hosting the DLR languages for scripting support so that
        users can extend the basic functionality of the host application using any (DLR) language of their
        choice, irrespective of the programming language used to implement the host appplication.
    }

    section "Hosting" do
        introduction %{
            A quick survey of functionality includes:
            * Create ScriptRuntimes locally or in remote app domains.
            * Execute snippets of code.
            * Execute files of code in their own execution context (ScriptScope).
            * Explicitly choose language engines to use or just execute files to let the DLR find the right engine.
            * Create scopes privately or publicly for executing code in.
            * Create scopes, set variables in the scope to provide host object models, and publish the scopes for dynamic languages to import, require, etc.
            * Create scopes, set variables to provide object models, and execute code within the scopes.
            * Fetch dynamic objects and functions from scopes bound to names or execute expressions that return objects.
            * Call dynamic functions as host command implementations or event handlers.
            * Get reflection information for object members, parameter information, and documentation.
            * Control how files are resolved when dynamic languages import other files of code.
    
            Hosts always start by calling statically on the ScriptRuntime to create a ScriptRuntime. In the 
            simplest case, the host can set globals and execute files that access the globals.  In more advanced 
            scenarios, hosts can fully control language engines, get services from them, work with compiled code, 
            explicitly execute code in specific scopes, interact in rich ways with dynamic objects from the 
            ScriptRuntime, and so on.

            A detailed specification of the hosting APIs is available at 
            http://www.codeplex.com/dlr/Wiki/View.aspx?title=Docs%20and%20specs.
        }
        
        chapter "Getting started" do

            task(:body => %{
                    We first need to create a language "engine" using the <tt>IronRuby.create_engine</tt>
                    method. This name is available only when doing hosting from IronRuby _itself_. If you
                    are using a different language to implement the host, the method name would be
                    <tt>IronRuby.Ruby.CreateEngine</tt>. You will also have to make sure to add a 
                    reference to IronRuby.dll. This is done using the <tt>/r</tt> command-line compiler
                    option for C# and VB.Net, or using the <tt>clr.AddReference</tt> method from IronPython.
                },
                :setup => lambda do |bind|
                    eval "engine = '(undefined)'", bind
                    eval "def script() 'undefined' end", bind
                end,
                :code => 'engine = IronRuby.create_engine',
                :test_hook => lambda { |type, spec, bind| spec.assert_equal(IronRuby.to_clr_type.full_name, "IronRuby.Ruby") if type == :before }
              ) { |iar| iar.bind.engine.redirect_output; true }
                
            task :body => %{
                    Now let's execute some code.
                },
                :code => "engine.execute '$x = 100'"

            task :body => %{
                    What did that do? Let's read back the value of <tt>$x</tt> to make sure it is set
                    as expected.
                },
                :code => "engine.execute '$x'"
                
            task(:body => %{
                    We can verify that the code ran in a separate context by checking if <tt>$x</tt> exists 
                    in the current context.
                },
                :code => 'puts $x'
                ) { |iar| iar.output.chomp == eval('$x', iar.bind).inspect }
                
            task(:body => %{
                    Since typing <tt>engine.execute...</tt> gets verbose, let's define a method called
                    +script+ to encapsulate it. The name also represents the fact that the String parameter
                    it expects is conceptually arbitrary script code that the user can type. 
                    
                    Since Ruby methods cannot access outer local variables, we will need to store +engine+ 
                    as a global variable first.
                },
                :code => [
                    '$engine = engine',
                    'def script(script_code) $engine.execute(script_code) end']
                ) { |iar| iar.bind.script('$x') == 100 }
                
            task(:body => %{
                    We will use +script+ throughout the rest of the tutorial. Let's try it now to print the 
                    value of +$x+.
                },
                :code => "script 'puts $x'"
                ) { |iar| iar.output.chomp == '100' }
                
            task :body => %{
                    We can also get the value of a global constant like +Object+. This will be displayed
                    as <tt>Object#2</tt> to indicate that it belongs to a different ScriptEngine.
                },
                :code => "script 'Object'"
        end

        chapter "Global variables" do
            introduction %{
                Running user script code gets more interesting if the host application can set variables
                that the user code can use. The variables will typically be set to the object model of the
                host application. The tutorial application you are using stores the tutorials as
                <tt>Tutorial::Tutorial.all</tt>. We will use this object model in this chapter.
            }
                
            HostingTutorial.ensure_script_method

            task(:body => %{
                    Let's set a Ruby global variable called <tt>Tutorials</tt>. The name should be a valid
                    constant name (ie. it should begin with an upper case letter)
                },
                :code => "engine.runtime.globals.set_variable 'Tutorials', ::Tutorial.all"
                ) { |iar| iar.bind.engine.runtime.globals.get_variable('Tutorials') == Tutorial.all }
                
            task(:body => %{
                    Now the user's script code has access to it! Let's have the user check how many tutorials there are.
                },
                :code => "script 'Tutorials.size'"
                ) { |iar| iar.result == Tutorial.all.size }
                
            task(:body => %{
                    This works in the reverse direction too. The script code can set global variables that
                    the host application can read back out.
                },
                :code => [
                    "script 'ThisIsScriptCode = 200'",
                    "engine.runtime.globals.get_variable('ThisIsScriptCode')"]
                ) { |iar| iar.result == 200 and iar.input =~ /get_variable/ }
        end

        chapter "Scopes" do
            introduction %{
                Creating a ScriptEngine provides isolation of the host application and user script
                code. However, it is often useful to have isolation within the ScriptEngine. For
                example, multiple scripts might happen to use the same variable name. Normally,
                local variables defined in different .rb files are isolated from each other.
                The same effect can be achieved by creating multiple instances of +ScriptScope+,
                which more or less corresponds to a .rb file.
            }
            
            HostingTutorial.ensure_script_method
                    
            task(:body => %{
                    A scope is created using the +create_scope+ method.
                },
                :code => 'scope1 = engine.create_scope'
                ) { |iar| iar.bind.scope1 }
                
            task :body => %{
                    We can execute code in the scope by using an overload of the +execute+ method
                    that accepts a scope.
                },
                :code => "engine.execute '2+2', scope1"
                
            task(:body => %{
                    Now that we know how to create a scope, let's create a second one. We will also
                    set local variables with the same name in each of the scope, but initialize them
                    to different values.
                },
                :code => [
                    'scope2 = engine.create_scope',
                    "engine.execute 'x = 101', scope1",
                    "engine.execute 'x = 102', scope2"]
                ) { |iar| iar.bind.engine.execute('x', iar.bind.scope2) == 102 }
                
            task :body => %{
                    We can now verify that the two scopes are independent by inspecting the local
                    variable in each of them.
                },
                :code => [
                    "engine.execute 'x', scope1",
                    "engine.execute 'x', scope2"]
                
            task :body => %{
                    As a final step, we will make sure that the two scopes do share the same set of
                    global constants.
                },
                :code => "engine.execute('Object', scope1) == engine.execute('Object', scope2)"                
        end

        chapter "Per-scopes variables" do
            introduction %{
                Now that we know how to create multiple scopes, we can use per-scope local variables,
                instead of global variables, for values that are specific to each scope. For example,
                for a tutorial defined in <tt>name_tutorial.rb</tt>, you might want to load files in a folder
                called +name_scripts+, and set +tutorial+ to point to the tutorial created by 
                <tt>name_tutorial.rb</tt>.
            }
            
            HostingTutorial.ensure_script_method
                    
            task(:body => %{
                    Let's create two scopes
                },
                :code => [
                    'scope1 = $engine.create_scope',
                    'scope2 = $engine.create_scope']
                ) { |iar| iar.bind.scope1 and iar.bind.scope2 }
                
            task(:body => %{
                    Now we will set a variable named +tutorial+ in each of the scopes.
                },
                :code => [
                    "scope1.set_variable 'tutorial', ::Tutorial.all.values[0]",
                    "scope2.set_variable 'tutorial', ::Tutorial.all.values[1]"]
                ) { |iar| iar.bind.scope2.get_variable 'tutorial' }
                
            task :body => %{
                    Now we can execute the same script code in the two scopes.
                },
                :code => [
                    "$engine.execute 'tutorial.name', scope1",
                    "$engine.execute 'tutorial.name', scope2"]
        end
    end
    
    section "Hello IronPython!" do
    
        introduction %{
            So far, we have hosted IronRuby within IronRuby. Now we will host IronPython from IronRuby
            to show that it really is easy to host multiple languages, and the host application
            does not have to change much to accomodate multiple languages.
        }
        
        chapter "Hello IronPython!" do
        
            task(:body => %{
                    You will first need to make sure that you have IronPython installed.
                    Note that you have to use the specific release of IronPython that matches
                    the version of IronRuby you are using. If you use unmatched versions, then
                    you will not be able to host IronPython. The hosting APIs are defined
                    in Microsoft.Scripting.dll, and all language you try to host have to use
                    the exact same version of the assembly. 
                    
                    Let's first confirm that you can atleast load IronPython.                    
                 },
                 :code => "load_assembly 'IronPython'"
                ) { |iar| eval('IronPython::Hosting::Python', iar.bind).kind_of? Class }

            task(:body => %{
                    Now let's include the Hosting namespace.
                 },
                 :code => [
                    "load_assembly 'Microsoft.Scripting'",
                    "include Microsoft::Scripting::Hosting"]
                ) { |iar| iar.bind.Object.constants.include? "ScriptRuntime" }

            task(:body => %{
                    To be able to host a language, you need to have a config file. This tutorial
                    runs using ir.exe, which normally includes a config file ir.exe.config with
                    a section like this (taken from the IronRuby 0.6 release).
                    
                      <microsoft.scripting>
                        <languages>
                          <language names="IronPython;Python;py" extensions=".py" displayName="IronPython 2.6 Alpha" type="IronPython.Runtime.PythonContext, IronPython, Version=2.6.0.10, Culture=neutral, PublicKeyToken=null" />
                          <language names="IronRuby;Ruby;rb" extensions=".rb" displayName="IronRuby" type="IronRuby.Runtime.RubyContext, IronRuby, Version=0.6.0.0, Culture=neutral, PublicKeyToken=null" />
                        </languages>
                    
                        <options>
                          <set language="Ruby" option="LibraryPaths" value="..\..\Languages\Ruby\libs\;..\..\External.LCA_RESTRICTED\Languages\Ruby\redist-libs\ruby\site_ruby\1.8\;..\..\External.LCA_RESTRICTED\Languages\Ruby\redist-libs\ruby\1.8\" />
                        </options>
                      </microsoft.scripting>
                    
                    If you do hosting from another application, you will need to use a similar
                    config file.
                 },
                 :silverlight => false,
                 :code => [
                    "setup = ScriptRuntimeSetup.read_configuration",
                    "runtime = ScriptRuntime.new setup"]
                ) { |iar| iar.bind.runtime }

            task(:body => %{
                    To be able to host a language in Silverlight, you need to include the language assemblies 
                    in the XAP file and then create a +ScriptRuntime+ configured with information about 
                    that language.
                 },
                :silverlight => true,
                :code => [
                    'ls = IronPython::Hosting::Python.create_language_setup nil',
                    'srs = ScriptRuntimeSetup.new',
                    'srs.language_setups.add ls',
                    'runtime = ScriptRuntime.new srs']
                ) { |iar| iar.bind.runtime }

            task(:body => %{
                    Now create the Python engine. This step will fail if you are using a mismatched
                    version of IronPython.
                 },
                 :code => "python = runtime.get_engine 'Python'"
                ) { |iar| iar.bind.python.redirect_output; true }

            task :body => %{
                    Now you can run Python code! We will call the builtin function +dir+ to get the
                    list of all methods of the +str+ type.
                 },
                 :code => "python.execute 'dir(str)'"
        end
    end
   
    summary %{
        Congratulations! You have completed the Hosting tutorial. 
           
        For more information about the DLR and the DLR Hosting APIs, please visit http://www.codeplex.com/dlr/.
    }
end

