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

if $0 == __FILE__
  $LOAD_PATH << File.dirname(__FILE__)
  begin
    require("wpf/SplashScreen.dll")
    SplashScreen::SplashScreen.show
  rescue System::IO::FileNotFoundException => e
    abort %{
      The GUI tutorial requires .NET 3.5 SP1 which can be downloaded from
      http://go.microsoft.com/fwlink/?LinkId=122089. An online version of
      the tutorial is available at http://ironruby.net/Tutorial.
      Exception details: #{e}
    }.gsub(/\n/, "").gsub(/\s+/, ' ').strip
  end
end

begin
  require 'c:/dev/repl'
  alias debugger repl
rescue LoadError
  # get repl.rb: http://gist.github.com/116393
end

if ARGV.include?('TRACE')
  begin
    require 'c:/dev/trace'
  rescue LoadError
    # get trace.rb
  end
end

$:.unshift(File.dirname(__FILE__) + "/app")

require 'gui_tutorial'

module WpfTutorial
  class App < System::Windows::Application
    def self.current
      @current ||= App.new
    end

    def self.run(options = {})
      unless Application.Current
        if options[:explicit_shutdown]
          current.shutdown_mode = ShutdownMode.on_explicit_shutdown
        end
        current.run GuiTutorial::Window.current
      else
        App.Current.main_window = GuiTutorial::Window.current
        GuiTutorial::Window.current.show!
      end
    end

    def self.run_interactive(proc_obj)
      if Application.Current
        app_callback = System::Threading::ThreadStart.new { proc_obj.call rescue puts $! }
        Application.current.dispatcher.invoke(Threading::DispatcherPriority.Normal, app_callback)
      else
        warn "Setting explicit shutdown. Exit the process by calling 'unload'"
        # Run the app on another thread so that the interactive REPL can stay on the main thread
        Wpf.create_sta_thread { proc_obj.call rescue puts $! }
      end
    end
  end
end

if $0 == __FILE__
  WpfTutorial::App.run

elsif $0 == nil or $0 == "iirb"
  include System::Windows
  include System::Threading
  include System::Windows::Threading

  def reload
    load __FILE__
  end

  def unload
    Application.current.dispatcher.invoke(DispatcherPriority.normal, 
      ThreadStart.new{ Application.current.shutdown })
    exit
  end

  WpfTutorial::App.run_interactive lambda{ 
    WpfTutorial::App.run(:explicit_shutdown => true)
  }
end
