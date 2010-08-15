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

require 'WindowsBase'
require 'PresentationCore'
require 'PresentationFramework'
require 'System.Windows.Forms'
require 'ui_logic'

def start
  app = Windows::Application.new
  idu = IronDiskUsage.new(app)
  app.run
end

def app_start
  app = System::Windows::Application.new
  idu = IronDiskUsage.new(app)
  $dispatcher = System::Threading::Dispatcher.from_thread(System::Threading::Thread.current_thread)
  $are.set
  app.run
end

def start_interactive
  raise "start_interactive doesn't work yet"
  $are = System::Threading::AutoResetEvent.new(false)
  
  t = Thread.new do
    app_start
  end
  $are.wait_one
end

if __FILE__ == $0
  start
else
  start_interactive
end
