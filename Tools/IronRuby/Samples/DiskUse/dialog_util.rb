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

include System
include System::Windows

module DialogUtil
  def load_xaml(filename)
    f = IO::FileStream.new(filename, IO::FileMode.Open, IO::FileAccess.Read)
    begin
      element = Markup::XamlReader::Load(f)
    ensure
      f.close
    end
    element
  end

  module_function :load_xaml
end

class InfoPopup
  def initialize
    @window = Windows::Window.new
    @window.window_style = Windows::WindowStyle.None
    @window.width = 300
    @window.height = 55
    @window.show_in_taskbar = false
    @window.topmost = true

    t = Controls::TextBlock.new
    t.padding = Windows::Thickness.new(5,5,5,5)
    t.text_wrapping = Windows::TextWrapping.NoWrap
    t.background = Media::Brushes.LightYellow
    @window.content = t
  end

  def set_position(x,y)  
    @window.left = x
    @window.top = y
  end

  def show
    @window.show
  end

  def hide
    @window.hide
  end

  def text=(text)
    @window.content.text = text
  end

  def clear_text
    @window.content.inlines.clear
  end

  def add_text(text)
    @window.content.inlines.add(Documents::Run.new(text))
  end

  def add_bold_text(text)
    @window.content.inlines.add(Documents::Bold.new(Documents::Run.new(text)))
  end
end
