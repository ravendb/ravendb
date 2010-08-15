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

require 'dialog_util'
require 'file_struct'
include System
include System::Windows

class VerticalRectanglesRenderer
  def initialize(canvas)
    @popup = InfoPopup.new
    @brush_files = []
    @color_props = nil
    @canvas = canvas
    @canvas.mouse_enter {|s,e| @popup.show}
    @canvas.mouse_leave {|s,e| @popup.hide}
    @canvas.mouse_move do |s,e| 
      pos = Windows::Forms::Cursor.position
      @popup.set_position(pos.x + 15, pos.y + 20)
    end
  end

  def update_dimensions
    start_x = @canvas.actual_width / 8.0
    width = start_x * 6.0

    start_y = @canvas.actual_height / 20.0
    end_y = start_y * 19.0
    g_height = end_y - start_y

    last_y = start_y
    @canvas.children.each do |shape|
      r_height = shape.tag.size / shape.tag.parent.size * g_height
      shape.data.rect = Windows::Rect.new(start_x, last_y, width, r_height)

      last_y += r_height
    end
  end

  def update_files(disk_dir)
    start_x = @canvas.actual_width / 8.0
    width = start_x * 6.0

    start_y = @canvas.actual_height / 20.0
    end_y = start_y * 19.0
    g_height = end_y - start_y

    @brush_files.each do |f|
      f.drawing.brush = nil
    end
    @brush_files = []

    last_y = start_y
    @canvas.children.clear
    disk_dir.files.each do |f|
      p = Shapes::Path.new
      p.fill = get_random_brush
      p.stroke = Media::Brushes.Black
      p.stroke_thickness = 1.25
      @canvas.children.add(p)

      f.drawing.brush = p.fill
      @brush_files << f

      r = Media::RectangleGeometry.new
      r.radius_x = 2.5
      r.radius_y = 2.5

      r_height = f.size.to_f / disk_dir.size * g_height
      r.rect = Windows::Rect.new(start_x, last_y, width, r_height)

      last_y += r_height
      p.data = r
      p.mouse_enter.add method(:on_rect_mouse_enter)
      
      p.tag = f
    end
  end

  def on_rect_mouse_enter(sender, e)
    file_name = e.source.tag.name
    file_size = e.source.tag.size / 1048576.0
    parent_size = e.source.tag.parent.size / 1048576.0
    percent = 100.0 * file_size / parent_size
    if e.source.tag.is_a? DiskDir
      dir_tag = ' \t[dir]'
    else
      dir_tag = ''
    end

    @popup.clear_text
    @popup.add_bold_text(file_name)
    @popup.add_text("#{dir_tag}s\n#{"%0.2f" % file_size} MiB out of #{"%0.2f" % parent_size} MiB\n#{"%0.2f" % percent} % of parent directory")
  end

  def get_random_brush
    @color_props ||= Media::Brushes.to_clr_type.get_properties.to_a
    @color_props[rand(@color_props.length)].get_value(nil, nil)
  end
end
