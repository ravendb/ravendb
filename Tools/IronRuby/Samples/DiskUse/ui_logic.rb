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

require 'file_struct'
require 'renderers'
require 'dialog_util'

include System::Windows

class IronDiskUsage
  attr_reader :window, :fs_root, :renderer
  attr_accessor :selected_file
  def initialize(app)
     @app = app
     @window = DialogUtil.load_xaml("mainWindow.xaml")
     @window.closing { @app.shutdown }
     @windowTitle = @window.title

     @fl_keeper = FileListKeeper.new(self)

     usage_canvas = @window.find_name('usageCanvas')
     @renderer = VerticalRectanglesRenderer.new(usage_canvas)
     usage_canvas.size_changed {@renderer.update_dimensions}

     @selected_file = nil
     @window.show

     unless load_root
       @app.shutdown
     else
       @fl_keeper.expand_root
     end
  end

  def load_root
    folder_dialog = Forms::FolderBrowserDialog.new
    folder_dialog.show_new_folder_button = false
    folder_dialog.selected_path = "C:\\"
    folder_dialog.description = "Please select the folder to analyse"

    return false if folder_dialog.show_dialog != Forms::DialogResult.OK

    @fs_root = DiskDir.new(folder_dialog.selected_path, nil, nil)
    @fl_keeper.refresh(@fs_root)
    true
  end

  def loading=(val)
    @window.title = @windowTitle + (val ? "...loading" : "")
  end
end

class FileListKeeper
  def initialize(owner)
    @owner = owner
    @control = owner.window.find_name('fileList')
  end

  def refresh(root_dir)
    @control.items.clear
    add_item(@control.items, @owner.fs_root)
    @control.items[0].expanded.add method(:on_expanded)
  end

  def expand_root
    @control.items[0].is_expanded = true
  end

  def gen_items(file_item)
    file_item.tag.files.each {|disk_file| add_item(file_item.items, disk_file)}
  end

  def on_expanded(sender, e)
    tv_item = e.source
    if tv_item.tag.is_a? DiskDir
      @owner.loading = true

      tv_item.items.clear
      tv_item.tag.load
      gen_items(tv_item)

      @owner.selected_file = tv_item.tag
      @owner.renderer.update_files(tv_item.tag)

      @owner.loading = false
    end
  end
  
  def add_item(collection, disk_file)
    item = Controls::TreeViewItem.new
    item.header = make_item_header(disk_file)
    item.tag = disk_file
    if disk_file.is_a? DiskDir
      item.items.add('dummy')
      item.font_weight = System::Windows::FontWeights.Bold
    else
      item.font_weight = System::Windows::FontWeights.Normal
    end
    collection.add(item)
  end

  def make_item_header(disk_file)
    # RectangleGeometry
    r = Media::RectangleGeometry.new
    r.rect= Windows::Rect.new(0,0,15,15)
    r.radius_x = 3
    r.radius_y = 3

    # inside a GeometryDrawing (and keep it in the DiskFile)
    g = Media::GeometryDrawing.new
    g.geometry = r
    g.pen = Media::Pen.new
    g.pen.brush = Media::Brushes.Black
    #We will set the brush on render
    disk_file.drawing = g

    # Inside a DrawingImage
    d = Media::DrawingImage.new
    d.drawing = g

    # Inside an Image
    i = Controls::Image.new
    i.source = d

    #Inside a DockPanel with the name TextBlock beside it
    dock = Controls::DockPanel.new
    dock.children.add(i)
    t = Controls::TextBlock.new
    t.text = ' ' + disk_file.name
    dock.children.add(t)
    dock
  end
  private :make_item_header
end
