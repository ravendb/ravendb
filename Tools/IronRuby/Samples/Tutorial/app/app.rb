include Microsoft::Scripting::Silverlight
include System::Windows::Browser

if HtmlPage.is_enabled && document.query_string.contains_key('console')
  repl = Repl.show('ruby')
  $stdout = repl.output_buffer
  $stderr = repl.output_buffer
  puts '> console opened'
end

def load_assembly_from_path(path)
  DynamicApplication.current.runtime.host.platform_adaptation_layer.
  load_assembly_from_path(path)
end

# From Silverlight 3 SDK
load_assembly_from_path "System.Windows.Controls.dll"
require 'System.Windows.Controls'

# From Silverlight 3 Toolkit
load_assembly_from_path "System.Windows.Controls.Toolkit.dll"
require 'System.Windows.Controls.Toolkit'

require 'gui_tutorial'
Application.Current.RootVisual = GuiTutorial::Window.current

