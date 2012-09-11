using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace Raven.Studio.Features.Settings
{
    public partial class VersioningSettingsView : UserControl
    {
        public VersioningSettingsView()
        {
            InitializeComponent();
        }

	    private void AutoCompleteGotFocus(object sender, RoutedEventArgs e)
	    {
		    var autoCompleteBox = (AutoCompleteBox)sender;
		    var text = autoCompleteBox.Text ?? "";
			DependencyObject o = VisualTreeHelper.GetChild(autoCompleteBox, 0); 
			o = VisualTreeHelper.GetChild(o, 0); 
			((TextBox)(o)).Text = " ";
			((TextBox)(o)).Text = text;
	    }
    }
}
