using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Features.Util
{
	public partial class ErrorWindow : ChildWindow
	{
		public ErrorWindow(string text)
		{
			InitializeComponent();
			ErrorTextBox.Text = text;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}