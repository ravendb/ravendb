using System.Windows;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Util
{
	public partial class ErrorWindow : PopupWindow
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