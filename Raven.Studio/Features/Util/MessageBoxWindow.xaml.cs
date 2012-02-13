using System.Windows;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Util
{
	public partial class MessageBoxWindow : PopupWindow
	{
		public MessageBoxWindow(string title, string message)
		{
			InitializeComponent();
			Title = title;
			MessageTextBox.Text = message;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}