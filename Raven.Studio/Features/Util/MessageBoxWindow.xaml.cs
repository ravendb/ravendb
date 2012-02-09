using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Features.Util
{
	public partial class MessageBoxWindow : ChildWindow
	{
		public MessageBoxWindow(string title, string message)
		{
			InitializeComponent();
			Title = title;
			MessageTextBox.Text = message;
			KeyBoard.Register(this);
			KeyBoard.IsCtrlHold = false;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}