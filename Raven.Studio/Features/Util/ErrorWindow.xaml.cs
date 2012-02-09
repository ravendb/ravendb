using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Features.Util
{
	public partial class ErrorWindow : ChildWindow
	{
		public ErrorWindow(string text)
		{
			InitializeComponent();
			ErrorTextBox.Text = text;
			KeyBoard.Register(this);
			KeyBoard.IsCtrlHold = false;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}