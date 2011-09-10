using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Features.Input
{
	public partial class InputWindow : ChildWindow
	{
		public InputWindow()
		{
			InitializeComponent();
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}
	}
}

