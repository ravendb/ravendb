using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

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
			DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = false;
		}

		private void LayoutRoot_OnKeyDown(object sender, KeyEventArgs e)
		{
			switch (e.Key)
			{
				case Key.Enter:
					OKButton_Click(this, e);
					break;
				case Key.Escape:
					CancelButton_Click(sender, e);
					break;
			}
		}
	}
}