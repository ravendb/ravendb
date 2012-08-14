using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class NewDatabase : ChildWindow
	{
		public NewDatabase()
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

		private void CheckBox_Checked(object sender, RoutedEventArgs e)
		{
			this.AdvancedSettings.Visibility = Visibility.Visible;
		}

		private void CheckBox_Unchecked(object sender, RoutedEventArgs e)
		{
			this.AdvancedSettings.Visibility = Visibility.Collapsed;
		}

		private void Toggle(object sender, RoutedEventArgs e)
		{
			var textblock = sender as TextBlock;
			if (textblock == null)
				return;

			var checkbox = this.FindName("Show" + textblock.Text.Split(null)[0]) as CheckBox;
			if (checkbox == null)
				return;
			checkbox.IsChecked = !checkbox.IsChecked;
		}
	}
}