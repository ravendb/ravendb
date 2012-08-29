using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace Raven.Studio.Controls
{
	public partial class NewDatabase : ChildWindow
	{
		public List<string> Bundles { get; private set; }

		public NewDatabase()
		{
			InitializeComponent();
			DataContext = this;
			Bundles = new List<string>();
			KeyDown += (sender, args) =>
			{
				if (args.Key == Key.Escape)
					DialogResult = false;
			};
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

		private void DbName_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
		{
			if (e.Key == Key.Enter && !string.IsNullOrWhiteSpace(DbName.Text))
				DialogResult = true;
				
		}

		private void ToggleBundles(object sender, RoutedEventArgs e)
		{
			var textblock = sender as TextBlock;
			if (textblock == null)
				return;

			var checkbox = this.FindName(textblock.Text.Split(null)[0]) as CheckBox;
			if (checkbox == null)
				return;
			checkbox.IsChecked = !checkbox.IsChecked;
		}

		private void Checked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if (checkbox == null)
				return;
			Bundles.Add(checkbox.Name);

			var border = FindName(checkbox.Name + "Settings") as Border;
			if (border != null)
			{
				border.Visibility = Visibility.Visible;
			}
		}

		private void UnChecked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if (checkbox == null)
				return;
			Bundles.Remove(checkbox.Name);

			var border = FindName(checkbox.Name + "Settings") as Border;
			if (border != null)
			{
				border.Visibility = Visibility.Collapsed;
			}
		}
	}
}