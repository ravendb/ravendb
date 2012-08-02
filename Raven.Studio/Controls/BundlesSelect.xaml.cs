using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class BundlesSelect : ChildWindow
	{
		public List<string> Bundles { get; private set; }

		public BundlesSelect()
		{
			InitializeComponent();
			DataContext = this;
			Bundles = new List<string>();
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = false;
		}

		private void Checked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if(checkbox == null)
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

