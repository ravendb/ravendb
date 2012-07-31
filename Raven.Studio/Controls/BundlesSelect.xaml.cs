using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Controls
{
	public partial class BundlesSelect : ChildWindow
	{
		public List<string> Bundles { get; private set; }
		public List<VersioningData> VersioningData { get; set; } 
		public BundlesSelect()
		{
			InitializeComponent();
			DataContext = this;
			Bundles = new List<string>();
			VersioningData = new List<VersioningData>();
			VersioningData.Add(new VersioningData
			{
				Exlude = true,
				Id = "test1"
			});
			VersioningData.Add(new VersioningData
			{
				Exlude = false,
				Id = "test2"
			});
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}

		private void Checked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if(checkbox == null)
				return;
			Bundles.Add(checkbox.Name);

			var grid = FindName(checkbox.Name + "Settings") as Grid;
			if (grid != null)
			{
				grid.Visibility = Visibility.Visible;
			}

		}

		private void UnChecked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if (checkbox == null)
				return;
			Bundles.Remove(checkbox.Name);

			var grid = FindName(checkbox.Name + "Settings") as Grid;
			if (grid != null)
			{
				grid.Visibility = Visibility.Collapsed;
			}
		}
	}

	public class VersioningData
	{
		public bool Exlude { get; set; }
		public string Id { get; set; }
	}
}

