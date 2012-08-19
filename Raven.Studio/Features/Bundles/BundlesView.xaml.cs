using System.Windows;
using System.Windows.Controls;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Bundles
{
	public partial class BundlesView : PageView
	{
		public BundlesView()
		{
			InitializeComponent();
			MaxSize.Maximum = int.MaxValue;
			WarnSize.Maximum = int.MaxValue;
			MaxDocs.Maximum = int.MaxValue;
			WarnDocs.Maximum = int.MaxValue;
		}

		private void UseConnectionCombo_SelectionChanged(object sender, System.Windows.Controls.SelectionChangedEventArgs e)
		{
			var connectionCombo = sender as ComboBox;
			if (connectionCombo == null)
				return;
			
			var grid = connectionCombo.Parent as Grid;
			var children = grid.Children;

			if (connectionCombo.SelectedIndex == 0)
			{	
				foreach (var child in children)
				{
					if (child is TextBox)
					{
						switch ((child as TextBox).Name)
						{
							case "UrlText":
								child.Visibility = Visibility.Visible;
								break;
							case "ConnectionText":
								child.Visibility = Visibility.Collapsed;
								break;
						}
					}
					else if (child is Grid)
					{
						child.Visibility = Visibility.Visible;
					}
				}
			}
			else
			{
				foreach (var child in children)
				{
					if (child is TextBox)
					{
						switch ((child as TextBox).Name)
						{
							case "UrlText":
								child.Visibility = Visibility.Collapsed;
								break;
							case "ConnectionText":
								child.Visibility = Visibility.Visible;
								break;
						}
					}
					else if (child is Grid)
					{
						child.Visibility = Visibility.Collapsed;
					}
				}
			}

		}

		private void DeleteReplication(object sender, RoutedEventArgs e)
		{
			var bundleModel = this.DataContext as BaseBundlesModel;
			var button = sender as HyperlinkButton;
			if (bundleModel != null && button != null)
			{
				bundleModel.ReplicationDestinations.Remove(button.DataContext as ReplicationDestination);
			}
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			var parentWindow = this.Parent as ChildWindow;
			if (parentWindow == null)
				return;
			parentWindow.DialogResult = false;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			var parentWindow = this.Parent as ChildWindow;
			if (parentWindow == null)
				return;
			parentWindow.DialogResult = true;
		}

		private void DeleteVersioning(object sender, RoutedEventArgs e)
		{
			var bundleModel = this.DataContext as BaseBundlesModel;
			var button = sender as HyperlinkButton;
			if (bundleModel != null && button != null)
			{
				bundleModel.VersioningConfigurations.Remove(button.DataContext as VersioningConfiguration);
			}
		}
	}
}