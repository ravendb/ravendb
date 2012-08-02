using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using Raven.Bundles.Versioning.Data;

namespace Raven.Studio.Controls
{
	public partial class VersioningSettings : ChildWindow
	{
		public ObservableCollection<VersioningConfiguration> VersioningData { get; set; } 

		public VersioningSettings()
		{
			InitializeComponent();
			DataContext = this;
			VersioningData = new ObservableCollection<VersioningConfiguration>();

		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}
		private void AddVersioning(object sender, RoutedEventArgs e)
		{
			VersioningData.Add(new VersioningConfiguration());
		}
	}
}

