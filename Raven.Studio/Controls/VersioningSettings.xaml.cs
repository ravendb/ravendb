using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class VersioningSettings : ChildWindow
	{
		public ObservableCollection<VersioningData> VersioningData { get; set; } 

		public VersioningSettings()
		{
			InitializeComponent();
			DataContext = this;
			VersioningData = new ObservableCollection<VersioningData>();

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
			VersioningData.Add(new VersioningData());
		}
	}

	public class VersioningData
	{
		public bool Exlude { get; set; }
		public string Id { get; set; }
	}
}

