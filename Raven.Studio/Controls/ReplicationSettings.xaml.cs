using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using Raven.Abstractions.Replication;

namespace Raven.Studio.Controls
{
	public partial class ReplicationSettings : ChildWindow
	{
		public ObservableCollection<ReplicationDestination> Destinations { get; set; }
		public ReplicationSettings()
		{
			Destinations = new ObservableCollection<ReplicationDestination>();
			InitializeComponent();
			DataContext = this;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}

		private void AddDestination(object sender, RoutedEventArgs e)
		{
			Destinations.Add(new ReplicationDestination());
		}
	}
}

