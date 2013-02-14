using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class ResetReplication : ChildWindow
	{
		public List<string> HasChanges { get; set; }
		public List<string> Selected { get; set; } 

		public ResetReplication()
		{
			Selected = new List<string>();
			this.DataContext = this;
			InitializeComponent();
			
		}
		public ResetReplication(List<string> hasChanges) : this()
		{
			HasChanges = hasChanges;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void Checked(object sender, RoutedEventArgs e)
		{
			var checkBox = sender as CheckBox;
			if(checkBox == null)
				return;

			Selected.Add((string)checkBox.Content);
		}

		private void UnChecked(object sender, RoutedEventArgs e)
		{
			var checkBox = sender as CheckBox;
			if (checkBox == null)
				return;

			var item = (string)checkBox.Content;
			if (Selected.Contains(item))
				Selected.Remove(item);
		}
	}
}

