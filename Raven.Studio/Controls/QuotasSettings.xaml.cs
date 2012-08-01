using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class QuotasSettings : ChildWindow
	{
		public QuotasSettings()
		{
			InitializeComponent();
			MaxSize.Value = 50;
			WarnSize.Value = 45;
			MaxDocs.Value = 10000;
			WarnDocs.Value = 8000;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}
	}
}

