using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public partial class ShowEncryptionMessage : ChildWindow
	{
		private readonly string key;

		public ShowEncryptionMessage(string key)
		{
			InitializeComponent();
			this.key = key;
			KeyValue.Text = key;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = false;
		}

		private void KeyValue_TextChanged(object sender, TextChangedEventArgs e)
		{
			KeyValue.Text = key;
		}
	}
}

