using System;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Features.Errors
{
	public partial class ErrorWindow : ChildWindow
	{
		public ErrorWindow(string message, string details)
		{
			InitializeComponent();
			ErrorTextBox.Text = message + Environment.NewLine + Environment.NewLine + details;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}