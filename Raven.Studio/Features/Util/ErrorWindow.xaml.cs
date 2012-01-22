using System;
using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Util
{
	public partial class ErrorWindow : ChildWindow
	{
		public ErrorWindow(string message, string details)
		{
			InitializeComponent();
			ErrorTextBox.Text = string.Format("{1}{0}{0}{2}{0}{0}-- Additional Information --{0}Uri: {3}", Environment.NewLine, message, details, UrlUtil.Url);
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}