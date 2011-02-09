namespace Raven.Studio.Controls
{
	using System;
	using System.Windows;

	public partial class ErrorWindow
	{
		public ErrorWindow(Exception e)
		{
			InitializeComponent();
			if (e != null)
			{
				ErrorTextBox.Text = e.Message + Environment.NewLine + Environment.NewLine + e.StackTrace;
			}
		}

		public ErrorWindow(Uri uri)
		{
			InitializeComponent();
			if (uri != null)
			{
				ErrorTextBox.Text = "Page not found: \"" + uri + "\"";
			}
		}

		public ErrorWindow(string message, string details)
		{
			InitializeComponent();
			ErrorTextBox.Text = message + Environment.NewLine + Environment.NewLine + details;
		}

		void OkButtonClick(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}
	}
}