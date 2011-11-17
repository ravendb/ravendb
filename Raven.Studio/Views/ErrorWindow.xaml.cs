using System;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio
{
	public partial class ErrorWindow : ChildWindow
	{
		public ErrorWindow(Exception e)
			: this(e.Message, e.StackTrace)
		{
		}

		public ErrorWindow(Uri uri, Exception e)
			: this(string.Format("Could not load page: {0}. {2}Error Message: {1}", uri, e.Message, Environment.NewLine), e.StackTrace)
		{
		}

		public ErrorWindow(string message, string details)
		{
			InitializeComponent();
			ErrorTextBox.Text = message + Environment.NewLine + Environment.NewLine + details;
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			this.DialogResult = true;
		}
	}
}