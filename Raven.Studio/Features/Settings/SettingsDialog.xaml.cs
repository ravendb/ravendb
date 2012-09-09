using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Features.Settings
{
    public partial class SettingsDialog : UserControl
    {
        public SettingsDialog()
		{
			InitializeComponent();

			KeyDown += (sender, args) =>
			{
				if(args.Key == Key.Enter)
				{
					SetDialogResult(true);
				}
			};
		}

		private void SetDialogResult(bool setToValue)
		{
			var parentWindow = Parent as ChildWindow;
			if (parentWindow == null)
				return;
			parentWindow.DialogResult = setToValue;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			SetDialogResult(false);
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			SetDialogResult(true);
		}
    }
}
