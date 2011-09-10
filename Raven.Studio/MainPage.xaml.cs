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
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Raven.Studio
{
	public partial class MainPage : UserControl
	{
		public MainPage()
		{
			InitializeComponent();
		}

		// After the Frame navigates, ensure the HyperlinkButton representing the current page is selected
		private void ContentFrame_Navigated(object sender, NavigationEventArgs e)
		{
			foreach (UIElement child in LinksStackPanel.Children)
			{
				HyperlinkButton hb = child as HyperlinkButton;
				if (hb != null && hb.NavigateUri != null)
				{
					if (hb.NavigateUri.ToString().Equals(e.Uri.ToString()))
					{
						VisualStateManager.GoToState(hb, "ActiveLink", true);
					}
					else
					{
						VisualStateManager.GoToState(hb, "InactiveLink", true);
					}
				}
			}
		}

		// If an error occurs during navigation, show an error window
		private void ContentFrame_NavigationFailed(object sender, NavigationFailedEventArgs e)
		{
			e.Handled = true;
			ChildWindow errorWin = new ErrorWindow(e.Uri);
			errorWin.Show();
		}
	}
}