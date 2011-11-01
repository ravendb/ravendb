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
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

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
			HighlightCurrentPage(e.Uri);
		}

		private void HighlightCurrentPage(Uri currentUri)
		{
			foreach (var hyperlink in MainLinks.Children.OfType<HyperlinkButton>())
			{
				if (HyperlinkMatchesUri(currentUri.ToString(), hyperlink))
				{
					VisualStateManager.GoToState(hyperlink, "ActiveLink", true);
				}
				else
				{
					VisualStateManager.GoToState(hyperlink, "InactiveLink", true);
				}
			}

			if (currentUri.ToString() == string.Empty)
			{
				VisualStateManager.GoToState(SummaryLink, "ActiveLink", true);
			}
		}

		private static bool HyperlinkMatchesUri(string uri, HyperlinkButton link)
		{
			if (link.NavigateUri != null && 
				uri.StartsWith(link.NavigateUri.ToString(), StringComparison.InvariantCultureIgnoreCase))
			{
				return true;
			}

			var alternativeUris = LinkHighlighter.GetAlternativeUris(link);
			if (alternativeUris != null && alternativeUris.Any(alternative => uri.StartsWith(alternative, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			return false;
		}

		// If an error occurs during navigation, show an error window
		private void ContentFrame_NavigationFailed(object sender, NavigationFailedEventArgs e)
		{
			e.Handled = true;
			ChildWindow errorWin = new ErrorWindow(e.Uri);
			errorWin.Show();
		}

		private NavigationMode navigationMode = NavigationMode.New;

		// EnsureDatabaseParameterIncluded
		private void ContentFrame_Navigating(object sender, NavigatingCancelEventArgs e)
		{
			if (navigationMode != NavigationMode.New) return;

			var currentDatabase = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			var urlParser = new UrlParser(e.Uri.ToString());
			if (urlParser.GetQueryParam("database") != null)
				return;

			e.Cancel = true;
			navigationMode = NavigationMode.Refresh;
			urlParser.SetQueryParam("database", currentDatabase);
			urlParser.NavigateTo();
			navigationMode = NavigationMode.New;
		}
	}
}