using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
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

		public void Refresh()
		{
		   ContentFrame.Refresh();
		}


		private void ContentFrame_Navigated(object sender, NavigationEventArgs e)
		{
			HighlightCurrentPage(e.Uri);

			// update the current database here so that back button navigation works correctly with database changes
			ApplicationModel.Current.Server.Value.SetCurrentDatabase(new UrlParser(e.Uri.OriginalString));

			GC.Collect();
		}

		// After the Frame navigates, ensure the HyperlinkButton representing the current page is selected
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
		}

		private static bool HyperlinkMatchesUri(string uri, HyperlinkButton link)
		{
			var queryStart = uri.IndexOf('/', Math.Min(uri.Length, 1));
            if (queryStart < 0)
            {
                queryStart = uri.IndexOf('?');
            }

            if (queryStart > 0)
            {
                uri = uri.Substring(0, queryStart);
            }

			if (link.CommandParameter != null &&
				uri.Equals(link.CommandParameter.ToString(), StringComparison.InvariantCultureIgnoreCase))
			{
				return true;
			}

			var alternativeUris = LinkHighlighter.GetAlternativeUris(link);
			if (alternativeUris != null && alternativeUris.Any(alternative => uri.Equals(alternative, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			return false;
		}

		// If an error occurs during navigation, show an error window
		private void ContentFrame_NavigationFailed(object sender, NavigationFailedEventArgs e)
		{
			e.Handled = true;
			ApplicationModel.Current.AddErrorNotification(e.Exception, string.Format("Could not load page: {0}", e.Uri));
		}
	}
}
