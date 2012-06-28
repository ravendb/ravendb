using System;
using System.Windows;
using System.Windows.Browser;
using System.Windows.Input;

namespace Raven.Studio.Infrastructure
{
	public static class UrlUtil
	{
		static UrlUtil()
		{
			Url = Application.Current.Host.NavigationState;
			Application.Current.Host.NavigationStateChanged += (sender, args) => Url = args.NewNavigationState;
		}

		public static string Url { get; private set; }

		private static void Navigate(Uri source)
		{
			Application.Current.Host.NavigationState = source.ToString();
		}

        private static void Refresh()
        {
            (Application.Current.RootVisual as MainPage).Refresh();
        }

		public static void Navigate(string url, bool dontOpenNewTab = false, bool forceRefresh = false)
		{
			if (url == null)
				return;

			url = new UrlParser(url).BuildUrl();

			Execute.OnTheUI(() =>
			                	{
									if (Keyboard.Modifiers == ModifierKeys.Control && dontOpenNewTab == false)
			                		{
			                			OpenUrlOnANewTab(url);
										return;
			                		}

                                    if (Url == url && forceRefresh)
                                    {
                                        Refresh();
                                    }
                                    else
                                    {
                                        Navigate((new Uri(url, UriKind.Relative)));
                                    }
			                	});
		}

		private static void OpenUrlOnANewTab(string url)
		{
			var hostUrl = HtmlPage.Document.DocumentUri.OriginalString;
			var fregmentIndex = hostUrl.IndexOf('#');
			string host = fregmentIndex != -1 ? hostUrl.Substring(0, fregmentIndex) : hostUrl;

			HtmlPage.Window.Navigate(new Uri(host + "#" + url, UriKind.Absolute), "_blank");
		}
	}
}
