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
			Execute.OnTheUI(() => Application.Current.Host.NavigationState = source.ToString());
		}

		public static void Navigate(string url)
		{
			if (url == null)
				return;

			if (Keyboard.Modifiers == ModifierKeys.Control)
			{
				var hostUrl = HtmlPage.Document.DocumentUri.OriginalString;
				var fregmentIndex = hostUrl.IndexOf('#');
				string host = fregmentIndex != -1 ? hostUrl.Substring(0, fregmentIndex + 1) : hostUrl;

				//Fix for issue with FireFox
				if (host[host.Length - 1] != '#')
					host += "#";

				HtmlPage.Window.Navigate(new Uri(host + url, UriKind.Absolute), "_blank");
				return;
			}

			url = new UrlParser(url).BuildUrl();
			Navigate((new Uri(url, UriKind.Relative)));
		}
	}
}