using System;
using System.Windows;

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

			url = new UrlParser(url).BuildUrl();
			Navigate((new Uri(url, UriKind.Relative)));
		}
	}
}