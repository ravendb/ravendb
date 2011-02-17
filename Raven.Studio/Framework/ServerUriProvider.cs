namespace Raven.Studio.Framework
{
	using System.ComponentModel.Composition;
	using System.IO.IsolatedStorage;
	using System.Windows;
	using System.Windows.Browser;

	[Export]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class ServerUriProvider
	{
		string cached;

		public string GetServerUri()
		{
			if(string.IsNullOrEmpty(cached)) cached = DetermineUri();
			return cached;
		}

		static string DetermineUri()
		{
			string uri;
			const string uriLookup = "Uri";

			if (IsolatedStorageSettings.ApplicationSettings.TryGetValue(uriLookup, out uri))
				return uri;

			if (Application.Current.IsRunningOutOfBrowser)
			{
#if DEBUG
				uri = "http://localhost:8080";
#endif
			}
			else if (HtmlPage.Document.DocumentUri.Scheme == "file")
			{
				uri = "http://localhost:8080";
			} else 
			{
			

				uri = string.Format("{0}://{1}:{2}",
				                    HtmlPage.Document.DocumentUri.Scheme,
				                    HtmlPage.Document.DocumentUri.Host,
				                    HtmlPage.Document.DocumentUri.Port
					);
			}

#if !DEBUG
            IsolatedStorageSettings.ApplicationSettings[uriLookup] = uri;
            IsolatedStorageSettings.ApplicationSettings.Save();
#endif

			return uri;
		}
	}
}