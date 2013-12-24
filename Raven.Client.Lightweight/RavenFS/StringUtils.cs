#if SILVERLIGHT
using System.Net.Browser;
#else
using System.Web;
#endif

namespace Raven.Client.RavenFS
{
	public static class StringUtils
	{
		public static string UrlEncode(string textToEncode)
		{
#if SILVERLIGHT
	        return Uri.EscapeUriString(textToEncode);
#else
			return HttpUtility.UrlEncode(textToEncode);
#endif
		}

		public static string RemoveTrailingSlashAndEncode(string url)
		{
			while (url.EndsWith("/"))
				url = url.Substring(0, url.Length - 1);

			return UrlEncode(url);
		}
	}
}