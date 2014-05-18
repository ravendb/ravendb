using System.Web;

namespace Raven.Client.RavenFS
{
	public static class StringUtils
	{
		public static string UrlEncode(string textToEncode)
		{
			return HttpUtility.UrlEncode(textToEncode);
		}

		public static string RemoveTrailingSlashAndEncode(string url)
		{
			while (url.EndsWith("/"))
				url = url.Substring(0, url.Length - 1);

			return UrlEncode(url);
		}
	}
}