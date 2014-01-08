using System.Collections.Specialized;
using System.Net.Http;
using System.Net.Http.Headers;

namespace Raven.Client.Connection
{
	public static class HttpContentExtentions
	{
		public static HttpContent SetContentType(this HttpContent httpContent, NameValueCollection headers)
		{
			var contentType = headers["Content-Type"];

			if (contentType == null)
			{
				httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
			}
			else if(contentType != string.Empty)
			{
				httpContent.Headers.ContentType = new MediaTypeHeaderValue(contentType);
			}

			return httpContent;
		}
	}
}