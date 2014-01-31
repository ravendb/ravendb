using System;
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
				MediaTypeHeaderValue mediaTypeHeader;
				if (!MediaTypeHeaderValue.TryParse(contentType, out mediaTypeHeader)) //otherwise on constructor parameter such as 'application/json; charset=utf-8' will throw
					throw new ArgumentException("not recognized mime type in Content-Type header");	

				httpContent.Headers.ContentType = mediaTypeHeader;
			}

			return httpContent;
		}
	}
}