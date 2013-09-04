using System;
using System.IO;
using System.Net;
using System.Text;

namespace Raven.Tests.Security.OAuth
{
	public static class HttpWebRequestExtensions
	{
		public static HttpWebResponse MakeRequest(this HttpWebRequest request)
		{
			try
			{
				return (HttpWebResponse)request.GetResponse();
			}
			catch (WebException we)
			{
				if (we.Response != null)
				{
					return (HttpWebResponse)we.Response;
				}
				throw;
			}
		}

		public static HttpWebRequest WithBasicCredentials(this HttpWebRequest request, string url, string username, string password)
		{
			var authInfo = username + ":" + password;
			authInfo = Convert.ToBase64String(Encoding.Default.GetBytes(authInfo));
			request.Headers["Authorization"] = "Basic " + authInfo;

			return request;
		}

		public static HttpWebRequest WithoutCredentials(this HttpWebRequest request)
		{
			request.Headers["Authorization"] = null;

			return request;
		}

		public static HttpWebRequest WithAccept(this HttpWebRequest request, string contentType)
		{
			request.Accept = contentType;

			return request;
		}

		public static HttpWebRequest WithHeader(this HttpWebRequest request, string key, string value)
		{
			request.Headers[key] = value;

			return request;
		}

		public static HttpWebRequest WithoutHeader(this HttpWebRequest request, string key)
		{
			request.Headers.Remove(key);

			return request;
		}

		public static string ReadToEnd(this HttpWebResponse response)
		{
			string content;

			using (var sr = new StreamReader(response.GetResponseStream()))
				content = sr.ReadToEnd();

			return content;
		}

		public static HttpWebRequest WithBearerTokenAuthorization(this HttpWebRequest request, string token)
		{
			request.Headers["Authorization"] = "Bearer " + token;

			return request;
		}
	}
}