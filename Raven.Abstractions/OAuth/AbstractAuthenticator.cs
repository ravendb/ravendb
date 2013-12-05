using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Abstractions.Connection;

namespace Raven.Abstractions.OAuth
{
	public abstract class AbstractAuthenticator
	{
		protected string CurrentOauthToken;

		public virtual void ConfigureRequest(object sender, WebRequestEventArgs e)
		{
			if (string.IsNullOrEmpty(CurrentOauthToken))
				return;

#if NETFX_CORE || SILVERLIGHT
			e.Client.DefaultRequestHeaders.Add("Authorization", CurrentOauthToken);
#else
			SetHeader(e.Request.Headers, "Authorization", CurrentOauthToken);
#endif
		}

		protected void SetAuthorization(HttpClient httpClient)
		{
			if (string.IsNullOrEmpty(CurrentOauthToken))
				return;

			try
			{
				httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", CurrentOauthToken);
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Could not set the Authorization to the value 'Bearer {0}'", CurrentOauthToken), ex);
			}
		}

#if !SILVERLIGHT && !NETFX_CORE
		protected static void SetHeader(WebHeaderCollection headers, string key, string value)
		{
			try
			{
				headers[key] = value;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not set '" + key + "' = '" + value + "'", e);
			}
		}

		public abstract Action<HttpWebRequest> DoOAuthRequest(string oauthSource, string apiKey);
#endif
	}
}