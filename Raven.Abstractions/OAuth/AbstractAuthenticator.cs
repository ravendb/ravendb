using System;
using Raven.Abstractions.Connection;
#if !SILVERLIGHT && !NETFX_CORE
using System.Net;
#endif
using System.Net.Http;
using System.Net.Http.Headers;

namespace Raven.Abstractions.OAuth
{
	public abstract class AbstractAuthenticator
	{
		protected readonly string ApiKey;
		protected string CurrentOauthToken { get; set; }

		protected AbstractAuthenticator(string apiKey)
		{
			ApiKey = apiKey;
		}

		public virtual void ConfigureRequest(object sender, WebRequestEventArgs e)
		{
			if (string.IsNullOrEmpty(CurrentOauthToken))
				return;

#if NETFX_CORE || SILVERLIGHT
			SetAuthorization(e.Client);
#else
			SetHeader(e.Request.Headers, "Authorization", "Bearer " + CurrentOauthToken);
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

		public abstract Action<HttpWebRequest> DoOAuthRequest(string oauthSource);
#endif
	}
}