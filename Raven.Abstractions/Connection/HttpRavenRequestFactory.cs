#if !NETFX_CORE
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.OAuth;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequestFactory
	{
		public int? RequestTimeoutInMs { get; set; }

		readonly ConcurrentDictionary<Tuple<string, string>, AbstractAuthenticator> authenticators = new ConcurrentDictionary<Tuple<string, string>, AbstractAuthenticator>();

		public void ConfigureRequest(RavenConnectionStringOptions options, WebRequest request)
		{
			if (RequestTimeoutInMs.HasValue)
				request.Timeout = RequestTimeoutInMs.Value;

			if (options.ApiKey == null)
			{
				request.Credentials = options.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				return;
			}

			var webRequestEventArgs = new WebRequestEventArgs { Request = request, Credentials = new OperationCredentials(options.ApiKey, options.Credentials)};

			AbstractAuthenticator existingAuthenticator;
			if (authenticators.TryGetValue(GetCacheKey(options), out existingAuthenticator))
			{
				existingAuthenticator.ConfigureRequest(this, webRequestEventArgs);
			}
			else
			{
				var basicAuthenticator = new BasicAuthenticator(enableBasicAuthenticationOverUnsecuredHttp: false);
				var securedAuthenticator = new SecuredAuthenticator();

				basicAuthenticator.ConfigureRequest(this, webRequestEventArgs);
				securedAuthenticator.ConfigureRequest(this, webRequestEventArgs);
			}
		}

		private static Tuple<string, string> GetCacheKey(RavenConnectionStringOptions options)
		{
			return Tuple.Create(options.Url, options.ApiKey);
		}

		public HttpRavenRequest Create(string url, string method, RavenConnectionStringOptions connectionStringOptions)
		{
			return new HttpRavenRequest(url, method, ConfigureRequest, HandleUnauthorizedResponse, connectionStringOptions);
		}

		private Action<HttpWebRequest> HandleUnauthorizedResponse(RavenConnectionStringOptions options, WebResponse webResponse)
		{
			if (options.ApiKey == null)
				return null;

			var oauthSource = webResponse.Headers["OAuth-Source"];

			var useBasicAuthenticator =
				string.IsNullOrEmpty(oauthSource) == false &&
				oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false;

			if (string.IsNullOrEmpty(oauthSource))
				oauthSource = options.Url + "/OAuth/API-Key";

			var authenticator = authenticators.GetOrAdd(
				GetCacheKey(options),
				_ =>
				{
					if (useBasicAuthenticator)
					{
						return new BasicAuthenticator(enableBasicAuthenticationOverUnsecuredHttp: false);
					}

					return new SecuredAuthenticator();
				});

			return authenticator.DoOAuthRequest(oauthSource, options.ApiKey);
		}
	}
}
#endif