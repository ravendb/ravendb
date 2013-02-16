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

		readonly ConcurrentDictionary<string, AbstractAuthenticator> authenticators = new ConcurrentDictionary<string, AbstractAuthenticator>();

		public void ConfigureRequest(RavenConnectionStringOptions options, WebRequest request)
		{
			if (RequestTimeoutInMs.HasValue)
				request.Timeout = RequestTimeoutInMs.Value;

			if (options.ApiKey == null)
			{
				request.Credentials = options.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				return;
			}

			var webRequestEventArgs = new WebRequestEventArgs { Request = request };

			AbstractAuthenticator existingAuthenticator;
			if (authenticators.TryGetValue(options.ApiKey, out existingAuthenticator))
			{
				existingAuthenticator.ConfigureRequest(this, webRequestEventArgs);
			}
			else
			{
				// TODO: Not sure where to get this or if it's needed
				bool enableBasicAuthenticationOverUnsecuredHttp = false;
				var basicAuthenticator = new BasicAuthenticator(options.ApiKey, enableBasicAuthenticationOverUnsecuredHttp);
				var securedAuthenticator = new SecuredAuthenticator(options.ApiKey);

				basicAuthenticator.ConfigureRequest(this, webRequestEventArgs);
				securedAuthenticator.ConfigureRequest(this, webRequestEventArgs);
			}
		}

		public HttpRavenRequest Create(string url, string method, RavenConnectionStringOptions connectionStringOptions)
		{
			return new HttpRavenRequest(url, method, ConfigureRequest, HandleUnauthorizedResponse, connectionStringOptions);
		}

		private bool HandleUnauthorizedResponse(RavenConnectionStringOptions options, WebResponse webResponse)
		{
			if (options.ApiKey == null)
				return false;

			var oauthSource = webResponse.Headers["OAuth-Source"];

			var useBasicAuthenticator =
				string.IsNullOrEmpty(oauthSource) == false &&
				oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false;

			var authenticator = authenticators.GetOrAdd(
				options.ApiKey,
				apiKey =>
				{
					if (useBasicAuthenticator)
					{
						// TODO: Not sure where to get this or if it's needed
						bool enableBasicAuthenticationOverUnsecuredHttp = false;
						return new BasicAuthenticator(apiKey, enableBasicAuthenticationOverUnsecuredHttp);
					}

					return new SecuredAuthenticator(apiKey);
				});

			if (useBasicAuthenticator == false)
				oauthSource = options.Url + "/OAuth/API-Key";

			var result = authenticator.DoOAuthRequest(oauthSource);
			return result != null;
		}
	}
}