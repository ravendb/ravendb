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

		readonly ConcurrentDictionary<string, SecuredAuthenticator> authenticators = new ConcurrentDictionary<string, SecuredAuthenticator>();

		public void ConfigureRequest(RavenConnectionStringOptions options, WebRequest request)
		{
			if (RequestTimeoutInMs.HasValue)
				request.Timeout = RequestTimeoutInMs.Value;


			if (options.ApiKey == null)
			{
				request.Credentials = options.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				return;
			}

			var value = authenticators.GetOrAdd(options.ApiKey, s => new SecuredAuthenticator(s));

			value.ConfigureRequest(this, new WebRequestEventArgs
			{
				Request = request
			});
		}

		public HttpRavenRequest Create(string url, string method, RavenConnectionStringOptions connectionStringOptions)
		{
			return new HttpRavenRequest(url, method, ConfigureRequest, HandleUnauthorizedResponse, connectionStringOptions);
		}

		private bool HandleUnauthorizedResponse(RavenConnectionStringOptions options, WebResponse webResponse)
		{
			if (options.ApiKey == null)
				return false;

			var value = authenticators.GetOrAdd(options.ApiKey, s => new SecuredAuthenticator(s));

			var oauthSource = options.Url + "/OAuth/API-Key";

			var result = value.DoOAuthRequest(oauthSource);
			return result != null;
		}
	}
}