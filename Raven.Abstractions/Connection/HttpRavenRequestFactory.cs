using System.IO;
using System.Net;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequestFactory
	{
		public int? RequestTimeoutInMs { get; set; }

		private bool RefreshOauthToken(RavenConnectionStringOptions options, WebResponse response)
		{
			var oauthSource = response.Headers["OAuth-Source"];
			if (string.IsNullOrEmpty(oauthSource))
				return false;

			var authRequest = PrepareOAuthRequest(options, oauthSource);
			using (var authResponse = authRequest.GetResponse())
			using (var stream = authResponse.GetResponseStreamWithHttpDecompression())
			using (var reader = new StreamReader(stream))
			{
				options.CurrentOAuthToken = "Bearer " + reader.ReadToEnd();
			}
			return true;
		}

		private HttpWebRequest PrepareOAuthRequest(RavenConnectionStringOptions options, string oauthSource)
		{
			var authRequest = (HttpWebRequest) WebRequest.Create(oauthSource);
			authRequest.Credentials = options.Credentials;
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			authRequest.Accept = "application/json;charset=UTF-8";

			authRequest.Headers["grant_type"] = "client_credentials";

			if (string.IsNullOrEmpty(options.ApiKey) == false)
				authRequest.Headers["Api-Key"] = options.ApiKey;

			return authRequest;
		}

		public void ConfigureRequest(RavenConnectionStringOptions options, WebRequest request)
		{
			request.Credentials = options.Credentials ?? CredentialCache.DefaultNetworkCredentials;

			if (RequestTimeoutInMs.HasValue)
				request.Timeout = RequestTimeoutInMs.Value;
			
			if (string.IsNullOrEmpty(options.CurrentOAuthToken) == false)
				request.Headers["Authorization"] = options.CurrentOAuthToken;
		}

		public HttpRavenRequest Create(string url, string method, RavenConnectionStringOptions connectionStringOptions)
		{
			return new HttpRavenRequest(url, method, ConfigureRequest, RefreshOauthToken, connectionStringOptions);
		}
	}
}