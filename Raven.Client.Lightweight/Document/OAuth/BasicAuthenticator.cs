using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Extensions;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
using System.Net.Browser;
#endif

namespace Raven.Client.Document.OAuth
{
	public class BasicAuthenticator : AbstractAuthenticator
	{
		private readonly string apiKey;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
	
		public BasicAuthenticator(string apiKey, HttpJsonRequestFactory jsonRequestFactory)
		{
			this.apiKey = apiKey;
			this.jsonRequestFactory = jsonRequestFactory;
		}


		public Task<Action<HttpWebRequest>> HandleOAuthResponseAsync(string oauthSource)
		{
			var authRequest = PrepareOAuthRequest(oauthSource);
			return Task<WebResponse>.Factory.FromAsync(authRequest.BeginGetResponse, authRequest.EndGetResponse, null)
				.AddUrlIfFaulting(authRequest.RequestUri)
				.ConvertSecurityExceptionToServerNotFound()
				.ContinueWith(task =>
				{
#if !SILVERLIGHT
					using (var stream = task.Result.GetResponseStreamWithHttpDecompression())
#else
					using(var stream = task.Result.GetResponseStream())
#endif
					using (var reader = new StreamReader(stream))
					{
						CurrentOauthToken = "Bearer " + reader.ReadToEnd();
						return (Action<HttpWebRequest>) (request => SetHeader(request.Headers, "Authorization", CurrentOauthToken));
					}
				});
		}

#if !SILVERLIGHT
		public Action<HttpWebRequest> HandleOAuthResponse(string oauthSource)
		{
			var authRequest = PrepareOAuthRequest(oauthSource);
			using (var response = authRequest.GetResponse())
			{
				using (var stream = response.GetResponseStreamWithHttpDecompression())
				using (var reader = new StreamReader(stream))
				{
					CurrentOauthToken = "Bearer " + reader.ReadToEnd();
					return request => SetHeader(request.Headers, "Authorization", CurrentOauthToken);
				}
			}
		}
#endif

		private HttpWebRequest PrepareOAuthRequest(string oauthSource)
		{
#if !SILVERLIGHT
			var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
#else
			var authRequest = (HttpWebRequest) WebRequestCreator.ClientHttp.Create(new Uri(oauthSource.NoCache()));
#endif
			authRequest.Headers["grant_type"] = "client_credentials";
			authRequest.Accept = "application/json;charset=UTF-8";

			if (String.IsNullOrEmpty(apiKey) == false)
				SetHeader(authRequest.Headers, "Api-Key", apiKey);

			if (oauthSource.StartsWith("https", StringComparison.InvariantCultureIgnoreCase) == false &&
			   jsonRequestFactory.EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers == false)
				throw new InvalidOperationException(BasicOAuthOverHttpError);
			return authRequest;
		}

		private const string BasicOAuthOverHttpError = @"Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network.
Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism.
You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value), or setup your own behavior by providing a value for:
	documentStore.Conventions.HandleUnauthorizedResponse
If you are on an internal network or requires this for testing, you can disable this warning by calling:
	documentStore.JsonRequestFactory.EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = true;
";
	}
}