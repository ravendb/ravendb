using System;
using System.IO;
using System.Net;
using System.Security;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;

#if SILVERLIGHT
using Raven.Client.Connection;
using Raven.Client.Silverlight.Connection;
using System.Net.Browser;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif

namespace Raven.Abstractions.OAuth
{
	public class BasicAuthenticator : AbstractAuthenticator
	{
		private readonly bool enableBasicAuthenticationOverUnsecuredHttp;
	
		public BasicAuthenticator(bool enableBasicAuthenticationOverUnsecuredHttp)
		{
			this.enableBasicAuthenticationOverUnsecuredHttp = enableBasicAuthenticationOverUnsecuredHttp;
		}


		public async Task<Action<HttpWebRequest>> HandleOAuthResponseAsync(string oauthSource, string apiKey)
		{
			var authRequest = PrepareOAuthRequest(oauthSource, apiKey);
		    WebResponse webResponse;
		    try
		    {
		        webResponse =
		            await Task<WebResponse>.Factory.FromAsync(authRequest.BeginGetResponse, authRequest.EndGetResponse, null);
		    }
		    catch (SecurityException e)
		    {
		        throw new WebException(
		            "Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.",
		            e);
		    }
		    catch (Exception e)
		    {
		        e.Data["Url"] = authRequest.RequestUri;
		        throw;
		    }
#if SILVERLIGHT
            using (var stream = webResponse.GetResponseStream())
#else
			using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
#endif
            using (var reader = new StreamReader(stream))
            {
                CurrentOauthToken = "Bearer " + reader.ReadToEnd();
                return (Action<HttpWebRequest>)(request => SetHeader(request.Headers, "Authorization", CurrentOauthToken));
            }

		}

#if !SILVERLIGHT && !NETFX_CORE
		public override Action<HttpWebRequest> DoOAuthRequest(string oauthSource, string apiKey)
		{
			var authRequest = PrepareOAuthRequest(oauthSource, apiKey);
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

		private HttpWebRequest PrepareOAuthRequest(string oauthSource, string apiKey)
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

			if (oauthSource.StartsWith("https", StringComparison.OrdinalIgnoreCase) == false && enableBasicAuthenticationOverUnsecuredHttp == false)
				throw new InvalidOperationException(BasicOAuthOverHttpError);

			return authRequest;
		}

		private const string BasicOAuthOverHttpError = @"Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network.
Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism.
You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value), or setup your own behavior by providing a value for:
	documentStore.Conventions.HandleUnauthorizedResponse
If you are on an internal network or requires this for testing, you can disable this warning by calling:
	documentStore.JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = true;
";
	}
}
