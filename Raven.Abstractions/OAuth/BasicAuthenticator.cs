using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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

        public async Task<Action<HttpClient>> HandleOAuthResponseAsync(string oauthSource, string apiKey)
        {
            var httpClient = new HttpClient(new HttpClientHandler());
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "UTF-8" });

#if !SILVERLIGHT
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
#endif

            if (string.IsNullOrEmpty(apiKey) == false)
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Api-Key", apiKey);

            if (oauthSource.StartsWith("https", StringComparison.OrdinalIgnoreCase) == false && enableBasicAuthenticationOverUnsecuredHttp == false)
                throw new InvalidOperationException(BasicOAuthOverHttpError);

            var requestUri = oauthSource
#if SILVERLIGHT
				.NoCache()
#endif
;
            var response = await httpClient.GetAsync(requestUri)
                                           .ConvertSecurityExceptionToServerNotFound()
                                           .AddUrlIfFaulting(new Uri(requestUri));

            var stream = await response.GetResponseStreamWithHttpDecompression();
            using (var reader = new StreamReader(stream))
            {
                CurrentOauthToken = reader.ReadToEnd();
                return (Action<HttpClient>)(SetAuthorization);
            }
        }

#if !SILVERLIGHT && !NETFX_CORE
        private HttpWebRequest PrepareOAuthRequest(string oauthSource, string apiKey)
        {
            var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
            authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
            authRequest.Headers["grant_type"] = "client_credentials";
            authRequest.Accept = "application/json;charset=UTF-8";

            if (String.IsNullOrEmpty(apiKey) == false)
                SetHeader(authRequest.Headers, "Api-Key", apiKey);

            if (oauthSource.StartsWith("https", StringComparison.OrdinalIgnoreCase) == false && enableBasicAuthenticationOverUnsecuredHttp == false)
                throw new InvalidOperationException(BasicOAuthOverHttpError);

            return authRequest;
        }

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

        private const string BasicOAuthOverHttpError = @"Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network.
Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism.
You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value), or setup your own behavior by providing a value for:
	documentStore.Conventions.HandleUnauthorizedResponse
If you are on an internal network or requires this for testing, you can disable this warning by calling:
	documentStore.JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = true;
";
    }
}