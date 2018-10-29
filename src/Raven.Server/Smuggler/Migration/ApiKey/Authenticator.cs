using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Server.Extensions;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public class Authenticator
    {
        public static async Task<string> GetOAuthToken(
            string baseUrl,
            string oauthSource, 
            string apiKey, 
            bool skipServerCertificateValidation)
        {
            if (oauthSource == null)
                throw new ArgumentNullException(nameof(oauthSource));

            string serverRSAExponent = null;
            string serverRSAModulus = null;
            string challenge = null;

            // Note that at two tries will be needed in the normal case.
            // The first try will get back a challenge,
            // the second try will try authentication. If something goes wrong server-side though
            // (e.g. the server was just rebooted or the challenge timed out for some reason), we
            // might get a new challenge back, so we try a third time just in case.
            int tries = 0;
            while (true)
            {
                tries++;
                var handler = new HttpClientHandler();

                if (skipServerCertificateValidation)
                    handler.ServerCertificateCustomValidationCallback = (message, certificate2, arg3, arg4) => true;

                using (var httpClient = new HttpClient(handler))
                {
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "UTF-8" });

                    var data = string.Empty;
                    if (string.IsNullOrEmpty(serverRSAExponent) == false && 
                        string.IsNullOrEmpty(serverRSAModulus) == false && 
                        string.IsNullOrEmpty(challenge) == false)
                    {
                        var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
                        var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

                        var apiKeyParts = GetApiKeyParts(apiKey);

                        data = OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.RSAExponent, serverRSAExponent }, { OAuthHelper.Keys.RSAModulus, serverRSAModulus }, { OAuthHelper.Keys.EncryptedData, OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.APIKeyName, apiKeyParts.ApiKeyName }, { OAuthHelper.Keys.Challenge, challenge }, { OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiKeyParts.ApiSecret)) } })) } });
                    }

                    var requestUri = oauthSource;
                    var response = await httpClient.PostAsync(requestUri, new StringContent(data)).AddUrlIfFaulting(new Uri(requestUri)).ConvertSecurityExceptionToServerNotFound().ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        // We've already tried three times and failed
                        if (tries >= 3)
                            throw ErrorResponseException.FromResponseMessage(response);

                        if (response.StatusCode != HttpStatusCode.PreconditionFailed)
                            throw ErrorResponseException.FromResponseMessage(response);

                        var header = response.Headers.GetFirstValue("WWW-Authenticate");
                        if (header == null || header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey) == false)
                            throw new ErrorResponseException(response, "Got invalid WWW-Authenticate value");

                        var challengeDictionary = OAuthHelper.ParseDictionary(header.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());
                        serverRSAExponent = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
                        serverRSAModulus = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
                        challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);

                        if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
                        {
                            throw new InvalidOperationException("Invalid response from server, could not parse raven authentication information: " + header);
                        }

                        continue;
                    }

                    using (var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
                    using (var reader = new StreamReader(stream))
                    {
                        var currentOauthToken = reader.ReadToEnd();
                        return currentOauthToken;
                    }
                }
            }
        }

        public static (string ApiKeyName, string ApiSecret) GetApiKeyParts(string apiKey)
        {
            var apiKeyParts = apiKey.Split(new[] {'/'}, StringSplitOptions.None);
            if (apiKeyParts.Length > 2)
            {
                apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
            }

            if (apiKeyParts.Length < 2)
                throw new InvalidOperationException("Invalid API key");

            var apiKeyName = apiKeyParts[0].Trim();
            var apiSecret = apiKeyParts[1].Trim();
            return (apiKeyName, apiSecret);
        }

        public static async Task<string> GetLegacyOAuthToken(string oauthSource, string apiKey, bool enableBasicAuthenticationOverUnsecuredHttp)
        {
            using (var httpClient = new HttpClient(new HttpClientHandler()))
            {
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "UTF-8" });

                httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
                httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));

                if (string.IsNullOrEmpty(apiKey) == false)
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Api-Key", apiKey);

                if (oauthSource.StartsWith("https", StringComparison.OrdinalIgnoreCase) == false && enableBasicAuthenticationOverUnsecuredHttp == false)
                    throw new InvalidOperationException(BasicOAuthOverHttpError);

                var requestUri = oauthSource;
                var response = await httpClient.GetAsync(requestUri)
                    .ConvertSecurityExceptionToServerNotFound()
                    .AddUrlIfFaulting(new Uri(requestUri)).ConfigureAwait(false);

                var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false);
                using (var reader = new StreamReader(stream))
                {
                    var currentOauthToken = reader.ReadToEnd();
                    return currentOauthToken;
                }
            }
        }

        private const string BasicOAuthOverHttpError = @"Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network.
Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism.
You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value).
If you are on an internal network or require this for testing, you can disable this warning";
    }
}
