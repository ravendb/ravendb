using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;

using System;
using System.Collections.Generic;

using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.OAuth
{
    public class SecuredAuthenticator : AbstractAuthenticator, IDisposable
    {
        private readonly bool autoRefreshToken;
        private Timer autoRefreshTimer; 
        private readonly object locker = new object();

        private readonly TimeSpan defaultRefreshTimeInMilis = TimeSpan.FromMinutes(29);

        public SecuredAuthenticator(bool autoRefreshToken)
        {
            this.autoRefreshToken = autoRefreshToken;
        }

        public void Dispose()
        {
            autoRefreshTimer?.Dispose();
            autoRefreshTimer = null;
        }

        public override void ConfigureRequest(object sender, WebRequestEventArgs e)
        {
            if (CurrentOauthToken != null)
            {
                base.ConfigureRequest(sender, e);
                return;
            }

            if (e.Credentials != null && e.Credentials.ApiKey != null)
            {
                if (e.Client != null && e.Client.DefaultRequestHeaders.Contains("Has-Api-Key") == false)
                {                    
                    e.Client.DefaultRequestHeaders.Add("Has-Api-Key", "true");
                }
#if !DNXCORE50
                if (e.Request != null)
                    e.Request.Headers["Has-Api-Key"] = "true";
#endif
            }
        }

#if !DNXCORE50
        private Tuple<HttpWebRequest, string> PrepareOAuthRequest(string oauthSource, string serverRSAExponent, string serverRSAModulus, string challenge, string apiKey)
        {
            var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
            authRequest.Headers["grant_type"] = "client_credentials";
            authRequest.Accept = "application/json;charset=UTF-8";
            authRequest.Method = "POST";

            if (!string.IsNullOrEmpty(serverRSAExponent) && !string.IsNullOrEmpty(serverRSAModulus) && !string.IsNullOrEmpty(challenge))
            {
                var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
                var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

                var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);

                if (apiKeyParts.Length > 2)
                {
                    apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
                }

                if (apiKeyParts.Length < 2)
                    throw new InvalidOperationException("Invalid API key");

                var apiKeyName = apiKeyParts[0].Trim();
                var apiSecret = apiKeyParts[1].Trim();


                var data = OAuthHelper.DictionaryToString(new Dictionary<string, string>
                {
                    {OAuthHelper.Keys.RSAExponent, serverRSAExponent},
                    {OAuthHelper.Keys.RSAModulus, serverRSAModulus},
                    {
                        OAuthHelper.Keys.EncryptedData,
                        OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string>
                        {
                            {OAuthHelper.Keys.APIKeyName, apiKeyName},
                            {OAuthHelper.Keys.Challenge, challenge},
                            {OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret))}
                        }))
                    }
                });

                return Tuple.Create(authRequest, data);
            }
            authRequest.ContentLength = 0;
            return Tuple.Create(authRequest, (string)null);
        }

        // TODO: Delete this, and use the async one.
        public override Action<HttpWebRequest> DoOAuthRequest(string oauthSource, string apiKey)
        {
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
                var authRequestTuple = PrepareOAuthRequest(oauthSource, serverRSAExponent, serverRSAModulus, challenge, apiKey);
                var authRequest = authRequestTuple.Item1;
                if (authRequestTuple.Item2 != null)
                {
                    using (var stream = authRequest.GetRequestStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        writer.Write(authRequestTuple.Item2);
                    }
                }

                try
                {
                    using (var authResponse = authRequest.GetResponse())
                    using (var stream = authResponse.GetResponseStreamWithHttpDecompression())
                    using (var reader = new StreamReader(stream))
                    {
                        var currentOauthToken = reader.ReadToEnd();
                        CurrentOauthToken = currentOauthToken;
                        CurrentOauthTokenWithBearer = "Bearer " + currentOauthToken;

                        ScheduleTokenRefresh(oauthSource, apiKey);

                        return (Action<HttpWebRequest>)(request => SetHeader(request.Headers, "Authorization", CurrentOauthTokenWithBearer));
                    }
                }
                catch (WebException ex)
                {
                    if (tries > 2)
                        // We've already tried three times and failed
                        throw;

                    var authResponse = ex.Response as HttpWebResponse;
                    if (authResponse == null || authResponse.StatusCode != HttpStatusCode.PreconditionFailed)
                        throw;

                    var header = authResponse.Headers[HttpResponseHeader.WwwAuthenticate];
                    if (string.IsNullOrEmpty(header) || !header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey))
                        throw;

                    authResponse.Close();

                    var challengeDictionary = OAuthHelper.ParseDictionary(header.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());
                    serverRSAExponent = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
                    serverRSAModulus = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
                    challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);

                    if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
                    {
                        throw new InvalidOperationException("Invalid response from server, could not parse raven authentication information: " + header);
                    }
                }
            }
        }
#endif

        public async Task<Action<HttpClient>> DoOAuthRequestAsync(string baseUrl, string oauthSource, string apiKey)
        {
            if (oauthSource == null)
                throw new ArgumentNullException("oauthSource");

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
#if !DNXCORE50
                var handler = new WebRequestHandler();
#else
                var handler = new HttpClientHandler();
#endif

                using (var httpClient = new HttpClient(handler))
                {
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "UTF-8" });

                    string data = null;
                    if (!string.IsNullOrEmpty(serverRSAExponent) && !string.IsNullOrEmpty(serverRSAModulus) && !string.IsNullOrEmpty(challenge))
                    {
                        var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
                        var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

                        var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);
                        if (apiKeyParts.Length > 2)
                        {
                            apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
                        }
                        if (apiKeyParts.Length < 2) throw new InvalidOperationException("Invalid API key");

                        var apiKeyName = apiKeyParts[0].Trim();
                        var apiSecret = apiKeyParts[1].Trim();

                        data = OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.RSAExponent, serverRSAExponent }, { OAuthHelper.Keys.RSAModulus, serverRSAModulus }, { OAuthHelper.Keys.EncryptedData, OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.APIKeyName, apiKeyName }, { OAuthHelper.Keys.Challenge, challenge }, { OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret)) } })) } });
                    }

                    var requestUri = oauthSource;

                    var response = await httpClient.PostAsync(requestUri, data != null ? (HttpContent)new CompressedStringContent(data, true) : new StringContent("")).AddUrlIfFaulting(new Uri(requestUri)).ConvertSecurityExceptionToServerNotFound().ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        // We've already tried three times and failed
                        if (tries >= 3) throw ErrorResponseException.FromResponseMessage(response);

                        if (response.StatusCode != HttpStatusCode.PreconditionFailed) throw ErrorResponseException.FromResponseMessage(response);

                        var header = response.Headers.GetFirstValue("WWW-Authenticate");
                        if (header == null || header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey) == false) throw new ErrorResponseException(response, "Got invalid WWW-Authenticate value");

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
                        CurrentOauthToken = currentOauthToken;
                        CurrentOauthTokenWithBearer = "Bearer " + currentOauthToken;

                        ScheduleTokenRefresh(oauthSource, apiKey);

                        return (Action<HttpClient>)(SetAuthorization);
                    }
                }
            }
        }

        private void ScheduleTokenRefresh(string oauthSource, string apiKey)
        {
            if (!autoRefreshToken)
            {
                return;
            }

            lock (locker)
            {
                if (autoRefreshTimer != null)
                {
                    autoRefreshTimer.Change(defaultRefreshTimeInMilis, Timeout.InfiniteTimeSpan);
                }
                else
                {
                    autoRefreshTimer = new Timer(_ => DoOAuthRequestAsync(null, oauthSource, apiKey).IgnoreUnobservedExceptions(), null, defaultRefreshTimeInMilis, Timeout.InfiniteTimeSpan);
                }
            }
        }
    }
}
