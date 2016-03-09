using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Abstractions.OAuth
{
    public class SecuredAuthenticator : AbstractAuthenticator, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(SecuredAuthenticator));
        private string apiKey;
        private ClientWebSocket webSocket;
        private bool disposed;
        private readonly CancellationTokenSource disposedToken = new CancellationTokenSource();
        private Uri uri;
        private string serverRSAExponent;
        private string serverRSAModulus;
        private string challenge;
        private const int MaxWebSocketRecvSize = 4096;

        public override void ConfigureRequest(object sender, WebRequestEventArgs e)
        {
            if (CurrentOauthToken != null)
            {
                base.ConfigureRequest(sender, e);
                return;
            }

            if (e.Credentials?.ApiKey != null)
            {
                e.Client?.DefaultRequestHeaders.Add("Has-Api-Key", "true");
            }
        }

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
                var handler = new WinHttpHandler();

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

                        return (Action<HttpClient>)(SetAuthorization);
                    }
                }
            }
        }

        public async Task<Action<HttpClient>> DoOAuthRequestAsync(string url, string apiKey)
        {
            ThrowIfBadUrlOrApiKey(url, apiKey);

            this.apiKey = apiKey;

            uri = new Uri(url.Replace("http://", "ws://").Replace(".fiddler", ""));

            webSocket = new ClientWebSocket();

            try
            {
                await EstablishConnection();
                var recvRavenJObject = await Recieve();
                var challenge = ComputeChallenge(recvRavenJObject);
                await Send("ChallengeResponse", challenge);
                recvRavenJObject = await Recieve();
                SetCurrentTokenFromReply(recvRavenJObject);

                return (Action<HttpClient>)(SetAuthorization);
            }
            catch (Exception ex)
            {
                Logger.DebugException($"Failed to DoOAuthRequest to {url} with {apiKey}", ex);
                throw;
            }
            finally
            {
                Dispose();
            }
        }

        private void ThrowIfBadUrlOrApiKey(string url, string apiKey)
        {
            if (string.IsNullOrEmpty(url))
                throw new InvalidOperationException("DoAuthRequest provided with invalid url");

            if (this.apiKey != null)
                throw new InvalidOperationException($"DoAuthRequest is in the middle of negotiation with {url}");

            if (apiKey == null)
                throw new InvalidOperationException("DoAuthRequest provided with null apiKey");
        }

        private async Task EstablishConnection()
        {
            if (disposed)
                return;

            Logger.Info("Trying to connect using WebSocket to {0} for authentication", uri);
            try
            {
                await webSocket.ConnectAsync(uri, CancellationToken.None);
            }
            catch (WebSocketException webSocketException)
            {
                throw new InvalidOperationException($"Cannot connect using WebSocket to {uri} for authentication", webSocketException);
            }
        }

        private async Task Send(string command, string commandParameter)
        {
            Logger.Info($"Sending WebSocket Authentication Command {command} - {commandParameter} to {uri}");

            var ravenJObject = new RavenJObject
            {
                ["Command"] = command,
                ["Param"] = commandParameter,
            };

            var stream = new MemoryStream();
            ravenJObject.WriteTo(stream);
            ArraySegment<byte> bytes;
            stream.TryGetBuffer(out bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None).ConfigureAwait(false);
        }

        private string ComputeChallenge(RavenJObject recvRavenJObject)

        {
            if (recvRavenJObject == null)
                throw new InvalidDataException("Got null json object in ComputeChallenge");

            var responseCode = recvRavenJObject.Value<string>("ResponseCode");

            if (responseCode == null)
                throw new InvalidOperationException("Missing 'ResponseCode' in authentication response");

            if (Logger.IsDebugEnabled)
                Logger.Debug($"Got authentication response with command {responseCode} - for {uri}");

            if (responseCode.Equals("400")) // BadRequest
            {
                DisposeAsync.Wait();
                var description = recvRavenJObject.Value<string>("Description");
                if (description == null)
                    throw new InvalidDataException($"Missing or empty 'Description' in message, server response code : Bad Request");

                throw new InvalidOperationException($"Server response code : Bad Request - {description}");
            }

            if (responseCode.Equals("412") == false) // Precondition Failed
                throw new InvalidOperationException($"Got invalid responseCode {responseCode} - for {uri}");

            var wwwAuthenticate = recvRavenJObject.Value<string>("WwwAuthenticate");

            if (string.IsNullOrEmpty(wwwAuthenticate) || !wwwAuthenticate.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey))
                throw new InvalidOperationException("Missing or empty 'WwwAuthenticate' in message");

            var challengeDictionary = OAuthHelper.ParseDictionary(wwwAuthenticate.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());

            serverRSAExponent = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
            serverRSAModulus = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
            challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);

            if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
                throw new InvalidOperationException($"Invalid response from server, could not parse raven authentication information:{wwwAuthenticate}");

            var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
            var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

            var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);

            if (apiKeyParts.Length > 2)
            {
                apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
            }

            if (apiKeyParts.Length < 2)
                throw new InvalidOperationException("Invalid Api-Key. Contains less then two parts separated with slash");

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

            return data;
        }

        private async Task<RavenJObject> Recieve()
        {
            try
            {
                if (webSocket.State != WebSocketState.Open)
                    throw new InvalidOperationException(
                        $"Trying to 'ReceiveAsync' WebSocket while not in Open state. State is {webSocket.State}");

                using (var ms = new MemoryStream())
                {
                    ArraySegment<byte> bytes;
                    ms.SetLength(MaxWebSocketRecvSize);
                    ms.TryGetBuffer(out bytes);
                    var arraySegment = new ArraySegment<byte>(bytes.Array, 0, MaxWebSocketRecvSize);
                    var result = await webSocket.ReceiveAsync(arraySegment, disposedToken.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Client got close message from server and is closing connection");

                        // actual socket close from dispose
                        return null;
                    }

                    if (result.EndOfMessage == false)
                    {
                        var err = $"In Recieve got response longer then {MaxWebSocketRecvSize}";
                        var ex = new InvalidOperationException(err);
                        Logger.DebugException(err, ex);
                        throw ex;
                    }

                    using (var reader = new StreamReader(ms, Encoding.UTF8, true, MaxWebSocketRecvSize, true))
                    using (var jsonReader = new RavenJsonTextReader(reader)
                    {
                        SupportMultipleContent = true
                    })
                    {
                        if (jsonReader.Read() == false)
                            throw new InvalidDataException("Couldn't read recieved websocket json message");
                        return RavenJObject.Load(jsonReader);
                    }
                }
            }
            catch (WebSocketException ex)
            {
                Logger.DebugException("Failed to receive a message, client was probably disconnected", ex);
                throw;
            }
        }

        private void SetCurrentTokenFromReply(RavenJObject recvRavenJObject)
        {
            var responseCode = recvRavenJObject.Value<string>("ResponseCode");

            if (responseCode == null)
                throw new InvalidOperationException("Missing 'ResponseCode' in authentication response");

            if (Logger.IsDebugEnabled)
                Logger.Debug($"Got authentication response with command {responseCode} - for {uri}");

            string currentOauthToken = null;

            if (responseCode.Equals("200"))
            {
                currentOauthToken = recvRavenJObject.Value<string>("currentOauthToken");
                if (currentOauthToken == null)
                    throw new InvalidOperationException("Missing 'currentOauthToken' in 'ChallengeResponse' message");

                CurrentOauthToken = currentOauthToken;
                CurrentOauthTokenWithBearer = $"Bearer {CurrentOauthToken}";
            }
            else if (responseCode.Equals("400")) // BadRequest
            {
                DisposeAsync.Wait();
                var description = recvRavenJObject.Value<string>("Description");
                if (description == null)
                    throw new InvalidDataException($"Missing or empty 'Description' in message");

                throw new InvalidOperationException($"Server response code : Bad Request - {description}");
            }
            else if (responseCode.Equals("401")) // Unauthrized
            {
                var description = recvRavenJObject.Value<string>("Description");
                if (description == null)
                    throw new InvalidDataException($"Missing or empty 'Description' in message");

                throw new InvalidOperationException($"Server response code : Unauthrized - {description}");
            }
            else
            {
                throw new InvalidOperationException($"Server response with unknow code : {responseCode}");
            }
        }

        public void Dispose()
        {
            if (disposed)
                return;

            AsyncHelpers.RunSync(() => DisposeAsync);
        }

        public Task DisposeAsync
        {
            get
            {
                disposed = true;

                disposedToken.Cancel();

                return webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by the client", CancellationToken.None)
                    .ContinueWith(_ =>
                    {
                        try
                        {
                            webSocket?.Dispose();
                        }
                        catch (Exception e)
                        {
                            Logger.ErrorException($"Got error from server connection for {uri.ToString()}", e);
                        }
                    });
            }
        }
    }
}
