using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Client.OAuth;
using Raven.Client.Platform;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    public class ApiKeyAuthenticator : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ApiKeyAuthenticator>("Client");
        private readonly CancellationTokenSource _disposedToken = new CancellationTokenSource();

        public async Task<string> AuthenticateAsync(string url, string apiKey, JsonOperationContext context)
        {
            var uri = new Uri(url.ToWebSocketPath());

            using (var webSocket = new RavenClientWebSocket())
            {
                try
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Trying to connect using WebSocket to {uri} for authentication");
                    try
                    {
                        await webSocket.ConnectAsync(uri, _disposedToken.Token);
                    }
                    catch (WebSocketException webSocketException)
                    {
                        throw new InvalidOperationException($"Cannot connect using WebSocket to {uri} for authentication", webSocketException);
                    }

                    AuthenticatorChallenge authenticatorChallenge;
                    using (var result = await Recieve(webSocket, context))
                    {
                        if (result == null)
                            throw new InvalidDataException("Got null authtication challenge");
                        authenticatorChallenge = JsonDeserializationClient.AuthenticatorChallenge(result);
                    }
                    var challenge = ComputeChallenge(authenticatorChallenge, apiKey);
                    await Send(webSocket, context, "ChallengeResponse", challenge);

                    string currentToken;
                    using (var reader = await Recieve(webSocket, context))
                    {
                        string error;
                        if (reader.TryGet("Error", out error))
                        {
                            string exceptionType;
                            if (reader.TryGet("ExceptionType", out exceptionType) == false || exceptionType == "InvalidOperationException")
                                throw new InvalidOperationException("Server returned error: " + error);

                            if (exceptionType == "InvalidApiKeyException")
                                throw new InvalidApiKeyException(error);
                        }

                        string currentOauthToken;
                        if (reader.TryGet("CurrentToken", out currentOauthToken) == false || currentOauthToken == null)
                            throw new InvalidOperationException("Missing 'CurrentToken' in response message");

                        currentToken = currentOauthToken;
                    }

                    try
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Close from client", _disposedToken.Token);
                    }
                    catch (Exception ex)
                    {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failed to close the client", ex);
                    }

                    return currentToken;
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to DoOAuthRequest to {url} with {apiKey}", ex);
                    throw;
                }
            }
        }

        private async Task Send(RavenClientWebSocket webSocket, JsonOperationContext context, string command, string commandParameter)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Sending WebSocket Authentication Command {command} - {commandParameter}");

            var json = new DynamicJsonValue
            {
                [command] = commandParameter
            };

            using (var stream = new MemoryStream())
            using(var writer = new BlittableJsonTextWriter(context, stream))
            {
                context.Write(writer, json);
                writer.Flush();

                ArraySegment<byte> bytes;
                stream.TryGetBuffer(out bytes);
                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None).ConfigureAwait(false);
            }
        }

        private string ComputeChallenge(AuthenticatorChallenge challenge, string apiKey)
        {
            if (string.IsNullOrEmpty(challenge.RSAExponent) || string.IsNullOrEmpty(challenge.RSAModulus) || string.IsNullOrEmpty(challenge.Challenge))
                throw new InvalidOperationException("Invalid authentication information");

            var apiKeyParts = ExtractApiKeyAndSecret(apiKey);
            var apiKeyName = apiKeyParts[0].Trim();
            var apiSecret = apiKeyParts[1].Trim();

            var data = OAuthHelper.DictionaryToString(new Dictionary<string, string>
            {
                {OAuthHelper.Keys.RSAExponent, challenge.RSAExponent},
                {OAuthHelper.Keys.RSAModulus, challenge.RSAModulus},
                {
                    OAuthHelper.Keys.EncryptedData,
                    OAuthHelper.EncryptAsymmetric(OAuthHelper.ParseBytes(challenge.RSAExponent),
                        OAuthHelper.ParseBytes(challenge.RSAModulus),
                        OAuthHelper.DictionaryToString(new Dictionary<string, string>
                        {
                            {OAuthHelper.Keys.APIKeyName, apiKeyName},
                            {OAuthHelper.Keys.Challenge, challenge.Challenge},
                            {OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format("{0};{1}", challenge, apiSecret))}
                        }))
                }
            });

            return data;
        }

        private string[] ExtractApiKeyAndSecret(string apiKey)
        {
            var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);

            if (apiKeyParts.Length > 2)
            {
                apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
            }

            if (apiKeyParts.Length < 2)
                throw new InvalidApiKeyException("Invalid Api-Key. Contains less then two parts separated with slash");

            return apiKeyParts;
        }

        private async Task<BlittableJsonReaderObject> Recieve(RavenClientWebSocket webSocket, JsonOperationContext context)
        {
            BlittableJsonDocumentBuilder builder = null;
            try
            {
                if (webSocket.State != WebSocketState.Open)
                    throw new InvalidOperationException(
                        $"Trying to 'ReceiveAsync' WebSocket while not in Open state. State is {webSocket.State}");

                var state = new JsonParserState();
                JsonOperationContext.ManagedPinnedBuffer buffer;
                using (context.GetManagedBuffer(out buffer))
                using (var parser = new UnmanagedJsonParser(context, state, "")) //TODO: FIXME
                {
                    builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, nameof(ApiKeyAuthenticator) + "." + nameof(Recieve), parser, state);
                    builder.ReadObject();
                    while (builder.Read() == false)
                    {
                        var result = await webSocket.ReceiveAsync(buffer.Buffer, _disposedToken.Token);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Client got close message from server and is closing connection");

                            builder.Dispose();
                            // actual socket close from dispose
                            return null;
                        }

                        if (result.EndOfMessage == false)
                        {
                             throw new EndOfStreamException("Stream ended without reaching end of json content.");
                        }

                        parser.SetBuffer(buffer, result.Count);
                    }
                    builder.FinalizeDocument();

                    return builder.CreateReader();
                }
            }
            catch (WebSocketException ex)
            {
                builder?.Dispose();
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to receive a message, client was probably disconnected", ex);
                throw;
            }
        }

        public void Dispose()
        {
            _disposedToken.Cancel();
        }
    }
}