using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.OAuth;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.Authentication
{
    public class OAuthApiKeyHandler : RequestHandler
    {
        private const string DebugTag = "/oauth/api-key";
        private const int MaxOAuthContentLength = 1500;
        private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

        private static readonly Logger _logger = LoggerSetup.Instance.GetLogger<OAuthApiKeyHandler>("Raven/Server");

        [RavenAction("/oauth/api-key", "GET", "/oauth/api-key", NoAuthorizationRequired = true)]
        public async Task OauthGetApiKey()
        {
            try
            {
                using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                {
                    try
                    {
                        JsonOperationContext context;
                        using (ServerStore.ContextPool.AllocateOperationContext(out context))
                        {
                            await SendInitialChallenge(webSocket);
                            var accessToken = await ProcessToken(context, webSocket);
                            if (accessToken == null)
                            {
                                await SendError(webSocket, "Unable to authenticate api key", "InvalidApiKeyException");
                                return;
                            }

                            AccessToken old;
                            if (Server.AccessTokensByName.TryGetValue(accessToken.Name, out old))
                            {
                                AccessToken value;
                                Server.AccessTokensByName.TryRemove(old.Name, out value);
                            }

                            Server.AccessTokensById[accessToken.Token] = accessToken;
                            Server.AccessTokensByName[accessToken.Name] = accessToken;

                            await SendResponse(webSocket, new DynamicJsonValue
                            {
                                ["CurrentToken"] = accessToken.Token
                            });

                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by server",
                                ServerStore.ServerShutdown);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Failed to authenticate api key", e);
                        await SendError(webSocket, e.ToString(), "InvalidOperationException");
                    }
                }
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Got exception while handling /oauth/api-key endpoint", ex);
            }
        }

        private async Task SendInitialChallenge(WebSocket webSocket)
        {
            var challengeData = new Dictionary<string, string>
            {
                {OAuthHelper.Keys.ChallengeTimestamp, OAuthServerHelper.DateTimeToString(SystemTime.UtcNow)},
                {
                    OAuthHelper.Keys.ChallengeSalt,
                    OAuthHelper.BytesToString(OAuthServerHelper.RandomBytes(OAuthHelper.Keys.ChallengeSaltLength))
                }
            };

            var json = new DynamicJsonValue
            {
                [OAuthHelper.Keys.RSAExponent] = OAuthServerHelper.RSAExponent,
                [OAuthHelper.Keys.RSAModulus] = OAuthServerHelper.RSAModulus,
                [OAuthHelper.Keys.Challenge] =
                    OAuthServerHelper.EncryptSymmetric(OAuthHelper.DictionaryToString(challengeData))
            };

            await SendResponse(webSocket, json).ConfigureAwait(false);
        }

        private string ExtractChallengeResponse(BlittableJsonReaderObject reader)
        {
            string requestContents;
            if (reader.TryGet("ChallengeResponse", out requestContents) == false)
            {
                throw new InvalidOperationException("Missing 'ChallengeResponse' property");
            }

            if (requestContents.Length > MaxOAuthContentLength)
            {
                throw new InvalidOperationException(
                    "Cannot respond with token to content length " + requestContents.Length + " bigger then " + MaxOAuthContentLength);
            }

            if (requestContents.Length == 0)
            {
                throw new InvalidOperationException("Got zero length requestContent in 'ChallengeResponse' message");
            }
            return requestContents;
        }


        private async Task<AccessToken> ProcessToken(JsonOperationContext context, WebSocket webSocket)
        {
            using (var reader = await context.ReadFromWebSocket(webSocket, DebugTag, ServerStore.ServerShutdown))
            {
                var requestContents = ExtractChallengeResponse(reader);

                var encryptedData = ExtractEncryptedData(requestContents);

                var challengeDictionary = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptAsymmetric(encryptedData));

                var apiKeyName = challengeDictionary.GetOrDefault(OAuthHelper.Keys.APIKeyName);
                var challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);
                var response = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Response);

                if (string.IsNullOrEmpty(apiKeyName) || string.IsNullOrEmpty(challenge) || string.IsNullOrEmpty(response))
                {
                    throw new InvalidOperationException(
                        "Got null or empty apiKeyName/challenge/response in 'ChallengeResponse' message");
                }

                var challengeData = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptSymmetric(challenge));

                var timestampStr = challengeData.GetOrDefault(OAuthHelper.Keys.ChallengeTimestamp);

                if (string.IsNullOrEmpty(timestampStr))
                {
                    throw new InvalidOperationException("Got null or empty encryptedData in 'ChallengeResponse' message");
                }

                ThrowIfTimestampNotVerified(timestampStr);

                string secret;
                var accessToken = BuildAccessTokenAndGetApiKeySecret(apiKeyName, out secret);

                var expectedResponse = OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, secret));

                if (response != expectedResponse)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Failure to authenticate api key {apiKeyName}");
                    return null;
                }
                return accessToken;
            }
        }

        private void ThrowIfTimestampNotVerified(string timestampStr)
        {
            var challengeTimestamp = OAuthServerHelper.ParseDateTime(timestampStr);
            if (challengeTimestamp + MaxChallengeAge < SystemTime.UtcNow || challengeTimestamp > SystemTime.UtcNow)
            {
                throw new InvalidOperationException(
                    "The challenge is either too old or from the future in 'ChallengeResponse' message" +
                    $"challengeTimestamp={challengeTimestamp}, MaxChallengeAge={MaxChallengeAge}, SystemTime.UtcNow={SystemTime.UtcNow}");
            }
        }

        private string ExtractEncryptedData(string requestContents)
        {
            var requestContentsDictionary = OAuthHelper.ParseDictionary(requestContents);

            var rsaExponent = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
            var rsaModulus = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);

            if (rsaExponent == null || rsaModulus == null ||
                !rsaExponent.SequenceEqual(OAuthServerHelper.RSAExponent) ||
                !rsaModulus.SequenceEqual(OAuthServerHelper.RSAModulus))
            {
                throw new InvalidOperationException("Got invalid Exponent/Modulus in requestContent in 'ChallengeResponse' message");
            }

            var encryptedData = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.EncryptedData);
            if (string.IsNullOrEmpty(encryptedData))
            {
                throw new InvalidOperationException("Got null or empty encryptedData in 'ChallengeResponse' message");
            }

            return encryptedData;
        }

        private async Task SendError(WebSocket webSocket, string errorMsg, string exceptionType)
        {
            if (webSocket.State != WebSocketState.Open)
                return;
            var json = new DynamicJsonValue
            {
                ["Error"] = errorMsg,
                ["ExceptionType"] = exceptionType
            };
            try
            {
                await SendResponse(webSocket, json).ConfigureAwait(false);
                await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "Server side error",
                    ServerStore.ServerShutdown);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error sending error to client using web socket", e);
            }
        }

        private AccessToken BuildAccessTokenAndGetApiKeySecret(string apiKeyName, out string secret)
        {

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var apiDoc = ServerStore.Read(context, Constants.ApiKeyPrefix + apiKeyName);

                if (apiDoc == null)
                {
                    throw new InvalidOperationException($"Could not find api key: {apiKeyName}");
                }

                bool apiKeyDefinitionEnabled;
                if (apiDoc.TryGet("Enabled", out apiKeyDefinitionEnabled) == false ||
                    apiKeyDefinitionEnabled == false)
                {
                    throw new InvalidOperationException($"The api key {apiKeyName} has been disabled");
                }

                if (apiDoc.TryGet("Secret", out secret) == false)
                {
                    throw new InvalidOperationException($"Missing 'Secret' property in api kye: {apiKeyName}");
                }

                var databases = new Dictionary<string, AccessModes>(StringComparer.OrdinalIgnoreCase);

                BlittableJsonReaderObject accessMode;
                if (apiDoc.TryGet("ResourcesAccessMode", out accessMode) == false)
                {
                    throw new InvalidOperationException($"Missing 'ResourcesAccessMode' property in api key: {apiKeyName}");
                }

                for (var i = 0; i < accessMode.Count; i++)
                {
                    var dbName = accessMode.GetPropertyByIndex(i);

                    string accessValue;
                    if (accessMode.TryGet(dbName.Item1, out accessValue) == false)
                    {
                        throw new InvalidOperationException($"Missing value of dbName -'{dbName.Item1}' property in api key: {apiKeyName}");
                    }
                    AccessModes mode;
                    if (Enum.TryParse(accessValue, out mode) == false)
                    {
                        throw new InvalidOperationException(
                            $"Invalid value of dbName -'{dbName.Item1}' property in api key: {apiKeyName}, cannot understand: {accessValue}");
                    }
                    databases[dbName.Item1] = mode;
                }

                return new AccessToken
                {
                    Name = apiKeyName,
                    Token = Guid.NewGuid().ToString(),
                    AuthorizedDatabases = databases,
                    Issued = Stopwatch.GetTimestamp()
                };

            }
        }

        private async Task SendResponse(WebSocket webSocket, DynamicJsonValue json)
        {
            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                try
                {
                    using (var ms = new MemoryStream())
                    {
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                            context.Write(writer, json);

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                    }
                }
                catch (Exception ex)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Failed to send json response to the client", ex);
                }
            }
        }
    }
}
