using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Lucene.Net.Support;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Server.Authentication
{
    public class OAuthApiKeyHandler : RequestHandler
    {
        private const string debugTag = "/oauth/api-key";
        private static readonly ILog Logger = LogManager.GetLogger(typeof(OAuthApiKeyHandler));
        private const int MaxOAuthContentLength = 1500;
        private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

        [RavenAction("/oauth/api-key", "GET", "/oauth/api-key", SkipTryAuthorized = true)]
        public async Task OauthGetApiKey()
        {
            try
            {
                using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                {
                    MemoryOperationContext context;
                    using (ServerStore.ContextPool.AllocateOperationContext(out context))
                    {
                        await SendInitialChallenge(webSocket);

                        var reply = await context.ReadFromWebSocket(webSocket, debugTag, ServerStore.ServerShutdown);

                        await ResponseWithToken(reply, webSocket);

                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Close by server",
                                ServerStore.ServerShutdown);
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("WebSocket was closed from client");
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.DebugException("Got exception while handling /oauth/api-key EP", ex);
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

        private async Task ResponseWithToken(BlittableJsonReaderObject reader, WebSocket webSocket)
        {
            try
            {
                var requestContents = ExtractChallengeResponse(reader);

                var encryptedData = ExtractEncryptedData(requestContents);

                string apiKeyName, challenge, timestampStr, response;
                ExtractDecryptedData(encryptedData, out apiKeyName, out challenge, out timestampStr, out response);

                ThrowIfTimestampNotVerified(timestampStr);

                string secret;
                var accessToken = GetApiKeySecret(apiKeyName, out secret);

                var expectedResponse = OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, secret));

                if (response != expectedResponse)
                {
                    await SendError(webSocket, "Unauthorized Client - Invalid Challenge Key");
                    return;
                }

                var tokenKey = Guid.NewGuid().ToString();
                Server.AccessTokensById[tokenKey] = accessToken;

                var jsonToken = new DynamicJsonValue
                {
                    ["currentOauthToken"] = tokenKey
                };

                await SendResponse(webSocket, jsonToken).ConfigureAwait(false);
            }
            catch (Exception ioe)
            {
                try
                {
                    await SendError(webSocket, ioe.ToString());
                }
                catch (Exception e)
                {
                    Log.InfoException("Could not send oauth error to websocket client", e);
                }
            }
        }

        private void ThrowIfTimestampNotVerified(string timestampStr)
        {
            var challengeTimestamp = OAuthServerHelper.ParseDateTime(timestampStr);
            if (challengeTimestamp + MaxChallengeAge < SystemTime.UtcNow || challengeTimestamp > SystemTime.UtcNow)
            {
                throw new InvalidOperationException(
                    "The challenge is either old or from the future in 'ChallengeResponse' message" +
                    $"challengeTimestamp={challengeTimestamp}, MaxChallengeAge={MaxChallengeAge}, SystemTime.UtcNow={SystemTime.UtcNow}");
            }
        }

        private void ExtractDecryptedData(string encryptedData,
            out string apiKeyName, out string challenge, out string timestampStr, out string response)
        {
            timestampStr = null;

            var challengeDictionary = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptAsymmetric(encryptedData));

            apiKeyName = challengeDictionary.GetOrDefault(OAuthHelper.Keys.APIKeyName);
            challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);
            response = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Response);

            if (string.IsNullOrEmpty(apiKeyName) || string.IsNullOrEmpty(challenge) || string.IsNullOrEmpty(response))
            {
                throw new InvalidOperationException(
                    "Got null or empty apiKeyName/challenge/response in 'ChallengeResponse' message");
            }

            var challengeData = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptSymmetric(challenge));

            timestampStr = challengeData.GetOrDefault(OAuthHelper.Keys.ChallengeTimestamp);

            if (string.IsNullOrEmpty(timestampStr))
            {
                throw new InvalidOperationException("Got null or empty encryptedData in 'ChallengeResponse' message");
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

        private async Task SendError(WebSocket webSocket, string errorMsg)
        {
            var json = new DynamicJsonValue
            {
                ["Error"] = errorMsg
            };
            await SendResponse(webSocket, json).ConfigureAwait(false);
        }

        private AccessTokenBody GetApiKeySecret(string apiKeyName, out string secret)
        {

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var apiDoc = ServerStore.Read(context, Constants.ApiKeyPrefix + apiKeyName);

                if (apiDoc == null)
                {
                    throw new InvalidOperationException($"Could not find document ${Constants.ApiKeyPrefix}{apiKeyName}");
                }

                string apiKeyDefinitionEnabled;
                if (apiDoc.TryGet<string>("Enabled", out apiKeyDefinitionEnabled) == false ||
                    apiKeyDefinitionEnabled.Equals("False", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("Unauthorized Client - Unknown API Key");
                }

                if (apiDoc.TryGet("Secret", out secret) == false)
                {
                    throw new InvalidOperationException("Missing 'Secret' property in " + Constants.ApiKeyPrefix + apiKeyName);
                }

                List<ResourceAccess> databases = new EquatableList<ResourceAccess>();

                BlittableJsonReaderObject accessMode;
                if (apiDoc.TryGet("AccessMode", out accessMode) == false)
                {
                    throw new InvalidOperationException("Missing 'AccessMode' property in " + Constants.ApiKeyPrefix + apiKeyName);
                }

                for (var i = 0; i < accessMode.Count; i++)
                {
                    var dbName = accessMode.GetPropertyByIndex(i);

                    string accessValue;
                    if (accessMode.TryGet(dbName.Item1, out accessValue) == false)
                    {
                        throw new InvalidOperationException(
                            "Missing value of dbName -'" + dbName.Item1 + "' property in " + Constants.ApiKeyPrefix + apiKeyName);
                    }

                    databases.Add(new ResourceAccess
                    {
                        TenantId = dbName.Item1,
                        AccessMode = accessValue
                    });
                }

                return new AccessTokenBody
                {
                    UserId = apiKeyName,
                    AuthorizedDatabases = databases,
                    Issued = Stopwatch.GetTimestamp()
                };

            }
        }

        private async Task SendResponse(WebSocket webSocket, DynamicJsonValue json)
        {
            MemoryOperationContext context;
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
                    Log.ErrorException("Failed to send json response to the client", ex);
                }
            }
        }
    }
}
