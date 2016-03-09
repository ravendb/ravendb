using System;
using System.Collections.Generic;
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
    public class Authenticator : RequestHandler
    {
        private const string debugTag = "/oauth/api-key";
        private static readonly ILog Logger = LogManager.GetLogger(typeof(Authenticator));
        private const int MaxOAuthContentLength = 1500;
        private static readonly TimeSpan MaxChallengeAge = TimeSpan.FromMinutes(10);

        [RavenAction("/oauth/api-key", "GET", "/oauth/api-key")]
        public async Task OauthGetApiKey()
        {
            try
            {
                using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                {
                    MemoryOperationContext context;
                    using (ServerStore.ContextPool.AllocateOperationContext(out context))
                    {
                        await ResponseWithChallenge(webSocket);
                        var reply = await context.ReadFromWebSocket(webSocket, debugTag, ServerStore.ServerShutdown);

                        await ResponseWithToken(reply, webSocket);

                        await context.ReadFromWebSocket(webSocket, debugTag, ServerStore.ServerShutdown); // wait for socket closure

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

        private async Task ResponseWithChallenge(WebSocket webSocket)
        {
            var challengeData = new Dictionary<string, string>
            {
                {OAuthHelper.Keys.ChallengeTimestamp, OAuthServerHelper.DateTimeToString(SystemTime.UtcNow)},
                {
                    OAuthHelper.Keys.ChallengeSalt,
                    OAuthHelper.BytesToString(OAuthServerHelper.RandomBytes(OAuthHelper.Keys.ChallengeSaltLength))
                }
            };

            var responseData = new Dictionary<string, string>
            {
                {OAuthHelper.Keys.RSAExponent, OAuthServerHelper.RSAExponent},
                {OAuthHelper.Keys.RSAModulus, OAuthServerHelper.RSAModulus},
                {
                    OAuthHelper.Keys.Challenge,
                    OAuthServerHelper.EncryptSymmetric(OAuthHelper.DictionaryToString(challengeData))
                }
            };

            var json = new DynamicJsonValue
            {
                ["ResponseCode"] = "412", // HttpStatusCode.PreconditionFailed
                ["WwwAuthenticate"] =
                    $"{OAuthHelper.Keys.WWWAuthenticateHeaderKey} {OAuthHelper.DictionaryToString(responseData)}"
            };

            await SendResponse(webSocket, json).ConfigureAwait(false);
        }

        private async Task ResponseWithToken(BlittableJsonReaderObject reader, WebSocket webSocket)
        {
            string command;
            if (reader.TryGet("Command", out command) == false)
                throw new InvalidDataException("Missing 'Command' property");

            string requestContents;
            if (reader.TryGet("Param", out requestContents) == false)
                throw new InvalidDataException("Missing 'Param' property");

            if (command.Equals("ChallengeResponse") == false)
                throw new InvalidDataException("Missing 'ChallengeResponse' property");

            if (requestContents.Length > MaxOAuthContentLength)
            {
                var json = new DynamicJsonValue
                {
                    ["ResponseCode"] = "400", // HttpStatusCode.PreconditionFailed
                    ["Description"] = $"Cannot respond with token to content length ({requestContents.Length}) bigger then {MaxOAuthContentLength}"
                };
                await SendResponse(webSocket, json).ConfigureAwait(false);
                return;
            }

            if (requestContents.Length == 0)
                throw new InvalidDataException("Got zero length requestContent in 'ChallengeResponse' message");

            var requestContentsDictionary = OAuthHelper.ParseDictionary(requestContents);
            var rsaExponent = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
            var rsaModulus = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
            if (rsaExponent == null || rsaModulus == null ||
                !rsaExponent.SequenceEqual(OAuthServerHelper.RSAExponent) ||
                !rsaModulus.SequenceEqual(OAuthServerHelper.RSAModulus))
            {
                throw new InvalidDataException("Got invalid Exponent/Modulus in requestContent in 'ChallengeResponse' message");
            }

            var encryptedData = requestContentsDictionary.GetOrDefault(OAuthHelper.Keys.EncryptedData);

            if (string.IsNullOrEmpty(encryptedData))
                throw new InvalidDataException("Got null or empty encryptedData in 'ChallengeResponse' message");

            var challengeDictionary = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptAsymmetric(encryptedData));
            var apiKeyName = challengeDictionary.GetOrDefault(OAuthHelper.Keys.APIKeyName);
            var challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);
            var response = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Response);

            if (string.IsNullOrEmpty(apiKeyName) || string.IsNullOrEmpty(challenge) || string.IsNullOrEmpty(response))
                throw new InvalidDataException("Got null or empty apiKeyName/challenge/response in 'ChallengeResponse' message");

            var challengeData = OAuthHelper.ParseDictionary(OAuthServerHelper.DecryptSymmetric(challenge));
            var timestampStr = challengeData.GetOrDefault(OAuthHelper.Keys.ChallengeTimestamp);

            if (string.IsNullOrEmpty(timestampStr))
                throw new InvalidDataException("Got null or empty encryptedData in 'ChallengeResponse' message");

            var challengeTimestamp = OAuthServerHelper.ParseDateTime(timestampStr);
            if (challengeTimestamp + MaxChallengeAge < SystemTime.UtcNow || challengeTimestamp > SystemTime.UtcNow)
            {
                // The challenge is either old or from the future 
                throw new InvalidDataException("The challenge is either old or from the future in 'ChallengeResponse' message" +
                    $"challengeTimestamp={challengeTimestamp}, MaxChallengeAge={MaxChallengeAge}, SystemTime.UtcNow={SystemTime.UtcNow}");
            }

            string responseCode;
            string description;

            var apiKeyTuple = GetApiKeySecret(apiKeyName, out responseCode, out description);

            if (apiKeyTuple == null)
            {
                var json = new DynamicJsonValue
                {
                    ["ResponseCode"] = responseCode,
                    ["Description"] = description
                };
                await SendResponse(webSocket, json).ConfigureAwait(false);
                return;
            }

            var apiSecret = apiKeyTuple.Item1;
            if (string.IsNullOrEmpty(apiKeyName))
            {
                var json = new DynamicJsonValue
                {
                    ["ResponseCode"] = "401", // HttpStatusCode.Unauthorized
                    ["Description"] = $"Unauthorized Client - Invalid API Key"
                };
                await SendResponse(webSocket, json).ConfigureAwait(false);
                return;
            }

            var expectedResponse = OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret));

            if (response != expectedResponse)
            {
                var json = new DynamicJsonValue
                {
                    ["ResponseCode"] = "401", // HttpStatusCode.Unauthorized
                    ["Description"] = $"Unauthorized Client - Invalid Challenge Key"
                };
                await SendResponse(webSocket, json).ConfigureAwait(false);
                return;
            }

            var token = apiKeyTuple.Item2;

            var jsonToken = new DynamicJsonValue
            {
                ["ResponseCode"] = "200",
                ["currentOauthToken"] = token.Serialize()
            };
            await SendResponse(webSocket, jsonToken).ConfigureAwait(false);
        }

        private Tuple<string, AccessToken> GetApiKeySecret(string apiKeyName, out string responseCode, out string description)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var apiDoc = ServerStore.Read(context, Constants.ApiKeyPrefix + apiKeyName);

                if (apiDoc == null)
                {
                    responseCode = "404";
                    description = $"Could not find document ${Constants.ApiKeyPrefix}{apiKeyName}";
                    return null;
                }

                string apiKeyDefinitionEnabled;
                if (apiDoc.TryGet("Enabled", out apiKeyDefinitionEnabled) == false ||
                    apiKeyDefinitionEnabled.Equals("False", StringComparison.OrdinalIgnoreCase))
                {
                    responseCode = "401"; // HttpStatusCode.PreconditionFailed
                    description = "Unauthorized Client - Unknown API Key";
                    return null;
                }

                string secret;
                if (apiDoc.TryGet("Secret", out secret) == false)
                    throw new InvalidDataException($"Missing 'Secret' property in {Constants.ApiKeyPrefix}{apiKeyName}");

                List<ResourceAccess> databases = new EquatableList<ResourceAccess>();

                BlittableJsonReaderObject accessMode;
                if (apiDoc.TryGet("AccessMode", out accessMode) == false)
                    throw new InvalidDataException($"Missing 'AccessMode' property in {Constants.ApiKeyPrefix}{apiKeyName}");

                for (var i = 0; i < accessMode.Count; i++)
                {
                    var dbName = accessMode.GetPropertyByIndex(i);

                    string accessValue;
                    if (accessMode.TryGet(dbName.Item1, out accessValue) == false)
                        throw new InvalidDataException($"Missing value of dbName - '{dbName.Item1}' property in {Constants.ApiKeyPrefix}{apiKeyName}");

                    databases.Add(new ResourceAccess
                    {
                        TenantId = dbName.Item1,
                        AccessMode = accessValue
                    });
                }

                // creating token. response code and description are irrelevant from here.
                responseCode = "N/A";
                description = "N/A";

                return Tuple.Create(secret, AccessToken.Create(OAuthServerHelper.GetOAuthParameters(ServerStore.Configuration),
                    new AccessTokenBody
                    {
                        UserId = apiKeyName,
                        AuthorizedDatabases = databases
                    }));
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
