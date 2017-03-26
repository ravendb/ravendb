using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Web.Authentication
{
    public class OAuthApiKeyHandler : RequestHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<OAuthApiKeyHandler>("Raven/Server");
        
        // https://download.libsodium.org/libsodium/content/public-key_cryptography/sealed_boxes.html
        [RavenAction("/oauth/api-key", "POST", "/oauth/api-key body{string}", NoAuthorizationRequired = true)]
        public unsafe Task OauthGetApiKey()
        {
            var apiKeyName = GetStringQueryString("apiKey");
            string secret;
            AccessToken accessToken;
            byte[] hash, publicKey;

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
                using (context.GetManagedBuffer(out pinnedBuffer))
                using (var hashJson = context.ParseToMemory(RequestBodyStream(), "apikey", BlittableJsonDocumentBuilder.UsageMode.None, pinnedBuffer))
                {
                    hashJson.BlittableValidation();

                    object hashString;
                    if (hashJson.TryGetMember("hash", out hashString) == false)
                        throw new InvalidDataException("Missing 'hash' property in the POST request body");

                    object pkString;
                    if (hashJson.TryGetMember("public-key", out pkString) == false)
                        throw new InvalidDataException("Missing 'public-key' property in the POST request body");

                    hash = Convert.FromBase64String(((LazyStringValue)hashString).ToString());
                    publicKey = Convert.FromBase64String(((LazyStringValue)pkString).ToString());
                }

                try
                {
                    accessToken = BuildAccessTokenAndGetApiKeySecret(apiKeyName, out secret);
                }
                catch (AuthenticationException ex)
                {
                    GenerateError(ex.Message, context, (int)HttpStatusCode.Forbidden);
                    return Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    GenerateError(ex.Message, context, (int)HttpStatusCode.InternalServerError);
                    return Task.CompletedTask;
                }
            }

            var verifiedHash = new byte[Sodium.crypto_generichash_bytes_max()];
            if (hash.Length != verifiedHash.Length)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to authenticate api key. Hash length is invalid. Actual : {hash.Length}, Expected: {Sodium.crypto_generichash_bytes_max()}");
                GenerateError($"Hash length is invalid. Actual : {hash.Length}, Expected : {Sodium.crypto_generichash_bytes_max()}", 
                    context, (int)HttpStatusCode.Forbidden);
                return Task.CompletedTask;
            }

            fixed (byte* pk = publicKey)
            fixed (byte* h = hash)
            fixed (byte* vh = verifiedHash)
            fixed (byte* pSecret = Encoding.UTF8.GetBytes(secret))
            {
                Sodium.crypto_generichash(
                    vh,
                    (IntPtr)verifiedHash.Length,
                    pSecret,
                    (ulong)secret.Length,
                    pk, (IntPtr)publicKey.Length
                );

                if (Sodium.sodium_memcmp(h, vh, (IntPtr)verifiedHash.Length) != 0)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failed to authenticate api key with hash length=" + verifiedHash.Length + ". ApiKey=" + apiKeyName);
                    GenerateError("Unable to authenticate api key. Cannot verify hash", context, (int)HttpStatusCode.Forbidden);
                    return Task.CompletedTask;
                }

                var token = Encoding.UTF8.GetBytes(accessToken.Token);
                var cryptedToken = new byte[Sodium.crypto_box_sealbytes() + token.Length];
                fixed (byte* output = cryptedToken)
                fixed (byte* pToken = token)
                {
                    if (Sodium.crypto_box_seal(output,
                        pToken,
                        (ulong)token.Length,
                        pk) != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failed to authenticate api key. Cannot crypt token with length of " + (ulong)token.Length);
                        GenerateError("Unable to authenticate api key. Cannot crypt token", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                    AccessToken old;
                    if (Server.AccessTokensByName.TryGetValue(accessToken.Name, out old))
                    {
                        AccessToken value;
                        Server.AccessTokensByName.TryRemove(old.Name, out value);
                    }

                    Server.AccessTokensById[accessToken.Token] = accessToken;
                    Server.AccessTokensByName[accessToken.Name] = accessToken;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("cryptedToken");
                        writer.WriteString(Convert.ToBase64String(cryptedToken));
                        writer.WriteEndObject();
                        writer.Flush();
                    }
                }
            }
            return Task.CompletedTask;
        }

        private void GenerateError(string error, JsonOperationContext context, int statusCode)
        {
            HttpContext.Response.StatusCode = statusCode;

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Error");
                writer.WriteString(error);
                writer.WriteEndObject();
                writer.Flush();
            }
        }


        private AccessToken BuildAccessTokenAndGetApiKeySecret(string apiKeyName, out string secret)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var apiDoc = ServerStore.Cluster.Read(context, Constants.ApiKeys.Prefix + apiKeyName);

                if (apiDoc == null)
                {
                    throw new AuthenticationException($"Could not find api key: {apiKeyName}");
                }

                bool apiKeyDefinitionEnabled;
                if (apiDoc.TryGet("Enabled", out apiKeyDefinitionEnabled) == false ||
                    apiKeyDefinitionEnabled == false)
                {
                    throw new AuthenticationException($"The api key {apiKeyName} has been disabled");
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
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (var i = 0; i < accessMode.Count; i++)
                {
                    accessMode.GetPropertyByIndex(i, ref prop);

                    string accessValue;
                    if (accessMode.TryGet(prop.Name, out accessValue) == false)
                    {
                        throw new InvalidOperationException($"Missing value of dbName -'{prop.Name}' property in api key: {apiKeyName}");
                    }
                    AccessModes mode;
                    if (Enum.TryParse(accessValue, out mode) == false)
                    {
                        throw new InvalidOperationException(
                            $"Invalid value of dbName -'{prop.Name}' property in api key: {apiKeyName}, cannot understand: {accessValue}");
                    }
                    databases[prop.Name] = mode;
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
    }
}
