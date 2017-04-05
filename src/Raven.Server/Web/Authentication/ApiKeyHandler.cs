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
    public class ApiKeyHandler : RequestHandler
    {
        [RavenAction("/api-key/public-key", "GET", "/api-key/public-key", NoAuthorizationRequired = true)]
        public Task GetPublicKey()
        {

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("PublicKey");
                writer.WriteString(Convert.ToBase64String(Server.PublicKey));
                writer.WriteEndObject();
                writer.Flush();
                return Task.CompletedTask;
            }
        }

        // https://download.libsodium.org/libsodium/content/public-key_cryptography/sealed_boxes.html
        [RavenAction("/api-key/validate", "POST", "/api-key/validate?apiKey={key name} body{string}", NoAuthorizationRequired = true)]
        public unsafe Task OauthGetApiKey()
        {
            var apiKeyName = GetStringQueryString("apiKey");
            string localSecret;
            AccessToken accessToken;
            byte[] remoteCryptedSecret;
            byte[] clientPublicKey;
            byte[] serverKey;
            byte[] remoteNonce;

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
                using (context.GetManagedBuffer(out pinnedBuffer))
                using (var hashJson = context.ParseToMemory(RequestBodyStream(), "apikey", BlittableJsonDocumentBuilder.UsageMode.None, pinnedBuffer))
                {
                    hashJson.BlittableValidation();

                    string secretString;
                    if (hashJson.TryGet("Secret", out secretString) == false)
                    {
                        GenerateError("Missing 'Secret' property", context, (int)HttpStatusCode.BadRequest);
                        return Task.CompletedTask;
                    }
                    string nonceString;
                    if (hashJson.TryGet("Nonce", out nonceString) == false)
                    {
                        GenerateError("Missing 'Nonce' property", context, (int)HttpStatusCode.BadRequest);
                        return Task.CompletedTask;
                    }

                    string pkString;
                    if (hashJson.TryGet("PublicKey", out pkString) == false)
                    {
                        GenerateError("Missing 'PublicKey' property", context, (int)HttpStatusCode.BadRequest);
                        return Task.CompletedTask;
                    }
                    string serverKeyString;
                    if (hashJson.TryGet("ServerKey", out serverKeyString) == false)
                    {
                        GenerateError("Missing 'ServerKey' property", context, (int)HttpStatusCode.BadRequest);
                        return Task.CompletedTask;
                    }

                    remoteNonce = Convert.FromBase64String(nonceString);
                    remoteCryptedSecret = Convert.FromBase64String(secretString);
                    clientPublicKey = Convert.FromBase64String(pkString);
                    serverKey = Convert.FromBase64String(serverKeyString);
                }

                try
                {
                    accessToken = BuildAccessTokenAndGetApiKeySecret(apiKeyName, out localSecret);
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

            var localSecretAsBytes = Encoding.UTF8.GetBytes(localSecret);
            var hashLen = Sodium.crypto_generichash_bytes_max();
            var hashBuffer = new byte[hashLen];

            fixed (byte* hash = hashBuffer)
            fixed (byte* server_sk = Server.SecretKey)
            fixed (byte* server_pk = Server.PublicKey)
            fixed (byte* client_pk = clientPublicKey)
            fixed (byte* m = remoteCryptedSecret)
            fixed (byte* n = remoteNonce)
            fixed (byte* serverKeyFromClient = serverKey)
            fixed (byte* pLocalSecret = localSecretAsBytes)
            {
                if (Server.PublicKey.Length != serverKey.Length ||
                    Sodium.sodium_memcmp(serverKeyFromClient, server_pk, (IntPtr)Server.PublicKey.Length) != 0)
                {
                    GenerateError("The server public key is not valid", context, (int)HttpStatusCode.ExpectationFailed);
                    return Task.CompletedTask;
                }

                if (remoteCryptedSecret.Length != hashLen + Sodium.crypto_box_macbytes())
                {
                    GenerateError("Unable to authenticate api key. Invalid secret size", context, (int)HttpStatusCode.Forbidden);
                    return Task.CompletedTask;
                }

                if (Sodium.crypto_box_open_easy(m, m, remoteCryptedSecret.Length, n, client_pk, server_sk) != 0)
                {
                    GenerateError("Unable to authenticate api key. Cannot open box", context, (int)HttpStatusCode.Forbidden);
                    return Task.CompletedTask;
                }

                if (Sodium.crypto_generichash(hash, (IntPtr)hashBuffer.Length, pLocalSecret, (ulong)localSecretAsBytes.Length, client_pk, (IntPtr)clientPublicKey.Length) != 0)
                {
                    GenerateError("Unable to authenticate api key. Cannot generate hash", context, (int)HttpStatusCode.Forbidden);
                    return Task.CompletedTask;
                }

                if (Sodium.sodium_memcmp(hash, m, (IntPtr)hashLen) != 0)
                {
                    GenerateError("Unable to authenticate api key. Cannot verify hash", context, (int)HttpStatusCode.Forbidden);
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


                var token = new byte[Encoding.UTF8.GetByteCount(accessToken.Token) + Sodium.crypto_box_macbytes()];
                var tokenLen = Encoding.UTF8.GetBytes(accessToken.Token, 0, accessToken.Token.Length, token, 0);
                fixed (byte* c = token)
                {
                    Sodium.randombytes_buf(n, remoteNonce.Length);
                    if (Sodium.crypto_box_easy(c, c, tokenLen, n, client_pk, server_sk) != 0)
                    {
                        GenerateError("Unable to crypt token", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Token");
                        writer.WriteString(Convert.ToBase64String(token));
                        writer.WriteComma();
                        writer.WritePropertyName("Nonce");
                        writer.WriteString(Convert.ToBase64String(remoteNonce));
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

                var apiDoc = ServerStore.Read(context, Constants.ApiKeys.Prefix + apiKeyName);

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
