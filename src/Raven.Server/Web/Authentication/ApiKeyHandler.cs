using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
                writer.WriteString(Convert.ToBase64String(ServerStore.BoxPublicKey));
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

                string localSecret;
                ArraySegment<byte> accessTokenBytes;
                try
                {
                    accessTokenBytes = BuildAccessTokenAndGetApiKeySecret(apiKeyName, out localSecret);
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


                var localSecretAsBytes = Encoding.UTF8.GetBytes(localSecret);
                var hashLen = Sodium.crypto_generichash_bytes_max();
                var hashBuffer = new byte[hashLen];

                fixed (byte* hash = hashBuffer)
                fixed (byte* server_sk = ServerStore.BoxSecretKey)
                fixed (byte* server_pk = ServerStore.BoxPublicKey)
                fixed (byte* client_pk = clientPublicKey)
                fixed (byte* m = remoteCryptedSecret)
                fixed (byte* n = remoteNonce)
                fixed (byte* serverKeyFromClient = serverKey)
                fixed (byte* pLocalSecret = localSecretAsBytes)
                {
                    if (ServerStore.BoxPublicKey.Length != serverKey.Length ||
                        Sodium.sodium_memcmp(serverKeyFromClient, server_pk, (UIntPtr)ServerStore.BoxPublicKey.Length) != 0)
                    {
                        GenerateError("The server public key is not valid", context, (int)HttpStatusCode.ExpectationFailed);
                        return Task.CompletedTask;
                    }

                    if (remoteCryptedSecret.Length != hashLen + Sodium.crypto_box_macbytes())
                    {
                        GenerateError("Unable to authenticate api key. Invalid secret size", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    if (Sodium.crypto_box_open_easy(m, m, (ulong)remoteCryptedSecret.Length, n, client_pk, server_sk) != 0)
                    {
                        GenerateError("Unable to authenticate api key. Cannot open box", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    if (Sodium.crypto_generichash(hash, (UIntPtr)hashBuffer.Length, pLocalSecret, (ulong)localSecretAsBytes.Length, client_pk,
                            (UIntPtr)clientPublicKey.Length) != 0)
                    {
                        GenerateError("Unable to authenticate api key. Cannot generate hash", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    if (Sodium.sodium_memcmp(hash, m, (UIntPtr)hashLen) != 0)
                    {
                        GenerateError("Unable to authenticate api key. Cannot verify hash", context, (int)HttpStatusCode.Forbidden);
                        return Task.CompletedTask;
                    }

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                    var token = new byte[accessTokenBytes.Count + Sodium.crypto_box_macbytes()];
                    Buffer.BlockCopy(accessTokenBytes.Array, accessTokenBytes.Offset, token, 0, accessTokenBytes.Count);
                    fixed (byte* c = token)
                    {
                        Sodium.randombytes_buf(n, (UIntPtr)remoteNonce.Length);
                        if (Sodium.crypto_box_easy(c, c, (ulong)accessTokenBytes.Count, n, client_pk, server_sk) != 0)
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

        private unsafe ArraySegment<byte> BuildAccessTokenAndGetApiKeySecret(string apiKeyName, out string secret)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                using (context.OpenReadTransaction())
                {
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
                        throw new InvalidOperationException($"Missing 'Secret' property in api key: {apiKeyName}");
                    }
                }
                return SignedTokenGenerator.GenerateToken(context, ServerStore.SignSecretKey, apiKeyName, ServerStore.NodeTag);
            }
        }
    }
}
