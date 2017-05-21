using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Security;
using Raven.Client.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Http.OAuth
{
    internal class ApiKeyAuthenticator
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ApiKeyAuthenticator>("Client");
        private static readonly HttpClient Client = new HttpClient();

        private readonly ConcurrentDictionary<string, byte[]> _serverPublicKeys = new ConcurrentDictionary<string, byte[]>();

        public async Task<string> GetAuthenticationTokenAsync(string apiKey, string serverUrl, JsonOperationContext context)
        {
            if (string.IsNullOrEmpty(apiKey))
                return null;

            if (_serverPublicKeys.TryGetValue(serverUrl, out var serverPk) == false)
            {
                serverPk = await GetServerPublicKey(context, serverUrl);
                _serverPublicKeys.TryAdd(serverUrl, serverPk);
            }

            try
            {
                var publicKey = new byte[Sodium.crypto_box_publickeybytes()];
                var privateKey = new byte[Sodium.crypto_box_secretkeybytes()];

                var request = BuildGetTokenRequest(context, apiKey, serverUrl, serverPk, privateKey, publicKey);

                var response = await Client.SendAsync(request);
                if (response.StatusCode == HttpStatusCode.ExpectationFailed)
                {
                    // the server pk is invalid, retry, once

                    var oldServerPk = serverPk;
                    serverPk = await GetServerPublicKey(context, serverUrl);
                    _serverPublicKeys.TryUpdate(serverUrl, serverPk, oldServerPk);
                    request = BuildGetTokenRequest(context, apiKey, serverUrl, serverPk, privateKey, publicKey);
                    response = await Client.SendAsync(request);
                }

                if (response.StatusCode != HttpStatusCode.OK &&
                    response.StatusCode != HttpStatusCode.Forbidden &&
                    response.StatusCode != HttpStatusCode.InternalServerError)
                {
                    throw new AuthenticationException("Bad response from server " + response.StatusCode);
                }

                JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
                using (var stream = await response.Content.ReadAsStreamAsync())
                using (context.GetManagedBuffer(out pinnedBuffer))
                using (var tokenJson = context.ParseToMemory(stream, "apikey", BlittableJsonDocumentBuilder.UsageMode.None, pinnedBuffer))
                {
                    tokenJson.BlittableValidation();

                    object errorString;
                    if (tokenJson.TryGetMember("Error", out errorString) == true)
                        throw new AuthenticationException((LazyStringValue)errorString);

                    string cryptedToken;
                    if (tokenJson.TryGet("Token", out cryptedToken) == false)
                        throw new InvalidDataException("Missing 'Token' property in the POST request body");

                    string serverNonce;
                    if (tokenJson.TryGet("Nonce", out serverNonce) == false)
                        throw new InvalidDataException("Missing 'Nonce' property in the POST request body");

                    var cryptTokenBytes = Convert.FromBase64String(cryptedToken);
                    var nonceBytes = Convert.FromBase64String(serverNonce);

                    unsafe
                    {
                        fixed (byte* client_sk = privateKey)
                        fixed (byte* server_pk = serverPk)
                        fixed (byte* input = cryptTokenBytes)
                        fixed (byte* n = nonceBytes)
                        {
                            if (Sodium.crypto_box_open_easy(input, input, (ulong)cryptTokenBytes.Length, n, server_pk, client_sk) != 0)
                                throw new AuthenticationException(
                                    @"Unable to authenticate api key (message corrupted or not intended for this recipient");

                            return Encodings.Utf8.GetString(cryptTokenBytes, 0, cryptTokenBytes.Length - Sodium.crypto_box_macbytes());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to DoOAuthRequest to {serverUrl} with {apiKey}", ex);

                //if (ex is AuthenticationException)
                //    throw;

                //throw new AuthenticationException(ex.Message, ex);
                throw;
            }
        }

        private unsafe HttpRequestMessage BuildGetTokenRequest(JsonOperationContext context, string apiKey, string serverUrl, byte[] serverPk, byte[] privateKey, byte[] publicKey)
        {
            var url = serverUrl + "/api-key/validate";
            var apiKeyParts = ExtractApiKeyAndSecret(apiKey);
            var apiKeyName = apiKeyParts[0].Trim();
            var apiSecret = apiKeyParts[1].Trim();

            var nonce = new byte[Sodium.crypto_box_noncebytes()];
            var hashLen = Sodium.crypto_generichash_bytes_max();
            var apiSecretBytes = Encodings.Utf8.GetBytes(apiSecret);
            var buffer = new byte[hashLen + Sodium.crypto_box_macbytes()];

            fixed (byte* server_pk = serverPk)
            fixed (byte* client_sk = privateKey)
            fixed (byte* client_pk = publicKey)
            fixed (byte* n = nonce)
            fixed (byte* c = buffer)
            fixed (byte* bytes = apiSecretBytes)
            {
                Sodium.crypto_box_keypair(client_pk, client_sk);
                Sodium.randombytes_buf(n, (UIntPtr)nonce.Length);


                if (Sodium.crypto_generichash(c, (UIntPtr)hashLen, bytes, (ulong)apiSecretBytes.Length, client_pk, (UIntPtr)publicKey.Length) != 0)
                    throw new InvalidOperationException("Unable to generate hash");

                if (Sodium.crypto_box_easy(
                        c,
                        c,
                        (ulong)hashLen,
                        n,
                        server_pk,
                        client_sk
                    ) != 0)
                {
                    throw new AuthenticationException("Failed to generate crypt secret of apikey name=" + apiKeyName);
                }
            }

            var request = CreateRequest(buffer, publicKey, serverPk, nonce, context);
            request.RequestUri = new Uri($"{url}?apiKey={apiKeyName}");
            request.Headers.Add("Raven-Client-Version", RequestExecutor.ClientVersion);
            return request;
        }

        private static async Task<byte[]> GetServerPublicKey(JsonOperationContext context, string serverUrl)
        {
            byte[] serverPk;
            var message = await Client.GetAsync(serverUrl + "/api-key/public-key");
            message.EnsureSuccessStatusCode();
            JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
            using (var stream = await message.Content.ReadAsStreamAsync())
            using (context.GetManagedBuffer(out pinnedBuffer))
            using (var pkJson = context.ParseToMemory(stream, "publicKey", BlittableJsonDocumentBuilder.UsageMode.None, pinnedBuffer))
            {
                if (pkJson.TryGet("PublicKey", out string pkStr) == false)
                    throw new InvalidOperationException("Could not get server public key " + serverUrl);

                serverPk = Convert.FromBase64String(pkStr);
            }
            return serverPk;
        }

        private string[] ExtractApiKeyAndSecret(string apiKey)
        {
            var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);
            if (apiKeyParts.Length > 2)
            {
                apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
            }

            if (apiKeyParts.Length < 2)
                throw new AuthenticationException("Invalid Api-Key. Contains less then two parts separated with slash");

            return apiKeyParts;
        }

        public HttpRequestMessage CreateRequest(byte[] crypted, byte[] publicKey, byte[] serverPk, byte[] nonce, JsonOperationContext context)
        {
            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Secret");
                        writer.WriteString(Convert.ToBase64String(crypted));
                        writer.WriteComma();
                        writer.WritePropertyName("PublicKey");
                        writer.WriteString(Convert.ToBase64String(publicKey));
                        writer.WriteComma();
                        writer.WritePropertyName("Nonce");
                        writer.WriteString(Convert.ToBase64String(nonce));
                        writer.WritePropertyName("ServerKey");
                        writer.WriteString(Convert.ToBase64String(serverPk));
                        writer.WriteComma();
                        writer.WriteEndObject();
                    }
                })
            };
        }
    }
}