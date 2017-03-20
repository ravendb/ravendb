using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
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

        public async Task<string> GetAuthenticationTokenAsync(string apiKey, string url, JsonOperationContext context)
        {
            if (string.IsNullOrEmpty(apiKey))
                return null;
            var oauthSource = url + "/OAuth/API-Key";
            return await AuthenticateAsync(oauthSource, apiKey, context).ConfigureAwait(false);
        }

        public async Task<string> AuthenticateAsync(string url, string apiKey, JsonOperationContext context)
        {
            try
            {
                var apiKeyParts = ExtractApiKeyAndSecret(apiKey);
                var apiKeyName = apiKeyParts[0].Trim();
                var apiSecret = apiKeyParts[1].Trim();

                var publicKey = new byte[Sodium.crypto_box_publickeybytes()];
                var privateKey = new byte[Sodium.crypto_box_secretkeybytes()];
                var hash = new byte[Sodium.crypto_generichash_bytes_max()];

                unsafe
                {
                    fixed (byte* pubKey = publicKey)
                    fixed (byte* prvKey = privateKey)
                    fixed (byte* h = hash)
                    fixed (byte* pSecret = Encoding.UTF8.GetBytes(apiSecret))
                    {
                        Sodium.crypto_box_keypair(pubKey, prvKey);

                        if (Sodium.crypto_generichash(
                                h,
                                (IntPtr)hash.Length,
                                pSecret,
                                (ulong)(apiSecret.Length),
                                pubKey,
                                (IntPtr)publicKey.Length
                            ) != 0)
                        {
                            throw new AuthenticationException("Failed to generate hash for secret of apikey name=" + apiKeyName);
                        }
                    }
                }

                var request = CreateRequest(hash, publicKey, context);
                request.RequestUri = new Uri($"{url}?apiKey={apiKeyName}");
                request.Headers.Add("Raven-Client-Version", RequestExecutor.ClientVersion);

                var response = await Client.SendAsync(request);
                if (response.StatusCode != HttpStatusCode.OK &&
                    response.StatusCode != HttpStatusCode.Forbidden &&
                    response.StatusCode != HttpStatusCode.InternalServerError)
                {
                    throw new AuthenticationException("Bad response from server " + response.StatusCode);
                }

                var stream = await response.Content.ReadAsStreamAsync();
               
                JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
                using (context.GetManagedBuffer(out pinnedBuffer))
                using (var tokenJson = context.ParseToMemory(stream, "apikey", BlittableJsonDocumentBuilder.UsageMode.None, pinnedBuffer))
                {
                    tokenJson.BlittableValidation();

                    object errorString;
                    if (tokenJson.TryGetMember("Error", out errorString) == true)
                        throw new AuthenticationException((LazyStringValue)errorString);

                    object cryptedToken;
                    if (tokenJson.TryGetMember("cryptedToken", out cryptedToken) == false)
                        throw new InvalidDataException("Missing 'cryptedToken' property in the POST request body");

                    var cryptTokenBytes = Convert.FromBase64String(((LazyStringValue)cryptedToken).ToString());

                    var token = new byte[cryptTokenBytes.Length - Sodium.crypto_box_sealbytes()];
                    unsafe
                    {
                        fixed (byte* pubKey = publicKey)
                        fixed (byte* prvKey = privateKey)
                        fixed (byte* input = cryptTokenBytes)
                        fixed (byte* output = token)
                        {
                            if (Sodium.crypto_box_seal_open(output, input, (ulong)cryptTokenBytes.Length, pubKey, prvKey) != 0)
                                throw new AuthenticationException(
                                    @"Unable to authenticate api key (message corrupted or not intended for this recipient");

                            return Encoding.UTF8.GetString(token);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to DoOAuthRequest to {url} with {apiKey}", ex);

                //if (ex is AuthenticationException)
                //    throw;

                //throw new AuthenticationException(ex.Message, ex);
                throw;
            }
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

        public HttpRequestMessage CreateRequest(byte[] hash, byte[] publicKey, JsonOperationContext context)
        {
            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    {
                        var hashString = Convert.ToBase64String(hash);
                        var pkString = Convert.ToBase64String(publicKey);

                        writer.WriteStartObject();
                        writer.WritePropertyName("hash");
                        writer.WriteString(hashString);
                        writer.WriteComma();
                        writer.WritePropertyName("public-key");
                        writer.WriteString(pkString);
                        writer.WriteEndObject();
                    }
                })
            };
        }
    }
}