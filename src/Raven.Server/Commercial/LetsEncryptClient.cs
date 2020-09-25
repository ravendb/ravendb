using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Server.Commercial
{
    public class LetsEncryptClient
    {
        public const string StagingV2 = "https://acme-staging-v02.api.letsencrypt.org/directory";
        public const string ProductionV2 = "https://acme-v02.api.letsencrypt.org/directory";

        private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented
        };

        private static Dictionary<string, HttpClient> _cachedClients = new Dictionary<string, HttpClient>(StringComparer.OrdinalIgnoreCase);

        private static HttpClient GetCachedClient(string url)
        {
            if (_cachedClients.TryGetValue(url, out var value))
            {
                return value;
            }

            lock (Locker)
            {
                if (_cachedClients.TryGetValue(url, out value))
                {
                    return value;
                }

                value = new HttpClient
                {
                    BaseAddress = new Uri(url)
                };

                _cachedClients = new Dictionary<string, HttpClient>(_cachedClients, StringComparer.OrdinalIgnoreCase)
                {
                    [url] = value
                };
                return value;
            }
        }

        /// <summary>
        ///     In our scenario, we assume a single single wizard progressing
        ///     and the locking is basic to the wizard progress. Adding explicit
        ///     locking to be sure that we are not corrupting disk state if user
        ///     is explicitly calling stuff concurrently (running the setup wizard
        ///     from two tabs?)
        /// </summary>
        private static readonly object Locker = new object();

        private Jws _jws;
        private readonly string _path;
        private readonly string _url;
        private string _nonce;
        private RSACryptoServiceProvider _accountKey;
        private RegistrationCache _cache;
        private HttpClient _client;
        private Directory _directory;
        private List<AuthorizationChallenge> _challenges = new List<AuthorizationChallenge>();
        private Order _currentOrder;

        public LetsEncryptClient(string url)
        {
            _url = url ?? throw new ArgumentNullException(nameof(url));

            var home = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(url));
            var file = Jws.Base64UrlEncoded(hash) + ".lets-encrypt.cache.json";
            _path = Path.Combine(home, file);
        }

        public async Task Init(string email, CancellationToken token = default(CancellationToken))
        {
            _accountKey = new RSACryptoServiceProvider(4096);
            _client = GetCachedClient(_url);
            (_directory, _) = await SendAsync<Directory>(HttpMethod.Get, new Uri("directory", UriKind.Relative), null, token);

            // Use this when testing against pebble
            //(_directory, _) = await SendAsync<Directory>(HttpMethod.Get, new Uri("dir", UriKind.Relative), null, token);

            if (File.Exists(_path))
            {
                bool success;
                try
                {
                    lock (Locker)
                    {
                        _cache = JsonConvert.DeserializeObject<RegistrationCache>(File.ReadAllText(_path));
                    }

                    _accountKey.ImportCspBlob(_cache.AccountKey);

                    // From: https://community.letsencrypt.org/t/acme-v2-strict-jws-kid-header-processing/63321
                    // "KeyID headers contain the full account URL as returned by the Location header in a newAccount response"
                    var kid = _cache.Location.ToString();

                    _jws = new Jws(_accountKey, kid);
                    success = true;
                }
                catch
                {
                    success = false;
                    // if we failed for any reason, we'll just
                    // generate a new registration
                }

                if (success)
                {
                    return;
                }
            }

            _jws = new Jws(_accountKey, null);
            var (account, response) = await SendAsync<Account>(HttpMethod.Post, _directory.NewAccount, new Account
            {
                // we validate this in the UI before we get here, so that is fine
                TermsOfServiceAgreed = true,
                Contacts = new[] { "mailto:" + email },
            }, token);
            _jws.SetKeyId(account);

            if (account.Status != "valid")
                throw new InvalidOperationException("Account status is not valid, was: " + account.Status + Environment.NewLine + response);

            lock (Locker)
            {
                _cache = new RegistrationCache
                {
                    Location = account.Location,
                    AccountKey = _accountKey.ExportCspBlob(true),
                    Id = account.Id,
                    Key = account.Key
                };
                File.WriteAllText(_path,
                    JsonConvert.SerializeObject(_cache, Formatting.Indented));
            }
        }

        private async Task<(TResult Result, string Response)> SendAsync<TResult>(HttpMethod method, Uri uri, object message, CancellationToken token) where TResult : class
        {
            var response = await SendAsyncInternal(method, uri, message, token);

            if (response.Content.Headers.ContentType.MediaType == "application/problem+json")
            {
                var problemJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                var problem = JsonConvert.DeserializeObject<Problem>(problemJson);
                problem.RawJson = problemJson;
                throw new LetsEncryptException(problem, response);
            }

            var responseText = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

            if (typeof(TResult) == typeof(string)
                && response.Content.Headers.ContentType.MediaType == "application/pem-certificate-chain")
            {
                return ((TResult)(object)responseText, null);
            }

            var responseContent = JObject.Parse(responseText).ToObject<TResult>();

            if (responseContent is IHasLocation ihl)
            {
                if (response.Headers.Location != null)
                    ihl.Location = response.Headers.Location;
            }

            return (responseContent, responseText);
        }

        private async Task<HttpResponseMessage> SendAsyncInternal(HttpMethod method, Uri uri, object message, CancellationToken token)
        {
            var hasNonce = _nonce != null;
            var retries = 3;
            do
            {
                var request = new HttpRequestMessage(method, uri);
                if (message != null)
                {
                    var encodedMessage = _jws.Encode(message, new JwsHeader
                    {
                        Nonce = _nonce,
                        Url = uri
                    });
                    var json = JsonConvert.SerializeObject(encodedMessage, jsonSettings);

                    request.Content = new StringContent(json, Encoding.UTF8, "application/jose+json")
                    {
                        Headers =
                        {
                            ContentType =
                            {
                                CharSet = string.Empty
                            }
                        }
                    };
                }

                HttpResponseMessage response;
                try
                {
                    response = await _client.SendAsync(request, token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (retries-- > 0)
                    {
                        continue;
                    }

                    throw new InvalidOperationException($"Let's Encrypt client failed to send the request (with retries): {request}", e);
                }

                if (response.Headers.TryGetValues("Replay-Nonce", out var vals))
                    _nonce = vals.FirstOrDefault();
                else
                    _nonce = null;

                if (response.Content.Headers.ContentType.MediaType == "application/problem+json")
                {
                    var problemJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                    if (retries-- > 0)
                    {
                        continue;
                    }

                    try
                    {
                        response.EnsureSuccessStatusCode();
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Let's Encrypt client failed to send the request (with retries): {request}. Problem: {problemJson}", e);
                    }
                }

                if (response.IsSuccessStatusCode || hasNonce || _nonce == null)
                {
                    return response; // either successful or no point in retry
                }
                hasNonce = true; // we only allow it once
            } while (true);
        }

        public async Task<Dictionary<string, string>> NewOrder(string[] hostnames, CancellationToken token = default(CancellationToken))
        {
            _challenges.Clear();
            var (order, response) = await SendAsync<Order>(HttpMethod.Post, _directory.NewOrder, new Order
            {
                Expires = DateTime.UtcNow.AddDays(2),
                Identifiers = hostnames.Select(hostname => new OrderIdentifier
                {
                    Type = "dns",
                    Value = hostname
                }).ToArray()
            }, token);

            if (order.Status != "pending" && order.Status != "ready")
                throw new InvalidOperationException("Created new order and expected status 'pending' or 'ready', but got: " + order.Status + Environment.NewLine +
                    response);
            _currentOrder = order;
            var results = new Dictionary<string, string>();
            foreach (var item in order.Authorizations)
            {
                // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                var (challengeResponse, responseText) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, item, string.Empty, token);

                var challenge = challengeResponse.Challenges.First(x => x.Type == "dns-01");
                _challenges.Add(challenge);
                var keyToken = _jws.GetKeyAuthorization(challenge.Token);
                using (var sha256 = SHA256.Create())
                {
                    var dnsToken = Jws.Base64UrlEncoded(sha256.ComputeHash(Encoding.UTF8.GetBytes(keyToken)));
                    results[challengeResponse.Identifier.Value.ToLowerInvariant()] = dnsToken;
                }

                if (challengeResponse.Status == "valid")
                    continue;

                if (challengeResponse.Status != "pending")
                    throw new InvalidOperationException("Expected authorization status 'pending', but got: " + order.Status +
                        Environment.NewLine + responseText);
            }

            return results;
        }

        public async Task CompleteChallenges(CancellationToken token = default(CancellationToken))
        {
            for (var index = 0; index < _challenges.Count; index++)
            {
                var challenge = _challenges[index];

                AuthorizationChallengeResponse result;
                string responseText;
                try
                {
                    // From: https://tools.ietf.org/html/rfc8555#section-7.5.1
                    // The first request of AuthorizationChallenge is POST with {} in the body.
                    // Then, all subsequent requests of AuthorizationChallenge (to get the status of the challenge) are POST-AS_GET with an empty body.
                    (result, responseText) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, challenge.Url, "{}", token);

                    while (result.Status == "pending")
                    {
                        // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                        (result, responseText) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, challenge.Url, string.Empty, token);

                        await Task.Delay(500, token);
                    }
                }
                catch (Exception e)
                {
                    AuthorizationChallengeResponse err = null;
                    string errorText = null;
                    try
                    {
                        // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                        (err, errorText) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, challenge.Url, string.Empty, token);
                    }
                    catch
                    {
                        // if we failed here, throw the original error
                        // since err isn't set
                    }

                    if (err == null)
                        throw;

                    throw new InvalidOperationException("Failed to complete challenge because: " + err.Error?.Detail +
                        Environment.NewLine + errorText, e);
                }
            }
        }

        public async Task<(X509Certificate2 Cert, RSA PrivateKey)> GetCertificate(RSA existingKey = null, CancellationToken token = default(CancellationToken))
        {
            var key = existingKey ?? new RSACryptoServiceProvider(4096);

            var csr = new CertificateRequest("CN=" + _currentOrder.Identifiers[0].Value,
                key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            var san = new SubjectAlternativeNameBuilder();
            foreach (var host in _currentOrder.Identifiers)
                san.AddDnsName(host.Value);

            csr.CertificateExtensions.Add(san.Build());

            var (response, responseText) = await SendAsync<Order>(HttpMethod.Post, _currentOrder.Finalize, new FinalizeRequest
            {
                CSR = Jws.Base64UrlEncoded(csr.CreateSigningRequest())
            }, token);

            while (true)
            {
                // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                (response, responseText) = await SendAsync<Order>(HttpMethod.Post, response.Location, string.Empty, token);

                if (response.Status == "valid")
                {
                    if (response.Certificate == null)
                        throw new InvalidOperationException($"Got a valid order status: '{response.Status}' but the certificate field is null! Response: '{responseText}'");

                    break;
                }

                if (response.Status == "processing")
                {
                    await Task.Delay(500);
                    continue;
                }
                throw new InvalidOperationException("Invalid order status: " + response.Status + Environment.NewLine +
                    responseText);
            }
            // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
            var (pem, _) = await SendAsync<string>(HttpMethod.Post, response.Certificate, string.Empty, token);

            var cert = new X509Certificate2(Encoding.UTF8.GetBytes(pem), (string)null, X509KeyStorageFlags.MachineKeySet);

            byte[] blob = null;
            switch (key)
            {
                case RSACng rsaCng:
                    var parameters = rsaCng.ExportParameters(true);
                    var newRsaCsp = new RSACryptoServiceProvider();
                    newRsaCsp.ImportParameters(parameters);
                    blob = newRsaCsp.ExportCspBlob(true);
                    break;

                case RSACryptoServiceProvider rsaCsp:
                    blob = rsaCsp.ExportCspBlob(true);
                    break;
            }

            _cache.CachedCerts[_currentOrder.Identifiers[0].Value] = new CertificateCache
            {
                Cert = pem,
                Private = blob
            };

            lock (Locker)
            {
                File.WriteAllText(_path,
                    JsonConvert.SerializeObject(_cache, Formatting.Indented));
            }

            return (cert, key);
        }

        public class CachedCertificateResult
        {
            public RSA PrivateKey;
            public X509Certificate2 Certificate;
        }

        public bool TryGetCachedCertificate(string host, out CachedCertificateResult value)
        {
            value = null;
            if (_cache.CachedCerts.TryGetValue(host, out var cache) == false)
            {
                return false;
            }

            if (cache.Private == null || cache.Cert == null)
                return false;

            var cert = new X509Certificate2(Encoding.UTF8.GetBytes(cache.Cert), (string)null, X509KeyStorageFlags.MachineKeySet);

            // if it is about to expire, we need to refresh
            if ((cert.NotAfter - DateTime.UtcNow).TotalDays < 14)
                return false;

            var rsa = new RSACryptoServiceProvider(4096);
            rsa.ImportCspBlob(cache.Private);

            value = new CachedCertificateResult
            {
                Certificate = cert,
                PrivateKey = rsa
            };
            return true;
        }

        public string GetTermsOfServiceUri(CancellationToken token = default(CancellationToken))
        {
            return _directory.Meta.TermsOfService;
        }

        public void ResetCachedCertificate(IEnumerable<string> hostsToRemove)
        {
            foreach (var host in hostsToRemove)
            {
                _cache.CachedCerts.Remove(host);
            }
        }

        private class RegistrationCache
        {
            public readonly Dictionary<string, CertificateCache> CachedCerts = new Dictionary<string, CertificateCache>(StringComparer.OrdinalIgnoreCase);
            public byte[] AccountKey;
            public string Id;
            public Jwk Key;
            public Uri Location;
        }

        private class CertificateCache
        {
            public string Cert;
            public byte[] Private;
        }

        private class AuthorizationChallengeResponse
        {
            [JsonProperty("identifier")]
            public OrderIdentifier Identifier { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("expires")]
            public DateTime? Expires { get; set; }

            [JsonProperty("wildcard")]
            public bool Wildcard { get; set; }

            [JsonProperty("challenges")]
            public AuthorizationChallenge[] Challenges { get; set; }

            [JsonProperty("error")]
            public Problem Error { get; set; }
        }

        private class AuthorizeChallenge
        {
            [JsonProperty("keyAuthorization")]
            public string KeyAuthorization { get; set; }
        }

        private class AuthorizationChallenge
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("url")]
            public Uri Url { get; set; }

            [JsonProperty("token")]
            public string Token { get; set; }

            [JsonProperty("error")]
            public Problem Error { get; set; }
        }

        private class Jwk
        {
            [JsonProperty("kty")]
            public string KeyType { get; set; }

            [JsonProperty("kid")]
            public string KeyId { get; set; }

            [JsonProperty("use")]
            public string Use { get; set; }

            [JsonProperty("n")]
            public string Modulus { get; set; }

            [JsonProperty("e")]
            public string Exponent { get; set; }

            [JsonProperty("d")]
            public string D { get; set; }

            [JsonProperty("p")]
            public string P { get; set; }

            [JsonProperty("q")]
            public string Q { get; set; }

            [JsonProperty("dp")]
            public string DP { get; set; }

            [JsonProperty("dq")]
            public string DQ { get; set; }

            [JsonProperty("qi")]
            public string InverseQ { get; set; }

            [JsonProperty("alg")]
            public string Algorithm { get; set; }
        }

        private class Directory
        {
            [JsonProperty("keyChange")]
            public Uri KeyChange { get; set; }

            [JsonProperty("newNonce")]
            public Uri NewNonce { get; set; }

            [JsonProperty("newAccount")]
            public Uri NewAccount { get; set; }

            [JsonProperty("newOrder")]
            public Uri NewOrder { get; set; }

            [JsonProperty("revokeCert")]
            public Uri RevokeCertificate { get; set; }

            [JsonProperty("meta")]
            public DirectoryMeta Meta { get; set; }
        }

        private class DirectoryMeta
        {
            [JsonProperty("termsOfService")]
            public string TermsOfService { get; set; }
        }

        public class Problem
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("detail")]
            public string Detail { get; set; }

            public string RawJson { get; set; }
        }

        public class LetsEncryptException : Exception
        {
            public LetsEncryptException(Problem problem, HttpResponseMessage response)
                : base($"{problem.Type}: {problem.Detail}")
            {
                Problem = problem;
                Response = response;
            }

            public LetsEncryptException()
            {
            }

            public LetsEncryptException(string message) : base(message)
            {
            }

            public LetsEncryptException(string message, Exception innerException) : base(message, innerException)
            {
            }

            public Problem Problem { get; }

            public HttpResponseMessage Response { get; }
        }

        private class JwsMessage
        {
            [JsonProperty("header")]
            public JwsHeader Header { get; set; }

            [JsonProperty("protected")]
            public string Protected { get; set; }

            [JsonProperty("payload")]
            public string Payload { get; set; }

            [JsonProperty("signature")]
            public string Signature { get; set; }
        }

        private class JwsHeader
        {
            public JwsHeader()
            {
            }

            public JwsHeader(string algorithm, Jwk key)
            {
                Algorithm = algorithm;
                Key = key;
            }

            [JsonProperty("alg")]
            public string Algorithm { get; set; }

            [JsonProperty("jwk")]
            public Jwk Key { get; set; }

            [JsonProperty("kid")]
            public string KeyId { get; set; }

            [JsonProperty("nonce")]
            public string Nonce { get; set; }

            [JsonProperty("url")]
            public Uri Url { get; set; }
        }

        private interface IHasLocation
        {
            Uri Location { get; set; }
        }

        private class Order : IHasLocation
        {
            public Uri Location { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("expires")]
            public DateTime? Expires { get; set; }

            [JsonProperty("identifiers")]
            public OrderIdentifier[] Identifiers { get; set; }

            [JsonProperty("notBefore")]
            public DateTime? NotBefore { get; set; }

            [JsonProperty("notAfter")]
            public DateTime? NotAfter { get; set; }

            [JsonProperty("error")]
            public Problem Error { get; set; }

            [JsonProperty("authorizations")]
            public Uri[] Authorizations { get; set; }

            [JsonProperty("finalize")]
            public Uri Finalize { get; set; }

            [JsonProperty("certificate")]
            public Uri Certificate { get; set; }
        }

        private class OrderIdentifier
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("value")]
            public string Value { get; set; }
        }

        private class Account : IHasLocation
        {
            [JsonProperty("termsOfServiceAgreed")]
            public bool TermsOfServiceAgreed { get; set; }

            [JsonProperty("contact")]
            public string[] Contacts { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("createdAt")]
            public DateTime CreatedAt { get; set; }

            [JsonProperty("key")]
            public Jwk Key { get; set; }

            [JsonProperty("initialIp")]
            public string InitialIp { get; set; }

            [JsonProperty("orders")]
            public Uri Orders { get; set; }

            public Uri Location { get; set; }
        }

        private class FinalizeRequest
        {
            [JsonProperty("csr")]
            public string CSR { get; set; }
        }

        private class Jws
        {
            private readonly Jwk _jwk;
            private readonly RSA _rsa;

            public Jws(RSA rsa, string keyId)
            {
                _rsa = rsa ?? throw new ArgumentNullException(nameof(rsa));

                var publicParameters = rsa.ExportParameters(false);

                _jwk = new Jwk
                {
                    KeyType = "RSA",
                    Exponent = Base64UrlEncoded(publicParameters.Exponent),
                    Modulus = Base64UrlEncoded(publicParameters.Modulus),
                    KeyId = keyId
                };
            }

            public JwsMessage Encode<TPayload>(TPayload payload, JwsHeader protectedHeader)
            {
                protectedHeader.Algorithm = "RS256";
                if (_jwk.KeyId != null)
                {
                    protectedHeader.KeyId = _jwk.KeyId;
                }
                else
                {
                    protectedHeader.Key = _jwk;
                }

                string encodedPayload;

                if (payload is string p)
                {
                    switch (p)
                    {
                        case "":
                            // From: https://tools.ietf.org/html/rfc8555#section-6.3
                            encodedPayload = string.Empty;
                            break;

                        case "{}":
                            // From: https://tools.ietf.org/html/rfc8555#section-7.5.1
                            encodedPayload = Base64UrlEncoded("{}");
                            break;

                        default:
                            throw new ArgumentException(nameof(payload));
                    }
                }
                else
                {
                    encodedPayload = Base64UrlEncoded(JsonConvert.SerializeObject(payload));
                }

                var message = new JwsMessage
                {
                    Payload = encodedPayload,
                    Protected = Base64UrlEncoded(JsonConvert.SerializeObject(protectedHeader))
                };

                message.Signature = Base64UrlEncoded(
                    _rsa.SignData(Encoding.ASCII.GetBytes(message.Protected + "." + message.Payload),
                        HashAlgorithmName.SHA256,
                        RSASignaturePadding.Pkcs1));

                return message;
            }

            private string GetSha256Thumbprint()
            {
                var json = "{\"e\":\"" + _jwk.Exponent + "\",\"kty\":\"RSA\",\"n\":\"" + _jwk.Modulus + "\"}";

                using (var sha256 = SHA256.Create())
                {
                    return Base64UrlEncoded(sha256.ComputeHash(Encoding.UTF8.GetBytes(json)));
                }
            }

            public string GetKeyAuthorization(string token)
            {
                return token + "." + GetSha256Thumbprint();
            }

            public static string Base64UrlEncoded(string s)
            {
                return Base64UrlEncoded(Encoding.UTF8.GetBytes(s));
            }

            public static string Base64UrlEncoded(byte[] arg)
            {
                var s = Convert.ToBase64String(arg); // Regular base64 encoder
                s = s.Split('=')[0]; // Remove any trailing '='s
                s = s.Replace('+', '-'); // 62nd char of encoding
                s = s.Replace('/', '_'); // 63rd char of encoding
                return s;
            }

            internal void SetKeyId(Account account)
            {
                // From: https://community.letsencrypt.org/t/acme-v2-strict-jws-kid-header-processing/63321
                // "KeyID headers contain the full account URL as returned by the Location header in a newAccount response"
                _jwk.KeyId = account.Location.ToString();
            }
        }
    }
}
