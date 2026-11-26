using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Utils;
using Sparrow.Platform;

namespace Raven.Server.Commercial
{
    public sealed class LetsEncryptClient
    {
        private static readonly AsyncRetryPolicy<HttpResponseMessage> RetryPolicy;

        private static readonly JsonSerializerOptions JwsMessageOptions = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = true,
            IncludeFields = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private static Dictionary<string, RavenHttpClient> _cachedClients = new(StringComparer.OrdinalIgnoreCase);

        private static RavenHttpClient GetCachedClient(string url)
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

                value = new RavenHttpClient
                {
                    BaseAddress = new Uri(url)
                };

                _cachedClients = new Dictionary<string, RavenHttpClient>(_cachedClients, StringComparer.OrdinalIgnoreCase)
                {
                    [url] = value
                };
                return value;
            }
        }

        static LetsEncryptClient()
        {
            var maxRetryAfterDelta = TimeSpan.FromMinutes(1);

            RetryPolicy = Policy
                .HandleResult<HttpResponseMessage>(message => message.IsSuccessStatusCode == false && message.Headers.RetryAfter != null)
                .WaitAndRetryAsync(
                    retryCount: 5,
                    sleepDurationProvider: (retry, result, _) =>
                    {
                        if (result.Result.Headers.RetryAfter.Delta.HasValue)
                        {
                            var retryAfter = result.Result.Headers.RetryAfter.Delta.Value;
                            if (retryAfter > maxRetryAfterDelta)
                                return maxRetryAfterDelta;

                            return retryAfter;
                        }

                        return TimeSpan.FromSeconds(retry);
                    },
                    onRetryAsync: (_, _, _, _) => Task.CompletedTask);
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
        private readonly string _directoryPath;
        private string _nonce;
        private RSACryptoServiceProvider _accountKey;
        private RegistrationCache _cache;
        private RavenHttpClient _client;
        private Directory _directory;
        private List<AuthorizationChallenge> _challenges = new List<AuthorizationChallenge>();
        private Order _currentOrder;

        public LetsEncryptClient(string url)
        {
            _url = url ?? throw new ArgumentNullException(nameof(url));
            _directoryPath = new Uri(_url).LocalPath.TrimStart('/');
            if (string.IsNullOrEmpty(_directoryPath))
                throw new ArgumentNullException(nameof(_directoryPath), "Url does not contain directory path");

            _path = GetCachePath(_url);
        }

        public async Task Init(string email, CancellationToken token = default(CancellationToken))
        {
            _accountKey = new RSACryptoServiceProvider(4096);
            _client = GetCachedClient(_url);
            (_directory, _) = await SendAsync<Directory>(HttpMethod.Get, new Uri(_directoryPath, UriKind.Relative), null, token);

            if (File.Exists(_path))
            {
                bool success;
                try
                {
                    lock (Locker)
                    {
                        _cache = JsonStringHelper.Deserialize<RegistrationCache>(File.ReadAllText(_path));
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
                    JsonStringHelper.Serialize(_cache, indented: true));
            }
        }

        private async Task<(TResult Result, string Response)> SendAsync<TResult>(HttpMethod method, Uri uri, object message, CancellationToken token) where TResult : class
        {
            var response = await SendAsyncInternal(method, uri, message, token);

            if (response.Content.Headers.ContentType.MediaType == "application/problem+json")
            {
                var problemJson = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);
                var problem = JsonStringHelper.Deserialize<Problem>(problemJson);
                problem.RawJson = problemJson;
                throw new LetsEncryptException(problem, response);
            }

            var responseText = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);

            if (typeof(TResult) == typeof(string)
                && response.Content.Headers.ContentType.MediaType == "application/pem-certificate-chain")
            {
                return ((TResult)(object)responseText, null);
            }

            var responseContent = JsonStringHelper.Deserialize<TResult>(responseText);
            if (responseContent == null)
                throw new LetsEncryptException($"Got an empty response from '{uri}' with status {response.StatusCode}");

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
                HttpResponseMessage response;
                HttpRequestMessage lastRequest = null;
                try
                {
                    response = await RetryPolicy.ExecuteAsync(t =>
                    {
                        lastRequest = CreateRequest();
                        return _client.SendAsync(lastRequest, t);
                    }, token, continueOnCapturedContext: false).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (retries-- > 0)
                    {
                        continue;
                    }

                    throw new InvalidOperationException($"Let's Encrypt client failed to send the request (with retries): {lastRequest}", e);
                }

                if (response.Headers.TryGetValues("Replay-Nonce", out var vals))
                    _nonce = vals.FirstOrDefault();
                else
                    _nonce = null;

                if (response.Content.Headers.ContentType.MediaType == "application/problem+json")
                {
                    var problemJson = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);

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
                        throw new InvalidOperationException($"Let's Encrypt client failed to send the request (with retries): {lastRequest}. Problem: {problemJson}", e);
                    }
                }

                if (response.IsSuccessStatusCode || hasNonce || _nonce == null)
                {
                    return response; // either successful or no point in retry
                }
                hasNonce = true; // we only allow it once
            } while (true);

            HttpRequestMessage CreateRequest()
            {
                var request = new HttpRequestMessage(method, uri);
                if (message == null) 
                    return request;

                var encodedMessage = _jws.Encode(message, new JwsHeader
                {
                    Nonce = _nonce,
                    Url = uri
                });

                var json = JsonSerializer.Serialize(encodedMessage, JwsMessageOptions);

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

                return request;
            }
        }

        public async Task<Dictionary<string, string>> NewOrder(string[] hostnames, string profile = null, CancellationToken token = default(CancellationToken))
        {
            _challenges.Clear();
            var dto = new Order
            {
                Expires = DateTime.UtcNow.AddDays(2),
                Identifiers = hostnames.Select(hostname => new OrderIdentifier
                {
                    Type = "dns",
                    Value = hostname
                }).ToArray()
            };
            
            if (string.IsNullOrEmpty(profile) == false)
            {
                dto.Profile = profile;
            }
            var (order, response) = await SendAsync<Order>(HttpMethod.Post, _directory.NewOrder, dto, token);

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
                try
                {
                    // From: https://tools.ietf.org/html/rfc8555#section-7.5.1
                    // The first request of AuthorizationChallenge is POST with {} in the body.
                    // Then, all subsequent requests of AuthorizationChallenge (to get the status of the challenge) are POST-AS_GET with an empty body.
                    (result, _) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, challenge.Url, "{}", token);

                    if (result.Status == "pending" || result.Status == "processing")
                    {
                        await WaitForStatusAsync(challenge.Url, new List<string> { "valid" }, token);
                    }
                }
                catch (Exception e)
                {
                    result = null;
                    string responseText = null;
                    try
                    {
                        // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                        (result, responseText) = await SendAsync<AuthorizationChallengeResponse>(HttpMethod.Post, challenge.Url, string.Empty, token);
                        if (result.Status == "valid")
                            continue;
                    }
                    catch
                    {
                        // if we failed here, throw the original error
                        // since err isn't set
                    }

                    if (result == null)
                        throw;

                    throw new InvalidOperationException("Failed to complete challenge because: " + result.Error?.Detail +
                        Environment.NewLine + responseText, e);
                }
            }
        }

        public async Task<(X509Certificate2 Cert, RSA PrivateKey)> GetCertificate(RSA existingKey = null, string acmeProfile = null, CancellationToken token = default(CancellationToken))
        {
            var key = existingKey?.GetExportableRsaPrivateKey() ?? RSA.Create(4096);

            var csr = new CertificateRequest("CN=" + _currentOrder.Identifiers[0].Value,
                key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            var san = new SubjectAlternativeNameBuilder();
            foreach (var host in _currentOrder.Identifiers)
                san.AddDnsName(host.Value);

            csr.CertificateExtensions.Add(san.Build());

            Order response;
            string responseText;
            // https://community.letsencrypt.org/t/acme-client-finalized-order-stuck-on-ready-state/165196/6
            foreach (var authorization in _currentOrder.Authorizations)
            {
                await WaitForStatusAsync(authorization, new List<string> { "valid" }, token);
            }

            await WaitForStatusAsync(_currentOrder.Location, new List<string> { "ready" }, token);

            try
            {
                (response, _) = await SendAsync<Order>(HttpMethod.Post, _currentOrder.Finalize,
                    new FinalizeRequest { CSR = Jws.Base64UrlEncoded(csr.CreateSigningRequest()) }, token);
            }
            catch (Exception)
            {
                (response, _) = await SendAsync<Order>(HttpMethod.Post, _currentOrder.Location, string.Empty, token);
                if (response.Status != "processing" && response.Status != "valid")
                    throw;
            }


            while (true)
            {
                // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                (response, responseText) = await SendAsync<Order>(HttpMethod.Post, _currentOrder.Location, string.Empty, token);

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

            var cert = CertificateLoaderUtil.CreateCertificateFromAny(Encoding.UTF8.GetBytes(pem));

            byte[] blob = null;
            switch (key)
            {
                case RSACng rsaCng:
                    // This is not supposed to happen, since RSACng is only used in Windows, so suppressing the warning.
                    if (PlatformDetails.RunningOnPosix)
                        throw new PlatformNotSupportedException($"The private key of the current server certificate is {nameof(RSACng)} which is only supported in Windows.");

                    RSAParameters parameters = rsaCng.ExportParameters(true);
                    var newRsaCsp = new RSACryptoServiceProvider();
                    newRsaCsp.ImportParameters(parameters);
                    blob = newRsaCsp.ExportCspBlob(true);
                    break;

                case RSACryptoServiceProvider rsaCsp:
                    blob = rsaCsp.ExportCspBlob(true);
                    break;
            }
            
            var cacheCertKey = LetsEncryptSetupUtils.GetCertCacheKey(acmeProfile, _currentOrder.Identifiers[0].Value);

            _cache.CachedCerts[cacheCertKey] = new CertificateCache
            {
                Cert = pem,
                Private = blob
            };

            lock (Locker)
            {
                File.WriteAllText(_path,
                    JsonStringHelper.Serialize(_cache, indented: true));
            }

            return (cert, key);
        }

        public sealed class CachedCertificateResult
        {
            public RSA PrivateKey;
            public X509Certificate2 Certificate;
        }

        private async Task WaitForStatusAsync(Uri uri, List<string> statusesToWaitFor, CancellationToken token = default)
        {
            while (true)
            {
                // post-as-get (https://community.letsencrypt.org/t/acme-v2-scheduled-deprecation-of-unauthenticated-resource-gets/74380)
                var (response, _) = await SendAsync<Order>(HttpMethod.Post, uri, string.Empty, token);
                if (statusesToWaitFor.Contains(response.Status))
                    break;

                await Task.Delay(500, token);
            }
        }

        public bool TryGetCachedCertificate(string certCacheKey, out CachedCertificateResult value)
        {
            value = null;
            if (_cache.CachedCerts.TryGetValue(certCacheKey, out var cache) == false)
            {
                return false;
            }

            if (cache.Private == null || cache.Cert == null)
                return false;

            var cert = CertificateLoaderUtil.CreateCertificateFromAny(Encoding.UTF8.GetBytes(cache.Cert));

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

        public void ResetCachedCertificate(IEnumerable<string> certCacheKeysToRemove)
        {
            foreach (var host in certCacheKeysToRemove)
            {
                _cache.CachedCerts.Remove(host);
            }
        }

        internal static string GetCachePath(string acmeUrl)
        {
            var home = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);
            var hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(acmeUrl));
            var file = Jws.Base64UrlEncoded(hash) + ".lets-encrypt.cache.json";
            return Path.Combine(home, file);
        }

        private sealed class RegistrationCache
        {
            [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
            public readonly Dictionary<string, CertificateCache> CachedCerts = new Dictionary<string, CertificateCache>(StringComparer.OrdinalIgnoreCase);
            public byte[] AccountKey;
            public string Id;
            public Jwk Key;
            public Uri Location;
        }

        private sealed class CertificateCache
        {
            public string Cert;
            public byte[] Private;
        }

        private sealed class AuthorizationChallengeResponse
        {
            [JsonPropertyName("identifier")]
            public OrderIdentifier Identifier { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; }

            [JsonPropertyName("expires")]
            public DateTime? Expires { get; set; }

            [JsonPropertyName("wildcard")]
            public bool Wildcard { get; set; }

            [JsonPropertyName("challenges")]
            public AuthorizationChallenge[] Challenges { get; set; }

            [JsonPropertyName("error")]
            public Problem Error { get; set; }
        }

        private sealed class AuthorizeChallenge
        {
            [JsonPropertyName("keyAuthorization")]
            public string KeyAuthorization { get; set; }
        }

        private sealed class AuthorizationChallenge
        {
            [JsonPropertyName("type")]
            public string Type { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; }

            [JsonPropertyName("url")]
            public Uri Url { get; set; }

            [JsonPropertyName("token")]
            public string Token { get; set; }

            [JsonPropertyName("error")]
            public Problem Error { get; set; }
        }

        private sealed class Jwk
        {
            [JsonPropertyName("kty")]
            public string KeyType { get; set; }

            [JsonPropertyName("kid")]
            public string KeyId { get; set; }

            [JsonPropertyName("use")]
            public string Use { get; set; }

            [JsonPropertyName("n")]
            public string Modulus { get; set; }

            [JsonPropertyName("e")]
            public string Exponent { get; set; }

            [JsonPropertyName("d")]
            public string D { get; set; }

            [JsonPropertyName("p")]
            public string P { get; set; }

            [JsonPropertyName("q")]
            public string Q { get; set; }

            [JsonPropertyName("dp")]
            public string DP { get; set; }

            [JsonPropertyName("dq")]
            public string DQ { get; set; }

            [JsonPropertyName("qi")]
            public string InverseQ { get; set; }

            [JsonPropertyName("alg")]
            public string Algorithm { get; set; }
        }

        private sealed class Directory
        {
            [JsonPropertyName("keyChange")]
            public Uri KeyChange { get; set; }

            [JsonPropertyName("newNonce")]
            public Uri NewNonce { get; set; }

            [JsonPropertyName("newAccount")]
            public Uri NewAccount { get; set; }

            [JsonPropertyName("newOrder")]
            public Uri NewOrder { get; set; }

            [JsonPropertyName("revokeCert")]
            public Uri RevokeCertificate { get; set; }

            [JsonPropertyName("meta")]
            public DirectoryMeta Meta { get; set; }
        }

        private sealed class DirectoryMeta
        {
            [JsonPropertyName("termsOfService")]
            public string TermsOfService { get; set; }
        }

        public sealed class Problem
        {
            [JsonPropertyName("type")]
            public string Type { get; set; }

            [JsonPropertyName("detail")]
            public string Detail { get; set; }

            public string RawJson { get; set; }
        }

        public sealed class LetsEncryptException : Exception
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

        private sealed class JwsMessage
        {
            [JsonPropertyName("header")]
            public JwsHeader Header { get; set; }

            [JsonPropertyName("protected")]
            public string Protected { get; set; }

            [JsonPropertyName("payload")]
            public string Payload { get; set; }

            [JsonPropertyName("signature")]
            public string Signature { get; set; }
        }

        private sealed class JwsHeader
        {
            public JwsHeader()
            {
            }

            public JwsHeader(string algorithm, Jwk key)
            {
                Algorithm = algorithm;
                Key = key;
            }

            [JsonPropertyName("alg")]
            public string Algorithm { get; set; }

            [JsonPropertyName("jwk")]
            public Jwk Key { get; set; }

            [JsonPropertyName("kid")]
            public string KeyId { get; set; }

            [JsonPropertyName("nonce")]
            public string Nonce { get; set; }

            [JsonPropertyName("url")]
            public Uri Url { get; set; }
        }

        private interface IHasLocation
        {
            Uri Location { get; set; }
        }

        private sealed class Order : IHasLocation
        {
            public Uri Location { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; }

            [JsonPropertyName("expires")]
            public DateTime? Expires { get; set; }

            [JsonPropertyName("identifiers")]
            public OrderIdentifier[] Identifiers { get; set; }

            [JsonPropertyName("notBefore")]
            public DateTime? NotBefore { get; set; }

            [JsonPropertyName("notAfter")]
            public DateTime? NotAfter { get; set; }

            [JsonPropertyName("error")]
            public Problem Error { get; set; }

            [JsonPropertyName("authorizations")]
            public Uri[] Authorizations { get; set; }

            [JsonPropertyName("finalize")]
            public Uri Finalize { get; set; }

            [JsonPropertyName("certificate")]
            public Uri Certificate { get; set; }
            
            [JsonPropertyName("profile")]
            public string Profile { get; set; }
        }

        private sealed class OrderIdentifier
        {
            [JsonPropertyName("type")]
            public string Type { get; set; }

            [JsonPropertyName("value")]
            public string Value { get; set; }
        }

        private sealed class Account : IHasLocation
        {
            [JsonPropertyName("termsOfServiceAgreed")]
            public bool TermsOfServiceAgreed { get; set; }

            [JsonPropertyName("contact")]
            public string[] Contacts { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; }

            [JsonPropertyName("id")]
            public string Id { get; set; }

            [JsonPropertyName("createdAt")]
            public DateTime CreatedAt { get; set; }

            [JsonPropertyName("key")]
            public Jwk Key { get; set; }

            [JsonPropertyName("initialIp")]
            public string InitialIp { get; set; }

            [JsonPropertyName("orders")]
            public Uri Orders { get; set; }

            public Uri Location { get; set; }
        }

        private sealed class FinalizeRequest
        {
            [JsonPropertyName("csr")]
            public string CSR { get; set; }
        }

        private sealed class Jws
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
                    encodedPayload = Base64UrlEncoded(JsonStringHelper.Serialize(payload));
                }

                var message = new JwsMessage
                {
                    Payload = encodedPayload,
                    Protected = Base64UrlEncoded(JsonStringHelper.Serialize(protectedHeader))
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
