using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;

namespace Raven.Server.Commercial
{
    public class LetsEncryptClient
    {
        public const string Staging = "https://acme-v01.api.letsencrypt.org/directory";
        public const string ProductionV1 = "https://acme-v01.api.letsencrypt.org/directory";

        
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

        private readonly List<Challenge> _challenges = new List<Challenge>();

        private readonly List<string> _hosts = new List<string>();
        private readonly string _path;
        private readonly string _url;
        private RSACryptoServiceProvider _accountKey;
        private RegistrationCache _cache;
        private AcmeClient _client;

        public LetsEncryptClient(string url)
        {
            _url = url ?? throw new ArgumentNullException(nameof(url));

            string localAppDataPath = null;
            try
            {
                // If we use the SpecialFolderOption.Create option, GetFolderPath fails with ArgumentException if the directory does not exist.
                // Until https://github.com/dotnet/corefx/issues/26677 is fixed, we'll ask for the path and create the directory ourselves.
                localAppDataPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.DoNotVerify);

                if (System.IO.Directory.Exists(localAppDataPath) == false)
                    System.IO.Directory.CreateDirectory(localAppDataPath);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to create the directory: {localAppDataPath}", e);
            }
            
            var hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(url));
            var file = Convert.ToBase64String(hash) + ".lets-encrypt.cache.json";
            _path = Path.Combine(localAppDataPath, file);
        }

        public async Task Init(string email, CancellationToken token = default (CancellationToken))
        {
            _accountKey = new RSACryptoServiceProvider(4096);

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
                    _client = new AcmeClient(GetCachedClient(_url), _accountKey);

                    return;
                }
            }

            // need to generate new account
            _client = new AcmeClient(new HttpClient
            {
                BaseAddress = new Uri(_url)
            }, _accountKey);
            var registration = await _client.RegisterAsync(new NewRegistrationRequest
            {
                Contact = new[] {"mailto:" + email}
            }, token);
            lock (Locker)
            {
                _cache = new RegistrationCache
                {
                    Location = registration.Location,
                    Id = registration.Id,
                    Key = registration.Key,
                    AccountKey = _accountKey.ExportCspBlob(true)
                };
                File.WriteAllText(_path,
                    JsonConvert.SerializeObject(_cache, Formatting.Indented));
            }
        }

        public async Task<string> GetDnsChallenge(string hostname, CancellationToken token = default (CancellationToken))
        {
            _hosts.Add(hostname);

            var challenge = await _client.NewDnsAuthorizationAsync(hostname, token);
            var dnsChallenge = challenge.Challenges.First(x => x.Type == "dns-01");
            var keyToken= _client.GetKeyAuthorization(dnsChallenge.Token);
            var computedDns = Jws.Base64UrlEncoded(SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(keyToken)));

            _challenges.Add(dnsChallenge);

            return computedDns;
        }

        public async Task CompleteChallenges(CancellationToken token = default (CancellationToken))
        {
            for (var index = 0; index < _challenges.Count; index++)
            {
                var challenge = _challenges[index];
                var completeChallenge = await _client.CompleteChallengeAsync(challenge, token);
                if (completeChallenge.Status != "valid")
                    throw new InvalidOperationException("Failed to complete challenge for " + _hosts[index] + Environment.NewLine +
                                                        JsonConvert.SerializeObject(challenge));
            }
        }

        public async Task<(byte[] Cert, RSA  PrivateKey)> GetCertificate(CancellationToken token = default (CancellationToken))
        {
            var key = new RSACryptoServiceProvider(4096); 
            var csr = new CertificateRequest("CN=" + _hosts[0],
                key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            var san = new SubjectAlternativeNameBuilder();
            foreach (var host in _hosts)
                san.AddDnsName(host);

            csr.CertificateExtensions.Add(san.Build());
            var certResponse = await _client.NewCertificateRequestAsync(csr.CreateSigningRequest(), token);
            _cache.CachedCerts[_hosts[0]] = new CertificateCache
            {
                Cert = certResponse.Certificate, 
                Private = key.ExportCspBlob(true)
            };

            lock (Locker)
            {
                File.WriteAllText(_path,
                    JsonConvert.SerializeObject(_cache, Formatting.Indented));
            }

            return (certResponse.Certificate, key);
        }

        public class CachedCertificateResult
        {
            public RSA PrivateKey;
            public byte[] Certificate;
        }

        public bool TryGetCachedCertificate(List<string> hosts, out CachedCertificateResult value)
        {
            value = null;
            if (_cache.CachedCerts.TryGetValue(hosts[0], out var cache) == false)
            {
                return false;
            }

            var cert = new X509Certificate2(cache.Cert, (string)null, X509KeyStorageFlags.MachineKeySet);

            var sanNames = cert.Extensions["2.5.29.17"];

            if (sanNames == null)
                return false;

            var generalNames = GeneralNames.GetInstance(Asn1Object.FromByteArray(sanNames.RawData));

            var certHosts = generalNames.GetNames();
            foreach (var host in _hosts)
            {
                var found = false;

                foreach (var certHost in certHosts)
                    if (host.Equals(certHost.Name.ToString(), StringComparison.OrdinalIgnoreCase))
                    {
                        found = true;
                        break;
                    }

                if (found == false)
                    return false;
            }

            // if it is about to expire, we need to refresh
            if ((cert.NotAfter - DateTime.UtcNow).TotalDays <= 30)
                return false;

            var rsa = new RSACryptoServiceProvider(4096);
            rsa.ImportCspBlob(cache.Private);

            value = new CachedCertificateResult
            {
                Certificate = cache.Cert,
                PrivateKey = rsa
            };
            return true;
        }
        
        
        public async Task<string> GetTermsOfServiceUri(CancellationToken token = default (CancellationToken))
        {
            var dir = await _client.EnsureDirectoryAsync(token);
            return dir.Meta.TermsOfService;
        }

        private class RegistrationCache
        {
            public readonly Dictionary<string, CertificateCache> CachedCerts = new Dictionary<string, CertificateCache>(StringComparer.OrdinalIgnoreCase);
            public byte[] AccountKey;
            public string Id;
            public Jwk Key;
            public string Location;
        }

        private class CertificateCache
        {
            public byte[] Cert;
            public byte[] Private;
        }

        #region ACME client 

        // the code below was taken from Oocx.Acme.Net project
        // https://github.com/oocx/acme.net/tree/97e7b1a1c44b6b4505b7b56de9594e2709fe1fd0
        // 
        // We want to use something that is self contained and easily modifiable by us. 

        private class AcmeClient
        {
            private readonly HttpClient _client;

            private readonly Jws _jws;
            private Directory _directory;

            private string _nonce;

            public AcmeClient(HttpClient client, RSA key)
            {
                _client = client ?? throw new ArgumentNullException(nameof(client));

                _jws = new Jws(key);
            }


            public string GetKeyAuthorization(string token)
            {
                return _jws.GetKeyAuthorization(token);
            }

            public async Task<Directory> GetDirectoryAsync(CancellationToken token = default (CancellationToken))
            {
                return await GetAsync<Directory>(new Uri("directory", UriKind.Relative), token).ConfigureAwait(false);
            }

            private void RememberNonce(HttpResponseMessage response)
            {
                try
                {
                    _nonce = response.Headers.GetValues("Replay-Nonce").First();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Cannot remember nonce", e);
                }
            }

            public async Task<RegistrationResponse> RegisterAsync(NewRegistrationRequest request, CancellationToken token = default (CancellationToken))
            {
                await EnsureDirectoryAsync(token).ConfigureAwait(false);

                if (request.Agreement == null) request.Agreement = _directory.Meta.TermsOfService;

                try
                {
                    var registration = await PostAsync<RegistrationResponse>(
                        _directory.NewRegistration,
                        request,
                        token
                    ).ConfigureAwait(false);

                    return registration;
                }
                catch (AcmeException ex) when (ex.Response.StatusCode == HttpStatusCode.Conflict)
                {
                    var location = ex.Response.Headers.Location.ToString();

                    var response = await PostAsync<RegistrationResponse>(
                        new Uri(location),
                        new UpdateRegistrationRequest(),
                        token
                    ).ConfigureAwait(false);

                    if (string.IsNullOrEmpty(response.Location)) response.Location = location;

                    return response;
                }
            }

            public async Task<Directory> EnsureDirectoryAsync(CancellationToken token = default (CancellationToken))
            {
                if (_directory == null || _nonce == null) 
                    _directory = await GetDirectoryAsync(token).ConfigureAwait(false);
                return _directory;
            }

            public async Task<RegistrationResponse> UpdateRegistrationAsync(UpdateRegistrationRequest request, CancellationToken token = default (CancellationToken))
            {
                await EnsureDirectoryAsync().ConfigureAwait(false);

                return await PostAsync<RegistrationResponse>(new Uri(request.Location), request, token).ConfigureAwait(false);
            }

            public async Task<AuthorizationResponse> NewDnsAuthorizationAsync(string dnsName, CancellationToken token = default (CancellationToken))
            {
                await EnsureDirectoryAsync().ConfigureAwait(false);

                var authorization = new AuthorizationRequest
                {
                    Resource = "new-authz",
                    Identifier = new Identifier("dns", dnsName)
                };

                return await PostAsync<AuthorizationResponse>(_directory.NewAuthorization, authorization, token).ConfigureAwait(false);
            }

            public async Task<Challenge> CompleteChallengeAsync(Challenge challenge, CancellationToken token = default (CancellationToken))
            {
                var challangeRequest = new KeyAuthorizationRequest
                {
                    KeyAuthorization = _jws.GetKeyAuthorization(challenge.Token)
                };

                challenge = await PostAsync<Challenge>(challenge.Uri, challangeRequest,token).ConfigureAwait(false);

                while (challenge?.Status == "pending")
                {
                    await Task.Delay(1000,token).ConfigureAwait(false);

                    challenge = await GetAsync<Challenge>(challenge.Uri,token).ConfigureAwait(false);
                }

                return challenge;
            }

            public async Task<CertificateResponse> NewCertificateRequestAsync(byte[] csr, CancellationToken token = default (CancellationToken))
            {
                await EnsureDirectoryAsync().ConfigureAwait(false);

                var request = new AcmeCertificateRequest
                {
                    Csr = Jws.Base64UrlEncoded(csr)
                };

                var response = await PostAsync<CertificateResponse>(
                    _directory.NewCertificate,
                    request,
                    token
                ).ConfigureAwait(false);

                return response;
            }

            #region Helpers

            private async Task<TResult> GetAsync<TResult>(Uri uri, CancellationToken token) where TResult : class
            {
                return await SendAsync<TResult>(HttpMethod.Get, uri, null,token).ConfigureAwait(false);
            }

            private async Task<TResult> PostAsync<TResult>(Uri uri, object message, CancellationToken token) where TResult : class
            {
                return await SendAsync<TResult>(HttpMethod.Post, uri, message, token).ConfigureAwait(false);
            }

            private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            };

            private async Task<TResult> SendAsync<TResult>(HttpMethod method, Uri uri, object message, CancellationToken token) where TResult : class
            {
                var hasNonce = _nonce != null;
                HttpResponseMessage response;

                do
                {
                    var nonceHeader = new AcmeHeader
                    {
                        Nonce = _nonce
                    };

                    var request = new HttpRequestMessage(method, uri);

                    if (message != null)
                    {
                        var encodedMessage = _jws.Encode(message, nonceHeader);
                        var json = JsonConvert.SerializeObject(encodedMessage, jsonSettings);

                        request.Content = new StringContent(json, Encoding.UTF8, "application/json");
                    }

                    response = await _client.SendAsync(request, token).ConfigureAwait(false);

                    if (response.Headers.TryGetValues("Replay-Nonce", out var vals))
                        _nonce = vals.FirstOrDefault();
                    else
                        _nonce = null;

                    if (response.IsSuccessStatusCode || hasNonce || _nonce == null)
                    {
                        break; // either successful or no point in retry
                    }

                    hasNonce = true; // we only allow it once

                } while (true);
                
                if (response.Content.Headers.ContentType.MediaType == "application/problem+json")
                {
                    var problemJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    var problem = JsonConvert.DeserializeObject<Problem>(problemJson);
                    throw new AcmeException(problem, response);
                }

                if (typeof(TResult) == typeof(CertificateResponse)
                    && response.Content.Headers.ContentType.MediaType == "application/pkix-cert")
                {
                    var certificateBytes = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);

                    var certificateResponse = new CertificateResponse
                    {
                        Certificate = certificateBytes
                    };

                    GetHeaderValues(response, certificateResponse);

                    return certificateResponse as TResult;
                }

                var responseText = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                var responseContent = JObject.Parse(responseText).ToObject<TResult>();

                GetHeaderValues(response, responseContent);

                return responseContent;
            }

            private static void GetHeaderValues<TResult>(HttpResponseMessage response, TResult responseContent)
            {
                var properties =
                    typeof(TResult).GetProperties(BindingFlags.Public | BindingFlags.SetProperty | BindingFlags.Instance)
                        .Where(p => p.PropertyType == typeof(string))
                        .ToDictionary(p => p.Name, p => p);

                foreach (var header in response.Headers)
                {
                    if (properties.TryGetValue(header.Key, out var property)
                        && header.Value.Count() == 1)
                        property.SetValue(responseContent, header.Value.First());

                    if (header.Key == "Link")
                        foreach (var link in header.Value)
                        {
                            var parts = link.Split(';');

                            if (parts.Length != 2) continue;

                            if (parts[1] == "rel=\"terms-of-service\"" && properties.ContainsKey("Agreement"))
                                properties["Agreement"].SetValue(responseContent, parts[0].Substring(1, parts[0].Length - 2));
                        }
                }
            }

            #endregion
        }

        private class AcmeException : Exception
        {
            public AcmeException(Problem problem, HttpResponseMessage response)
                : base($"{problem.Type}: {problem.Detail}")
            {
                Problem = problem;
                Response = response;
            }

            public Problem Problem { get; }

            public HttpResponseMessage Response { get; }
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

        // ref: http://tools.ietf.org/html/rfc7515#page-15

        private class Challenge
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            /// <summary>
            ///     The URI to which a response can be posted.
            /// </summary>
            [JsonProperty("uri")]
            public Uri Uri { get; set; }

            [JsonProperty("token")]
            public string Token { get; set; }

            /// <summary>
            ///     The status of this authorization.
            ///     Possible values are: "unknown", "pending", "processing", "valid", "invalid" and "revoked".
            ///     If this field is missing, then the default value is "pending".
            /// </summary>
            [JsonProperty("status")]
            public string Status { get; set; }

            /// <summary>
            ///     The error that occurred while the server was validating the challenge.
            /// </summary>
            [JsonProperty("error")]
            public Error Error { get; set; }

            [JsonProperty("tls")]
            public bool Tls { get; set; }

            /// <summary>
            ///     The time at which this challenge was completed by the server
            /// </summary>
            [JsonProperty("validated")]
            public DateTime? Validated { get; set; }

            [JsonProperty("keyAuthorization")]
            public string KeyAuthorization { get; set; }

            [JsonProperty("validationRecord")]
            public ValidationRecord[] ValidationRecord { get; set; }
        }

        private class Error
        {
            [JsonProperty("detail")]
            public string Detail { get; set; }

            [JsonProperty("type")]
            public string Type { get; set; }
        }

        private class ValidationRecord
        {
            [JsonProperty("addressResolved")]
            public string AddressResolved { get; set; }

            [JsonProperty("addressUsed")]
            public string AddressUsed { get; set; }

            [JsonProperty("hostname")]
            public string HostName { get; set; }

            [JsonProperty("port")]
            public string Port { get; set; }

            [JsonProperty("url")]
            public Uri Url { get; set; }
        }

        private abstract class RegistrationRequest
        {
            [JsonProperty("jwk")]
            public Jwk Key { get; set; }

            [JsonProperty("contact")]
            public string[] Contact { get; set; }

            [JsonProperty("agreement")]
            public string Agreement { get; set; }

            [JsonProperty("authorizations")]
            public string Authorizations { get; set; }

            [JsonProperty("certificates")]
            public string Certificates { get; set; }
        }

        private class NewRegistrationRequest : RegistrationRequest
        {
            [JsonProperty("resource")]
            public string Resource { get; } = "new-reg";
        }

        private class UpdateRegistrationRequest : RegistrationRequest
        {
            public UpdateRegistrationRequest()
            {
            }

            public UpdateRegistrationRequest(string location, string agreement, string[] contact)
            {
                Location = location ?? throw new ArgumentNullException(nameof(location));
                Agreement = agreement;
                Contact = contact;
            }

            [JsonIgnore]
            public string Location { get; }

            [JsonProperty("resource")]
            public string Resource { get; } = "reg";
        }

        private class Directory
        {
            [JsonProperty("new-reg")]
            public Uri NewRegistration { get; set; }

            [JsonProperty("recover-reg")]
            public Uri RecoverRegistration { get; set; }

            [JsonProperty("new-authz")]
            public Uri NewAuthorization { get; set; }

            [JsonProperty("new-cert")]
            public Uri NewCertificate { get; set; }

            [JsonProperty("revoke-cert")]
            public Uri RevokeCertificate { get; set; }

            [JsonProperty("key-change")]
            public Uri KeyChange { get; set; }

            [JsonProperty("meta")]
            public DirectoryMeta Meta { get; set; }
        }

        private class DirectoryMeta
        {
            [JsonProperty("terms-of-service")]
            public string TermsOfService { get; set; }

            [JsonProperty("website")]
            public string Website { get; set; }

            [JsonProperty("caa-identities")]
            public string[] CaaIdentities { get; set; }
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
        }

        private class RegistrationResponse
        {
            [JsonProperty("key")]
            public Jwk Key { get; set; }

            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("initialIp")]
            public string InitialIp { get; set; }

            [JsonProperty("createdAt")]
            public DateTime CreatedAt { get; set; }

            [JsonProperty("contact")]
            public string[] Contact { get; set; }

            [JsonProperty("agreement")]
            public string Agreement { get; set; }

            [JsonProperty("authorizations")]
            public string Authorization { get; set; }

            [JsonProperty("certificates")]
            public string Certificates { get; set; }

            public string Location { get; set; }
        }

        private class Problem
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("detail")]
            public string Detail { get; set; }
        }

        private class AuthorizationResponse
        {
            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("expires")]
            public string Expires { get; set; }

            [JsonProperty("identifier")]
            public Identifier Identifier { get; set; }

            [JsonProperty("challenges")]
            public Challenge[] Challenges { get; set; }

            [JsonProperty("combinations")]
            public int[][] Combinations { get; set; }

            [JsonIgnore]
            public Uri Location { get; set; }
        }
        // https://tools.ietf.org/html/draft-ietf-acme-acme-01#section-5.3

        private class Identifier
        {
            public Identifier()
            {
            }

            public Identifier(string type, string value)
            {
                Type = type;
                Value = value;
            }

            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("value")]
            public string Value { get; set; }
        }

        private class AuthorizationRequest
        {
            [JsonProperty("resource")]
            public string Resource { get; set; }

            [JsonProperty("identifier")]
            public Identifier Identifier { get; set; }
        }

        private class KeyAuthorizationRequest
        {
            [JsonProperty("resource")]
            public string Resource => "challenge";

            [JsonProperty("keyAuthorization")]
            public string KeyAuthorization { get; set; }
        }

        private class AcmeCertificateRequest
        {
            [JsonProperty("resource")]
            public string Resource => "new-cert";

            [JsonProperty("csr")]
            public string Csr { get; set; }
        }

        private class CertificateResponse
        {
            public string Location { get; set; }

            public byte[] Certificate { get; set; }
        }

        private class AcmeHeader
        {
            [JsonProperty("nonce")]
            public string Nonce { get; set; }
        }

        private class Jws
        {
            private readonly Jwk jwk;
            private readonly RSA rsa;

            public Jws(RSA rsa)
            {
                this.rsa = rsa ?? throw new ArgumentNullException(nameof(rsa));

                var publicParameters = rsa.ExportParameters(false);

                jwk = new Jwk
                {
                    KeyType = "RSA",
                    Exponent = Base64UrlEncoded(publicParameters.Exponent),
                    Modulus = Base64UrlEncoded(publicParameters.Modulus)
                };
            }

            public JwsMessage Encode<TPayload, THeader>(TPayload payload, THeader protectedHeader)
            {
                var message = new JwsMessage
                {
                    Header = new JwsHeader("RS256", jwk),
                    Payload = Base64UrlEncoded(JsonConvert.SerializeObject(payload)),
                    Protected = Base64UrlEncoded(JsonConvert.SerializeObject(protectedHeader))
                };

                message.Signature = Base64UrlEncoded(
                    rsa.SignData(Encoding.ASCII.GetBytes(message.Protected + "." + message.Payload),
                        HashAlgorithmName.SHA256,
                        RSASignaturePadding.Pkcs1));

                return message;
            }

            private string GetSha256Thumbprint()
            {
                var json = "{\"e\":\"" + jwk.Exponent + "\",\"kty\":\"RSA\",\"n\":\"" + jwk.Modulus + "\"}";

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
        }

        #endregion

        public void ResetCachedCertificate(IEnumerable<string> hostsToRemove)
        {
            foreach (var host in hostsToRemove)
            {
                _cache.CachedCerts.Remove(host);
            }
        }
    }
}
