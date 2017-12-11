using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Certes;
using Certes.Acme;
using Certes.Pkcs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Https;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public const string GoogleDnsApi = "https://dns.google.com";

        public static string BuildHostName(string subdomain, string domain)
        {
            return $"{subdomain.ToLower()}.{domain.ToLower()}.{RavenDbDomain}";
        }

        public static async Task<Uri> LetsEncryptAgreement(string email, ServerStore serverStore)
        {
            if (IsValidEmail(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);
            
            using (var acmeClient = new AcmeClient(new Uri(serverStore.Configuration.Core.AcmeUrl)))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 2
            };

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                ValidateSetupInfo(SetupMode.Secured, setupInfo, serverStore);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(token, setupInfo, serverStore, SetupMode.Secured);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation failed.", e);
                }

                progress.Processed++;
                progress.AddInfo("Validation is successful.");
                progress.AddInfo("Creating new RavenDB configuration settings.");
                onProgress(progress);

                try
                {
                    progress.SettingsZipFile =
                        await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, token, SetupMode.Secured, setupInfo, serverStore);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not create configuration settings.", e);
                }

                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                onProgress(progress);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in 'Secured Mode' failed.", e);
            }
            return progress;
        }

        private class LetsEncryptCache
        {
            public Dictionary<string, string> KeysByUrl { get; set; }
            public string Domain { get; set; }
            public string Certificate { get; set; }

            [JsonDeserializationIgnore]
            public X509Certificate2 CertificateInstance { get; set; }
        }

        public static async Task<X509Certificate2> RefreshLetsEncryptTask(SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            var cache = TryGetLetsEncryptCachedDetails();

            if (cache != null)
            {
                // here we explictly want to renew the certificate, so we must 
                // not get cache the actual certificate
                cache.CertificateInstance = null;
                cache.Certificate = null;
            }

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            using (var acmeClient = new AcmeClient(new Uri(serverStore.Configuration.Core.AcmeUrl)))
            {
                var dictionary = new Dictionary<string, Task<Challenge>>();
                var challengeResult = await InitialLetsEncryptChallenge(token, setupInfo, cache, acmeClient, dictionary, serverStore);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}.");


                try
                {
                    await UpdateDnsRecordsForCertificateRefreshTask(token, challengeResult.Challanges, setupInfo);

                    // Cache the current DNS topology so we can check it again
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}", e);
                }

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}");

                var cert = await CompleteAuthorizationAndGetCertificate(() =>
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations("Let's encrypt validation successful, acquiring certificate now...");
                    },
                    token,
                    setupInfo,
                    dictionary,
                    acmeClient,
                    challengeResult,
                    cache, 
                    serverStore);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Successfully acquired certificate from Let's Encrypt.");

                return cert;
            }
        }

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };

            try
            {
                var licenseStatus = serverStore.LicenseManager.GetLicenseStatus(setupInfo.License);

                if (licenseStatus.Expired)
                    throw new InvalidOperationException("The provided license for " + setupInfo.License.Name + " has expired (" + licenseStatus.Expiration + ")");

                AssertNoClusterDefined(serverStore);
                
                progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
                onProgress(progress);
                try
                {
                    ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo, serverStore);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation of supplied settings failed.", e);
                }

                var cache = TryGetLetsEncryptCachedDetails();


                progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
                onProgress(progress);

                using (var acmeClient = new AcmeClient(new Uri(serverStore.Configuration.Core.AcmeUrl)))
                {
                    var dictionary = new Dictionary<string, Task<Challenge>>();
                    var challengeResult = await InitialLetsEncryptChallenge(token, setupInfo, cache, acmeClient, dictionary, serverStore);

                    progress.Processed++;
                    progress.AddInfo(challengeResult.Challanges != null
                        ? "Successfully received challenge(s) information from Let's Encrypt."
                        : "Using cached Let's Encrypt certificate.");

                    progress.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}.");

                    onProgress(progress);

                    try
                    {
                        await UpdateDnsRecordsTask(onProgress, progress, token, challengeResult.Challanges, setupInfo);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}", e);
                    }

                    progress.Processed++;
                    progress.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{RavenDbDomain}");
                    progress.AddInfo("Completing Let's Encrypt challenge(s)...");
                    onProgress(progress);

                    await CompleteAuthorizationAndGetCertificate(() =>
                        {
                            progress.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                            progress.AddInfo("Acquiring certificate.");
                            onProgress(progress);
                        },
                        token,
                        setupInfo,
                        dictionary,
                        acmeClient,
                        challengeResult,
                        cache, 
                        serverStore);



                    progress.Processed++;
                    progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);

                    try
                    {
                        await ValidateServerCanRunWithSuppliedSettings(token, setupInfo, serverStore, SetupMode.LetsEncrypt);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Creating new RavenDB configuration settings.");

                    onProgress(progress);

                    try
                    {
                        progress.SettingsZipFile =
                            await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, token, SetupMode.LetsEncrypt, setupInfo, serverStore);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to create the configuration settings.", e);
                    }
                    
                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode finished successfully.");
                    onProgress(progress);
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in Let's Encrypt security mode failed.", e);
            }
            return progress;
        }

        private static void AssertNoClusterDefined(ServerStore serverStore)
        {
            var allNodes = serverStore.GetClusterTopology().AllNodes;
            if (allNodes.Count > 1)
            {
                throw new InvalidOperationException("This node is part of an already configured cluster and cannot be setup automatically any longer." +
                                                    Environment.NewLine +
                                                    "Either setup manually by editing the 'settings.json' file or delete the existing cluster, restart the server and try running setup again." + 
                                                    Environment.NewLine + 
                                                    "Existing cluster nodes " + JsonConvert.SerializeObject(allNodes, Formatting.Indented)
                                                    );
            }
        }

        private static async Task DeleteAllExistingCertificates(ServerStore serverStore)
        {
            // If a user repeats the setup process, there might be certificate leftovers in the cluster

            List<string> existingCertificateKeys;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                existingCertificateKeys = serverStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue)
                    .Select(item => Constants.Certificates.Prefix + JsonDeserializationServer.CertificateDefinition(item.Value).Thumbprint)
                    .ToList();
            }

            if (existingCertificateKeys.Count == 0)
                return;

            var res = await serverStore.SendToLeaderAsync(new DeleteCertificateCollectionFromClusterCommand()
            {
                Names = existingCertificateKeys
            });

            await serverStore.Cluster.WaitForIndexNotification(res.Index);
        }

        private static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(Action onValdiationSuccessful, CancellationToken token, SetupInfo setupInfo, Dictionary<string, Task<Challenge>> dictionary, AcmeClient acmeClient,
            (Dictionary<string, string> Challanges, byte[] Key) challengeResult, LetsEncryptCache cache, ServerStore serverStore)
        {
            if (challengeResult.Challanges == null && cache.Certificate != null)
            {
                setupInfo.Certificate = cache.Certificate;
                TrySaveLetEncryptCachedDetails(setupInfo, challengeResult.Key, serverStore);
                return null;
            }

            var csr = new CertificationRequestBuilder();
            try
            {
                var tasks = new List<Task>();
                foreach (var kvp in dictionary)
                {
                    tasks.Add(CompleteAuthorizationFor(acmeClient, kvp.Value.Result, token));
                }
                await Task.WhenAll(tasks);

                csr.AddName($"CN={BuildHostName("a", setupInfo.Domain)}");

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    csr.SubjectAlternativeNames.Add(BuildHostName(node.Key, setupInfo.Domain));
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
            }

            onValdiationSuccessful();


            (Dictionary<string, string> Challanges, byte[] Key) challengeResult1 = challengeResult;
            AcmeCertificate cert;
            try
            {
                cert = await acmeClient.NewCertificate(csr);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to aquire certificate from Let's Encrypt.", e);
            }

            try
            {
                var pfxBuilder = cert.ToPfx();
                var certBytes = pfxBuilder.Build(setupInfo.Domain.ToLower() + " cert", "");
                setupInfo.Certificate = Convert.ToBase64String(certBytes);

                TrySaveLetEncryptCachedDetails(setupInfo, challengeResult1.Key, serverStore);


                return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
            }
        }

        private static void TrySaveLetEncryptCachedDetails(SetupInfo setupInfo, byte[] key, ServerStore serverStore)
        {
            var cache = new LetsEncryptCache
            {
                Certificate = setupInfo.Certificate,
                Domain = setupInfo.Domain,
                KeysByUrl = new Dictionary<string, string>
                {
                    [serverStore.Configuration.Core.AcmeUrl] = Convert.ToBase64String(key)
                }
            };
            var cachePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), ".ravendb", "le.cache");

            try
            {
                var directory = Path.GetDirectoryName(cachePath);
                if (Directory.Exists(directory) == false)
                    Directory.CreateDirectory(directory);
                File.WriteAllText(cachePath, JsonConvert.SerializeObject(cache));
            }
            catch (Exception)
            {
                // it is fine to fail saving to the cache 
            }
        }

        private static LetsEncryptCache TryGetLetsEncryptCachedDetails()
        {
            // we use a file here instead of storing in the system db because we need
            // to reuse things if user re-run the setup process
            var cachePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), ".ravendb", "le.cache");
            try
            {
                var cache = JsonConvert.DeserializeObject<LetsEncryptCache>(File.ReadAllText(cachePath));
                cache.CertificateInstance = new X509Certificate2(Convert.FromBase64String(cache.Certificate));
                return cache;
            }
            catch (Exception)
            {
                // we can safely regenerate these values
                return null;
            }
        }

        private static async Task<(Dictionary<string, string> Challanges, byte[] Key)> InitialLetsEncryptChallenge(CancellationToken token, SetupInfo setupInfo, LetsEncryptCache cache, AcmeClient acmeClient, Dictionary<string, Task<Challenge>> dictionary, ServerStore serverStore)
        {
            try
            {
                var key = await SetupLetsEncryptAccount(setupInfo, cache, acmeClient, serverStore);

                // if the cache has less than 3 days, regenerate
                // intentionally using DateTime.Now, because the NotBefore / NotAfter are 
                // using local time, it seems, and we want to avoid generating a new cert
                // at the same day
                if (cache?.CertificateInstance != null &&
                    cache.CertificateInstance.NotBefore <= DateTime.Now &&
                    cache.CertificateInstance.NotAfter > DateTime.Now.AddDays(3))
                {
                    if (string.Equals(cache.Domain, setupInfo.Domain, StringComparison.OrdinalIgnoreCase))
                    {
                        var names = GetCertificateAlternativeNames(cache.CertificateInstance).ToArray();
                        var allExists = setupInfo.NodeSetupInfos.Keys.All(sd =>
                        {
                            var host = BuildHostName(sd, setupInfo.Domain);
                            return names.Contains(host, StringComparer.OrdinalIgnoreCase);
                        });
                        if (allExists)
                            return (null, key);
                    }
                }


                foreach (var tag in setupInfo.NodeSetupInfos)
                {
                    var host = BuildHostName(tag.Key, setupInfo.Domain);
                    var authz = acmeClient.NewAuthorization(new AuthorizationIdentifier
                    {
                        Type = AuthorizationIdentifierTypes.Dns,
                        Value = host
                    }).ContinueWith(t => { return t.Result.Data.Challenges.First(c => c.Type == ChallengeTypes.Dns01); }, token);
                    dictionary[tag.Key] = authz;
                }

                await Task.WhenAll(dictionary.Values.ToArray());
                return (dictionary.ToDictionary(x => x.Key.ToString(), x => acmeClient.ComputeDnsValue(x.Value.Result)), key);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to receive challenge(s) information from Let's Encrypt.", e);
            }
        }

        private static async Task<byte[]> SetupLetsEncryptAccount(SetupInfo setupInfo, LetsEncryptCache cache, AcmeClient acmeClient, ServerStore serverStore)
        {
            if (cache?.KeysByUrl != null && cache.KeysByUrl.TryGetValue(serverStore.Configuration.Core.AcmeUrl, out var accountKey))
            {
                try
                {
                    var key = Convert.FromBase64String(accountKey);
                    acmeClient.Use(new KeyInfo
                    {
                        PrivateKeyInfo = key
                    });
                    return key;
                }
                catch (Exception)
                {
                    // if failed, just build a new one
                }
            }

            var account = await acmeClient.NewRegistraton("mailto:" + setupInfo.Email);
            account.Data.Agreement = account.GetTermsOfServiceUri();
            await acmeClient.UpdateRegistration(account);

            TrySaveLetEncryptCachedDetails(setupInfo, account.Key.PrivateKeyInfo, serverStore);
            return account.Key.PrivateKeyInfo;
        }

        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }

        private static async Task UpdateDnsRecordsForCertificateRefreshTask(
            CancellationToken token,
            Dictionary<string, string> map,
            SetupInfo setupInfo)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                        Challenge = map[node.Key]
                    };

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Start update process for certificate: " + serializeObject);

                HttpResponseMessage response;
                try
                {
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }


                var id = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString).First().Value;

                try
                {
                    RegistrationResult registrationResult;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/registration-result?id=" + id,
                                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                        }

                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            throw new InvalidOperationException(
                                $"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                        }

                        registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);

                    } while (registrationResult.Status == "PENDING");
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new TimeoutException("Request failed due to a timeout error", e);
                }
            }
        }
        private static async Task UpdateDnsRecordsTask(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            CancellationToken token,
            Dictionary<string, string> map,
            SetupInfo setupInfo)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                        Ips = node.Value.Addresses
                    };

                    if (map == null)
                    {
                        // this means that we already have a cached certificate, so we just need to check if we need to update
                        // the DNS records.
                        try
                        {
                            var existing = Dns.GetHostAddresses(BuildHostName(node.Key, setupInfo.Domain))
                                .Select(ip => ip.ToString())
                                .ToList();
                            if (node.Value.Addresses.All(existing.Contains))
                                continue; // we can skip this
                        }
                        catch (Exception)
                        {
                            // it is expected that this won't exists
                        }
                        regNodeInfo.Challenge = "dummy value";
                    }
                    else
                    {
                        regNodeInfo.Challenge = map[node.Key];
                    }

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }
                progress.AddInfo($"Creating DNS record/challenge for node(s): {string.Join(", ", setupInfo.NodeSetupInfos.Keys)}.");

                onProgress(progress);

                if (registrationInfo.SubDomains.Count == 0)
                {
                    // no need to update anything, can skip doing DNS update
                    progress.AddInfo("Cached DNS values matched, skipping DNS update");
                    return;
                }


                var serializeObject = JsonConvert.SerializeObject(registrationInfo);
                HttpResponseMessage response;
                try
                {
                    progress.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                    progress.AddInfo("Please wait between 30 seconds and a few minutes, depending on the number of domains(nodes).");
                    onProgress(progress);
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                    progress.AddInfo("Waiting for DNS records to update...");
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }


                if (map == null && registrationInfo.SubDomains.Exists(x => x.SubDomain.StartsWith("A.", StringComparison.OrdinalIgnoreCase)) == false)
                {
                    progress.AddInfo("DNS update started successfully, since current node (A) DNS record didn't change, not waiting for full DNS propogation.");
                    return;
                }

                var id = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString).First().Value;

                try
                {
                    RegistrationResult registrationResult;
                    var i = 1;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/registration-result?id=" + id,
                                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                        }

                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            throw new InvalidOperationException(
                                $"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                        }

                        registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);

                        if (i % 120 == 0)
                            progress.AddInfo("This is taking too long, you might want to abort and restart if this goes on like this...");
                        else if (i % 45 == 0)
                            progress.AddInfo("If everything goes all right, we should be nearly there...");
                        else if (i % 30 == 0)
                            progress.AddInfo("The DNS update is still pending, carry on just a little bit longer...");
                        else if (i % 15 == 0)
                            progress.AddInfo("Please be patient, updating DNS records takes time...");
                        else if (i % 5 == 0)
                            progress.AddInfo("Waiting...");

                        onProgress(progress);

                        i++;
                    } while (registrationResult.Status == "PENDING");
                    progress.AddInfo("Got successful response from api.ravendb.net.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new TimeoutException("Request failed due to a timeout error", e);
                }
            }
        }

        private static async Task CompleteAuthorizationFor(AcmeClient client, Challenge dnsChallenge, CancellationToken token)
        {
            var challenge = await client.CompleteChallenge(dnsChallenge);

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token))
            {
                while (true)
                {
                    if (cts.IsCancellationRequested)
                        throw new TimeoutException("Timeout expired on completion of ACME authorization");

                    var authz = await client.GetAuthorization(challenge.Location);
                    if (authz.Data.Status == EntityStatus.Pending)
                    {
                        await Task.Delay(250, cts.Token);
                        continue;
                    }

                    if (authz.Data.Status == EntityStatus.Valid)
                        return;

                    throw new InvalidOperationException("Failed to authorize certificate: " + authz.Data.Status + Environment.NewLine + authz.Json);
                }
            }
        }

        public static void AssertLocalNodeCanListenToEndpoints(SetupInfo setupInfo, ServerStore serverStore)
        {
            var localNode = setupInfo.NodeSetupInfos[LocalNodeTag];
            var requestedEndpoints = localNode.Addresses.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port));
            var currentServerEndpoints = serverStore.Server.ListenEndpoints.Addresses.Select(ip => new IPEndPoint(ip, serverStore.Server.ListenEndpoints.Port)).ToArray();
            

            var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            var activeTcpListeners = ipProperties.GetActiveTcpListeners();

            foreach (var requestedEndpoint in requestedEndpoints)
            {
                if (activeTcpListeners.Contains(requestedEndpoint))
                {
                    if (currentServerEndpoints.Contains(requestedEndpoint))
                        continue; // OK... used by the current server

                    throw new InvalidOperationException($"The requested endpoint '{requestedEndpoint.Address}:{requestedEndpoint.Port}' is already in use by another process. You may go back in the wizard, change the settings and try again.");
                }
            }
        }

        public static async Task ValidateServerCanRunWithSuppliedSettings(CancellationToken token, SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode)
        {
            var localNode = setupInfo.NodeSetupInfos[LocalNodeTag];

            var ips = localNode.Addresses.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();

            var serverCert = setupInfo.GetX509Certificate();

            var localServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, LocalNodeTag, localNode.Port, out _);

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == localNode.Port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses;
                    if (ips.Length == 0 && currentIps.Length == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this 

                    if (ips.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                if (setupMode == SetupMode.LetsEncrypt)
                    await AssertDnsUpdatedSuccessfully(localServerUrl, ips, token);

                await SimulateRunningServer(serverCert, localServerUrl, ips, token, setupInfo, serverStore.Configuration.ConfigPath, setupMode);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
            }
        }

        public static void ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            if (setupMode == SetupMode.LetsEncrypt)
            {
                if (setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{LocalNodeTag}'.");
                if (IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid email address.");
                if (IsValidDomain(setupInfo.Domain) == false)
                    throw new ArgumentException("Invalid domain name.");
            }

            if (setupMode == SetupMode.Secured && string.IsNullOrWhiteSpace(setupInfo.Certificate))
                throw new ArgumentException($"{nameof(setupInfo.Certificate)} is a mandatory property for a secured setup");

            foreach (var node in setupInfo.NodeSetupInfos)
            {
                if (string.IsNullOrWhiteSpace(node.Key) || node.Key.Length != 1 || !char.IsLetter(node.Key[0]) || !char.IsUpper(node.Key[0]))
                    throw new ArgumentException("Node Tag [A-Z] (capital) is a mandatory property for a secured setup");

                if (node.Value.Port == 0)
                    setupInfo.NodeSetupInfos[node.Key].Port = 443;
            }

            AssertLocalNodeCanListenToEndpoints(setupInfo, serverStore);
        }

        public static bool IsValidEmail(string email)
        {
            if (string.IsNullOrWhiteSpace(email))
                return false;
            try
            {
                var addr = new System.Net.Mail.MailAddress(email);
                return addr.Address == email;
            }
            catch
            {
                return false;
            }
        }

        private static bool IsValidDomain(string domain)
        {
            if (string.IsNullOrWhiteSpace(domain))
                return false;

            return Uri.CheckHostName(domain) != UriHostNameType.Unknown;
        }

        public static void WriteSettingsJsonLocally(string settingsPath, string json)
        {
            var tmpPath = settingsPath + ".tmp";
            using (var file = new FileStream(tmpPath, FileMode.Create))
            using (var writer = new StreamWriter(file))
            {
                writer.Write(json);
                writer.Flush();
                file.Flush(true);
            }

            File.Replace(tmpPath, settingsPath, settingsPath + ".bak");
            if (PlatformDetails.RunningOnPosix)
                Syscall.FsyncDirectoryFor(settingsPath);
        }

        private static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, out string domain)
        {
            var cn = cert.GetNameInfo(X509NameType.DnsName, false);
            if (cn[0] == '*')
            {
                var parts = cn.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{cn} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                if (port == 443)
                    return $"https://{nodeTag.ToLower()}.{domain}";

                return $"https://{nodeTag.ToLower()}.{domain}:{port}";
            }

            domain = cn; //default for one node case

            foreach (var value in GetCertificateAlternativeNames(cert))
            {
                if (value.StartsWith(nodeTag + ".", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                domain = value;
                break;
            }

            var url = $"https://{domain}";
            if (port != 443)
                url += ":" + port;

            setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl = url;

            return setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl;
        }

        public static IEnumerable<string> GetCertificateAlternativeNames(X509Certificate2 cert)
        {
            var sanNames = cert.Extensions["2.5.29.17"];
            // If we have alternative names, find the apropriate url using the node tag
            foreach (var line in sanNames.Format(true).Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries))
            {
                string[] parts;

                if (line.Contains('='))
                {
                    parts = line.Split('=');
                }
                else if (line.Contains(':'))
                {
                    parts = line.Split(':');
                }
                else
                {
                    throw new InvalidOperationException($"Could not parse SAN names: {line}");
                }

                yield return parts.Length > 0 ? parts[1] : "";
            }
        }

        private static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, CancellationToken token, SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            try
            {
                var settingsPath = serverStore.Configuration.ConfigPath;
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var originalSettings = File.ReadAllText(settingsPath);
                        dynamic jsonObj = JsonConvert.DeserializeObject(originalSettings);

                        progress.AddInfo("Loading and validating server certificate.");
                        onProgress(progress);
                        byte[] serverCertBytes;
                        X509Certificate2 serverCert;
                        string domainFromCert;
                        string publicServerUrl;
                        string clientCertificateName;

                        try
                        {
                            var base64 = setupInfo.Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            serverCert = new X509Certificate2(serverCertBytes, setupInfo.Password, X509KeyStorageFlags.Exportable);

                            publicServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, LocalNodeTag, setupInfo.NodeSetupInfos[LocalNodeTag].Port, out domainFromCert);

                            try
                            {
                                serverStore.Engine.SetNewState(RachisState.Passive, null, serverStore.Engine.CurrentTerm, "During setup wizard, " +
                                                                                                                          "making sure there is no cluster from previous installation.");
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
                            }

                            serverStore.EnsureNotPassive(publicServerUrl);

                            await DeleteAllExistingCertificates(serverStore);

                            if (setupMode == SetupMode.LetsEncrypt)
                                await serverStore.LicenseManager.Activate(setupInfo.License, skipLeaseLicense: false);

                            serverStore.Server.Certificate =
                                SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes, setupInfo.Password);
                            
                            foreach (var node in setupInfo.NodeSetupInfos)
                            {
                                if (node.Key == LocalNodeTag)
                                    continue;
                                    
                                progress.AddInfo($"Adding node '{node.Key}' to the cluster.");
                                onProgress(progress);

                                setupInfo.NodeSetupInfos[node.Key].PublicServerUrl =
                                    GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, node.Value.Port, out var _);
                                
                                try
                                {
                                    await serverStore.AddNodeToClusterAsync(setupInfo.NodeSetupInfos[node.Key].PublicServerUrl, node.Key, validateNotInTopology: false);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException($"Failed to add node '{node.Key}' to the cluster.", e);
                                }
                            }
                            serverStore.EnsureServerCertificateIsInClusterState("Cluster-Wide Certificate");
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Could not load the certificate in the local server.", e);
                        }

                        progress.AddInfo("Generating the client certificate.");
                        onProgress(progress);
                        X509Certificate2 clientCert;

                        var name = (setupMode == SetupMode.Secured)
                            ? domainFromCert.ToLower()
                            : setupInfo.Domain.ToLower();

                        byte[] certBytes;
                        try
                        {
                            // requires server certificate to be loaded
                            clientCertificateName = $"{name}.client.certificate";
                            certBytes = await GenerateCertificateTask(clientCertificateName, serverStore);
                            clientCert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not generate a client certificate for '{name}'.", e);
                        }
                        if (setupInfo.RegisterClientCert)
                            RegisterClientCertInOs(onProgress, progress, clientCert);

                        progress.AddInfo("Writing certificates to zip archive.");
                        onProgress(progress);
                        try
                        {
                            var entry = archive.CreateEntry($"admin.client.certificate.{name}.pfx");
                            using (var entryStream = entry.Open())
                            {
                                var export = clientCert.Export(X509ContentType.Pfx);
                                entryStream.Write(export, 0, export.Length);
                            }

                            entry = archive.CreateEntry($"admin.client.certificate.{name}.pem");
                            using (var entryStream = entry.Open())
                            {
                                AdminCertificatesHandler.WriteCertificateAsPem(certBytes, null, entryStream);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }

                        jsonObj[RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString();
                        var certificateFileName = $"cluster.server.certificate.{name}.pfx";

                        if (setupInfo.ModifyLocalServer)
                        {
                            var certPath = Path.Combine(AppContext.BaseDirectory, certificateFileName);
                            using (var certfile = new FileStream(certPath, FileMode.Create))
                            {
                                certfile.Write(serverCertBytes, 0, serverCertBytes.Length);
                                certfile.Flush(true);
                            }// we'll be flushing the directory when we'll write the settings.json
                        }

                        jsonObj[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificateFileName;
                        if (string.IsNullOrEmpty(setupInfo.Password) == false)
                            jsonObj[RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = setupInfo.Password;

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            progress.AddInfo($"Creating settings file 'settings.json' for node {node.Key}.");
                            onProgress(progress);

                            if (node.Value.Addresses.Count != 0)
                                jsonObj[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = string.Join(";", node.Value.Addresses.Select(ip => IpAddressToUrl(ip, node.Value.Port)));

                            jsonObj[RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = string.IsNullOrEmpty(node.Value.PublicServerUrl)
                                ? GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, setupInfo.NodeSetupInfos[LocalNodeTag].Port, out var _)
                                : node.Value.PublicServerUrl;

                            var jsonString = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                            if (node.Key == LocalNodeTag && setupInfo.ModifyLocalServer)
                            {
                                try
                                {
                                    WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, jsonString);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException("Failed to write settings file 'settings.json' for the local sever.", e);
                                }
                            }

                            progress.AddInfo($"Adding settings file for node '{node.Key}' to zip archive.");
                            onProgress(progress);
                            try
                            {
                                var entry = archive.CreateEntry($"{node.Key}/settings.json");
                                using (var entryStream = entry.Open())
                                using (var writer = new StreamWriter(entryStream))
                                {
                                    writer.Write(jsonString);
                                    writer.Flush();
                                }
                                // we save this multiple times on each node, to make it easier 
                                // to deploy by just copying the node
                                entry = archive.CreateEntry($"{node.Key}/{certificateFileName}");
                                using (var entryStream = entry.Open())
                                {
                                    entryStream.Write(serverCertBytes, 0, serverCertBytes.Length);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Failed to write settings.json for node '{node.Key}' in zip archive.", e);
                            }
                        }

                        progress.AddInfo("Adding readme file to zip archive.");
                        onProgress(progress);
                        var currentHostName = setupMode == SetupMode.LetsEncrypt ? BuildHostName("A", setupInfo.Domain) : new Uri(publicServerUrl).Host;
                        string readmeString = CreateReadmeText(setupInfo, publicServerUrl, clientCertificateName, currentHostName);
                        progress.Readme = readmeString;
                        try
                        {
                            var entry = archive.CreateEntry("readme.txt");
                            using (var entryStream = entry.Open())
                            using (var writer = new StreamWriter(entryStream))
                            {
                                writer.Write(readmeString);
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write readme.txt to zip archive.", e);
                        }
                    }
                    return ms.ToArray();
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create setting file(s).", e);
            }
        }

        private static string IpAddressToUrl(string address, int port)
        {
            var url = "https://" + address;
            if (port != 443)
                url += ":" + port;
            return url;
        }

        public static void RegisterClientCertInOs(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, X509Certificate2 clientCert)
        {
            using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser, OpenFlags.ReadWrite))
            {
                try
                {
                    userPersonalStore.Add(clientCert);
                    progress.AddInfo($"Successfully registered the admin client certificate in the OS Personal CurrentUser Store '{userPersonalStore.Name}'.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to register client certificate in the current user personal store '{userPersonalStore.Name}'.", e);
                }
            }
        }


        private static string CreateReadmeText(SetupInfo setupInfo, string publicServerUrl, string clientCertificateName, string currentHostName)
        {
            var str =
                string.Format(WelcomeMessage.AsciiHeader, Environment.NewLine) + Environment.NewLine + Environment.NewLine +
                "Your RavenDB cluster settings, certificate and configuration are contained in this zip file." + Environment.NewLine;

            str += Environment.NewLine +
                   $"The new server is available at: {publicServerUrl}"
                   + Environment.NewLine;

            if (setupInfo.ModifyLocalServer)
            {
                str += ($"The current node (A - {currentHostName}) has already been configured and requires no further action on your part" +
                        Environment.NewLine);
            }
            str += Environment.NewLine;
            if (setupInfo.RegisterClientCert)
            {
                str +=
                    ($"An administrator client certificate ({clientCertificateName}) has been installed on this machine ({Environment.MachineName}) and you can now access the server in a secure fashion." + Environment.NewLine);
            }
            else
            {
                str +=
                    ($"An administrator client certificate ({clientCertificateName}) has been generated which can use to access the server." + Environment.NewLine);
            }

            str +=
                "If you are using Firefox, the certificate must be imported directly to the browser, you can do that via: Tools > Options > Advanced > 'Certificates: View Certificates'." +
                Environment.NewLine;

            str +=
                Environment.NewLine +
                "It is recommended that you'll generate additional certificates with reduced access rights for applications and users to use. You can do that in the 'Manage Server' > 'Certificates' page in the RavenDB Studio." +
                Environment.NewLine;

            if (setupInfo.NodeSetupInfos.Count > 1)
            {
                str +=
                    Environment.NewLine +
                    "As you are setting up a cluster, you will find the configuration for each of the nodes available in the folders in this zip file. All you'll" +
                    Environment.NewLine +
                    "to do is to extract the files from each folder to the base directory of the RavenDB node in question and start it. The cluster will configure" +
                    Environment.NewLine +
                    "itself and handle all setup for you." +
                    Environment.NewLine +
                    Environment.NewLine +
                    "Make sure that the various nodes can talk to each other using the URLs you have defined and that there is no firewall blocking communication between them."
                    + Environment.NewLine;

            }
            return str;
        }

        private class UniqueResponseResponder : IStartup
        {
            private readonly string _response;

            public UniqueResponseResponder(string response)
            {
                _response = response;
            }

            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                var x = services.BuildServiceProvider();

                return services.BuildServiceProvider();
            }

            public void Configure(IApplicationBuilder app)
            {
                app.Run(async context =>
                {
                    await context.Response.WriteAsync(_response);
                });
            }
        }

        public static async Task SimulateRunningServer(X509Certificate2 serverCertificate, string serverUrl, IPEndPoint[] addresses, CancellationToken token, SetupInfo setupInfo, string settingsPath, SetupMode setupMode)
        {
            var configuration = new RavenConfiguration(null, ResourceType.Server, settingsPath);
            configuration.Initialize();
            var guid = Guid.NewGuid().ToString();

            IWebHost webHost = null;
            try
            {
                try
                {
                    var responder = new UniqueResponseResponder(guid);

                    webHost = new WebHostBuilder()
                        .CaptureStartupErrors(captureStartupErrors: true)
                        .UseKestrel(options =>
                        {
                            var port = setupInfo.NodeSetupInfos[LocalNodeTag].Port;
                            if (addresses.Length == 0)
                            {
                                var defaultIp = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port);
                                options.Listen(defaultIp,
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"List of ip addresses for node '{LocalNodeTag}' is empty. Webhost listening to {defaultIp}");
                            }

                            foreach (var addr in addresses)
                            {
                                options.Listen(addr,
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                            }
                        })
                        .UseSetting(WebHostDefaults.ApplicationKey, "Setup simulation")
                        .ConfigureServices(collection =>
                        {
                            collection.AddSingleton(typeof(IStartup), responder);
                        })
                        .UseShutdownTimeout(TimeSpan.FromMilliseconds(150))
                        .Build();

                    await webHost.StartAsync(token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to start webhost on node '{LocalNodeTag}'.{Environment.NewLine}" +
                                                        $"Settings file:{settingsPath}.{Environment.NewLine} " +
                                                        $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.", e);
                }
                
                using (var httpMessageHandler = new HttpClientHandler())
                {
                    // on MacOS this is not supported because Apple...
                    if (PlatformDetails.RunningOnMacOsx == false)
                    {
                        httpMessageHandler.ServerCertificateCustomValidationCallback += (message, certificate2, chain, errors) =>
                        // we want to verify that we get the same thing back
                        {
                            if (certificate2.Thumbprint != serverCertificate.Thumbprint)
                                throw new InvalidOperationException("Expected to get " + serverCertificate.FriendlyName + " with thumbprint " +
                                                                    serverCertificate.Thumbprint + " but got " +
                                                                    certificate2.FriendlyName + " with thumbprint " + certificate2.Thumbprint);
                            return true;
                        };
                    }
                    
                    using (var client = new HttpClient(httpMessageHandler)
                    {
                        BaseAddress = new Uri(serverUrl),
                    })
                    {
                        HttpResponseMessage response = null;
                        string result = null;
                        try
                        {
                            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
                            {
                                response = await client.GetAsync("/are-you-there?", cts.Token);
                                response.EnsureSuccessStatusCode();
                                result = await response.Content.ReadAsStringAsync();
                                if (result != guid)
                                {
                                    throw new InvalidOperationException($"Expected result guid: {guid} but got {result}.");
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (setupMode == SetupMode.Secured && await CanResolveHostNameLocally(serverUrl, addresses) == false)
                            {
                                throw new InvalidOperationException(
                                    $"Failed to resolve '{serverUrl}'. Try to clear your local/network DNS cache and restart validation.", e);
                            }

                            throw new InvalidOperationException($"Client failed to contact webhost listening to '{serverUrl}'.{Environment.NewLine}" +
                                                                $"Settings file:{settingsPath}.{Environment.NewLine}" +
                                                                $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.{Environment.NewLine}" +
                                                                $"Response: {response?.StatusCode}.{Environment.NewLine}{result}", e);
                        }
                    }
                }
            }
            finally
            {
                if (webHost != null)
                    await webHost.StopAsync(TimeSpan.Zero);
            }
        }
        
        private static async Task<bool> CanResolveHostNameLocally(string serverUrl, IPEndPoint[] expectedAddresses)
        {
            var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();
            var hostname = new Uri(serverUrl).Host;
            HashSet<string> actualIps;

            try
            {
                actualIps = (await Dns.GetHostAddressesAsync(hostname)).Select(address => address.ToString()).ToHashSet();
            }
            catch (Exception)
            {
                return false;
            }

            return expectedIps.SetEquals(actualIps);
        }

        private static async Task AssertDnsUpdatedSuccessfully(string serverUrl, IPEndPoint[] expectedAddresses, CancellationToken token)
        {
            // First we'll try to resolve the hostname through googls's public dns api
            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
            {
                var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();

                var hostname = new Uri(serverUrl).Host;

                using (var client = new HttpClient { BaseAddress = new Uri(GoogleDnsApi) })
                {
                    var response = await client.GetAsync($"/resolve?name={hostname}", cts.Token);

                    var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (response.IsSuccessStatusCode == false)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Request failed with status {response.StatusCode}.{Environment.NewLine}{responseString}");

                    dynamic dnsResult = JsonConvert.DeserializeObject(responseString);

                    // DNS response format: https://developers.google.com/speed/public-dns/docs/dns-over-https

                    if (dnsResult.Status != 0)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Got a DNS failure response:{Environment.NewLine}{responseString}" +
                                                            Environment.NewLine + "Please wait a while until DNS propogation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");

                    JArray answers = dnsResult.Answer;
                    var googleIps = answers.Select(answer => answer["data"].ToString()).ToHashSet();

                    if (googleIps.SetEquals(expectedIps) == false)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Expected to get these ips: {string.Join(", ", expectedIps)} while google's actual result was: {string.Join(", ", googleIps)}"
                                                            + Environment.NewLine + "Please wait a while until DNS propogation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");
                }

                // Resolving through google worked, now let's check locally
                HashSet<string> actualIps;
                try
                {
                    actualIps = (await Dns.GetHostAddressesAsync(hostname)).Select(address => address.ToString()).ToHashSet();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Cannot resolve '{hostname}' locally but succeeded resolving the address using google's api ({GoogleDnsApi})."
                        + Environment.NewLine + "Try to clear your local/network DNS cache and restart validation."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use google's DNS server (8.8.8.8).", e);
                }

                if (expectedIps.SetEquals(actualIps) == false)
                    throw new InvalidOperationException(
                        $"Tried to resolve '{hostname}' locally but got an outdated result."
                        + Environment.NewLine + $"Expected to get these ips: {string.Join(", ", expectedIps)} while the actual result was: {string.Join(", ", actualIps)}"
                        + Environment.NewLine + $"If we try resolving through google's api ({GoogleDnsApi}), it works well."
                        + Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use google's DNS server (8.8.8.8).");
            }
        }

        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal stripped from authz checks, used by an unauthenticated client during setup only
        public static async Task<byte[]> GenerateCertificateTask(string name, ServerStore serverStore)
        {
            if (serverStore.Server.Certificate?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' because the server certificate is not loaded.");

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.Certificate, out var certBytes);

            var newCertDef = new CertificateDefinition
            {
                Name = name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint,
                NotAfter = selfSignedCertificate.NotAfter
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint, newCertDef));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return certBytes;
        }
    }
}
