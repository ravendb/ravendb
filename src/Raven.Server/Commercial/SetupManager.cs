using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Extensions;
using Raven.Server.Https;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
using Voron.Platform.Posix;
using OpenFlags = System.Security.Cryptography.X509Certificates.OpenFlags;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public const string GoogleDnsApi = "https://dns.google.com";

        public static string BuildHostName(string nodeTag, string userDomain, string rootDomain)
        {
            return $"{nodeTag}.{userDomain}.{rootDomain}".ToLower();
        }

        public static async Task<string> LetsEncryptAgreement(string email, ServerStore serverStore)
        {
            if (IsValidEmail(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(email);
            return acmeClient.GetTermsOfServiceUri();
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
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

                await ValidateSetupInfo(SetupMode.Secured, setupInfo, serverStore);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.Secured, token);
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
                        await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, SetupMode.Secured, setupInfo, serverStore, token);
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

        public static async Task<X509Certificate2> RefreshLetsEncryptTask(SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(setupInfo.Email, token);

            // here we explicitly want to refresh the cert, so we don't want it cached
            var cacheKeys = setupInfo.NodeSetupInfos.Select(node => BuildHostName(node.Key, setupInfo.Domain, setupInfo.RootDomain)).ToList();
            acmeClient.ResetCachedCertificate(cacheKeys);

            var challengeResult = await InitialLetsEncryptChallenge(setupInfo,acmeClient, token);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

            try
            {
                await UpdateDnsRecordsForCertificateRefreshTask(challengeResult.Challenge, setupInfo, token);

                // Cache the current DNS topology so we can check it again
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");

            var cert = await CompleteAuthorizationAndGetCertificate(() =>
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Let's encrypt validation successful, acquiring certificate now...");
                },
                setupInfo,
                acmeClient,
                challengeResult,
                token);

            if (Logger.IsOperationsEnabled)
                Logger.Operations("Successfully acquired certificate from Let's Encrypt.");

            return cert;
        }

        public static async Task<IOperationResult> ContinueClusterSetupTask(Action<IOperationProgress> onProgress, ContinueSetupInfo continueSetupInfo, ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo($"Continuing cluster setup on node {continueSetupInfo.NodeTag}.");
                onProgress(progress);

                byte[] zipBytes;
                byte[] serverCertBytes;
                X509Certificate2 serverCert;
                X509Certificate2 clientCert;
                BlittableJsonReaderObject settingsJsonObject;
                License license;
                Dictionary<string, string> otherNodesUrls;
                string firstNodeTag;

                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                progress.Processed++;
                progress.AddInfo("Extracting setup settings and certificates from zip file.");
                onProgress(progress);

                using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        settingsJsonObject = ExtractCertificatesAndSettingsJsonFromZip(zipBytes, continueSetupInfo.NodeTag, context, out serverCertBytes, 
                            out serverCert, out clientCert, out firstNodeTag, out otherNodesUrls, out license);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);

                    try
                    {
                        await ValidateServerCanRunOnThisNode(settingsJsonObject, serverCert, serverStore, continueSetupInfo.NodeTag, token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Writing configuration settings and certificate.");
                    onProgress(progress);

                    try
                    {
                        await CompleteConfigurationForNewNode(onProgress, progress, continueSetupInfo, settingsJsonObject, serverCertBytes, serverCert, 
                            clientCert, serverStore, firstNodeTag, otherNodesUrls, license);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not complete configuration for new node.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                    onProgress(progress);

                    settingsJsonObject.Dispose();
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Cluster setup on node {continueSetupInfo.NodeTag} has failed", e);
            }
            return progress;
        }

        public static BlittableJsonReaderObject ExtractCertificatesAndSettingsJsonFromZip(byte[] zipBytes, string currentNodeTag, JsonOperationContext context, out byte[] certBytes, out X509Certificate2 serverCert, out X509Certificate2 clientCert, out string firstNodeTag, out Dictionary<string, string> otherNodesUrls, out License license)
        {
            certBytes = null;
            byte[] clientCertBytes = null;
            BlittableJsonReaderObject currentNodeSettingsJson = null;
            license = null;

            otherNodesUrls = new Dictionary<string, string>();

            firstNodeTag = "A";

            using (var msZip = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(msZip, ZipArchiveMode.Read, false))
            {
                foreach (var entry in archive.Entries)
                {
                    // try to find setup.json file first, as we make decisions based on its contents 
                    if (entry.Name.Equals("setup.json"))
                    {
                        var json = context.Read(entry.Open(), "license/json");

                        SetupSettings setupSettings = JsonDeserializationServer.SetupSettings(json);
                        firstNodeTag = setupSettings.Nodes[0].Tag;
                        
                        // Since we allow to customize node tags, we stored information about the order of nodes into setup.json file
                        // The first node is the one in which the cluster should be initialized.
                        // If the file isn't found, it means we are using a zip which was created in the old codebase => first node has the tag 'A'
                    }
                }
                
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.StartsWith($"{currentNodeTag}/") && entry.Name.EndsWith(".pfx"))
                    {
                        using (var ms = new MemoryStream())
                        {
                            entry.Open().CopyTo(ms);
                            certBytes = ms.ToArray();
                        }
                    }

                    if (entry.Name.StartsWith("admin.client.certificate") && entry.Name.EndsWith(".pfx"))
                    {
                        using (var ms = new MemoryStream())
                        {
                            entry.Open().CopyTo(ms);
                            clientCertBytes = ms.ToArray();
                        }
                    }

                    if (entry.Name.Equals("license.json"))
                    {
                        var json = context.Read(entry.Open(), "license/json");
                        license = JsonDeserializationServer.License(json);
                    }

                    if (entry.Name.Equals("settings.json"))
                    {
                        using (var settingsJson = context.ReadForMemory(entry.Open(), "settings-json-from-zip"))
                        {
                            settingsJson.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
                            
                            if (entry.FullName.StartsWith($"{currentNodeTag}/"))
                            {
                                currentNodeSettingsJson = settingsJson.Clone(context);
                            }

                            // This is for the case where we take the zip file and use it to setup the first node as well.
                            // If this is the first node, we must collect the urls of the other nodes so that
                            // we will be able to add them to the cluster when we bootstrap the cluster.
                            if (entry.FullName.StartsWith(firstNodeTag + "/") == false && publicServerUrl != null)
                            {
                                var tag = entry.FullName.Substring(0, entry.FullName.Length - "/settings.json".Length);
                                otherNodesUrls.Add(tag, publicServerUrl);
                            }
                                
                        }
                    }
                }
            }

            if (certBytes == null)
                throw new InvalidOperationException($"Could not extract the server certificate of node '{currentNodeTag}'. Are you using the correct zip file?");
            if (clientCertBytes == null)
                throw new InvalidOperationException("Could not extract the client certificate. Are you using the correct zip file?");
            if (currentNodeSettingsJson == null)
                throw new InvalidOperationException($"Could not extract settings.json of node '{currentNodeTag}'. Are you using the correct zip file?");

            try
            {
                currentNodeSettingsJson.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);

                serverCert = new X509Certificate2(certBytes, certPassword, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to load the server certificate of node '{currentNodeTag}'.", e);
            }

            try
            {
                clientCert = new X509Certificate2(clientCertBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to load the client certificate.", e);
            }

            return currentNodeSettingsJson;
        }

        public static async Task<LicenseStatus> GetUpdatedLicenseStatus(ServerStore serverStore, License currentLicense, Reference<License> updatedLicense = null)
        {
            var license = 
                await serverStore.LicenseManager.GetUpdatedLicense(currentLicense).ConfigureAwait(false)
                ?? currentLicense;

            var licenseStatus = serverStore.LicenseManager.GetLicenseStatus(license);
            if (licenseStatus.Expired)
                throw new LicenseExpiredException($"The provided license for {license.Name} has expired ({licenseStatus.Expiration})");

            if (updatedLicense != null)
                updatedLicense.Value = license;

            return licenseStatus;
        }

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };

            try
            {
                var updatedLicense = new Reference<License>();
                await GetUpdatedLicenseStatus(serverStore, setupInfo.License, updatedLicense).ConfigureAwait(false);
                setupInfo.License = updatedLicense.Value;

                AssertNoClusterDefined(serverStore);

                progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
                onProgress(progress);
                try
                {
                    await ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo, serverStore);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation of supplied settings failed.", e);
                }
                
                progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
                onProgress(progress);
                
                var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
                await acmeClient.Init(setupInfo.Email, token);
                
                var challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

                progress.Processed++;
                progress.AddInfo(challengeResult.Challenge != null
                    ? "Successfully received challenge(s) information from Let's Encrypt."
                    : "Using cached Let's Encrypt certificate.");

                progress.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

                onProgress(progress);

                try
                {
                    await UpdateDnsRecordsTask(onProgress, progress, challengeResult.Challenge, setupInfo, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
                }

                progress.Processed++;
                progress.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
                progress.AddInfo("Completing Let's Encrypt challenge(s)...");
                onProgress(progress);

                await CompleteAuthorizationAndGetCertificate(() =>
                    {
                        progress.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                        progress.AddInfo("Acquiring certificate.");
                        onProgress(progress);
                    },
                    setupInfo,
                    acmeClient,
                    challengeResult,
                    token);

                progress.Processed++;
                progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.LetsEncrypt, token);
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
                        await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, SetupMode.LetsEncrypt, setupInfo, serverStore, token);
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

        private static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(Action onValidationSuccessful, SetupInfo setupInfo,  LetsEncryptClient client, 
            (string Challange, LetsEncryptClient.CachedCertificateResult Cache) challengeResult,  CancellationToken token)
        {
            if (challengeResult.Challange == null && challengeResult.Cache != null)
            {
                return BuildNewPfx(setupInfo, challengeResult.Cache.Certificate, challengeResult.Cache.PrivateKey);
            }

            try
            {
                await client.CompleteChallenges(token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
            }

            onValidationSuccessful();


            (X509Certificate2 Cert, RSA PrivateKey) result;
            try
            {
                result = await client.GetCertificate(token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to acquire certificate from Let's Encrypt.", e);
            }

            try
            {
                return BuildNewPfx(setupInfo, result.Cert, result.PrivateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
            }
        }

        private static X509Certificate2 BuildNewPfx(SetupInfo setupInfo, X509Certificate2 certificate, RSA privateKey)
        {
            var certWithKey = certificate.CopyWithPrivateKey(privateKey);

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();
                        
            var chain = new X509Chain();
            chain.Build(certificate);

            foreach (var item in chain.ChainElements)
            {
                var x509Certificate = DotNetUtilities.FromX509Certificate(item.Certificate);

                if (item.Certificate.Thumbprint == certificate.Thumbprint)
                {
                    var key = new AsymmetricKeyEntry(DotNetUtilities.GetKeyPair(certWithKey.PrivateKey).Private);
                    store.SetKeyEntry(x509Certificate.SubjectDN.ToString(), key, new[] { new X509CertificateEntry(x509Certificate) });
                    continue;
                }
                
                store.SetCertificateEntry(item.Certificate.Subject, new X509CertificateEntry(x509Certificate));
            }
            
            var memoryStream = new MemoryStream();
            store.Save(memoryStream, Array.Empty<char>(), new SecureRandom(new CryptoApiRandomGenerator()));
            var certBytes = memoryStream.ToArray();
            
            Debug.Assert(certBytes != null);
            setupInfo.Certificate = Convert.ToBase64String(certBytes);

            return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
        }

        private static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult Cache)> InitialLetsEncryptChallenge(
            SetupInfo setupInfo, 
            LetsEncryptClient client, 
            CancellationToken token)
        {
            try
            {
                var host = (setupInfo.Domain + "." + setupInfo.RootDomain).ToLowerInvariant() ;
                var wildcardHost = "*." + host;
                if (client.TryGetCachedCertificate(wildcardHost, out var certBytes))
                    return (null, certBytes);

                var result = await client.NewOrder(new[] { wildcardHost }, token);

                result.TryGetValue(host, out var challenge);
                // we may already be authorized for this? 
                return (challenge, null);

            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to receive challenge(s) information from Let's Encrypt.", e);
            }
        }

        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }

        private static async Task UpdateDnsRecordsForCertificateRefreshTask(
            string challenge,
            SetupInfo setupInfo, CancellationToken token)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challenge,
                    RootDomain = setupInfo.RootDomain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                    };

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Start update process for certificate. License Id: {registrationInfo.License.Id}, " +
                                      $"License Name: {registrationInfo.License.Name}, " +
                                      $"Domain: {registrationInfo.Domain}, " +
                                      $"RootDomain: {registrationInfo.RootDomain}");

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
                            response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id,
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
            string challenge,
            SetupInfo setupInfo,
            CancellationToken token)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challenge,
                    RootDomain = setupInfo.RootDomain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };


                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                        Ips = node.Value.ExternalIpAddress == null
                            ? node.Value.Addresses
                            : new List<string>
                            {
                                node.Value.ExternalIpAddress
                            }
                    };

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }
                progress.AddInfo($"Creating DNS record/challenge for node(s): {string.Join(", ", setupInfo.NodeSetupInfos.Keys)}.");

                onProgress(progress);

                if (registrationInfo.SubDomains.Count == 0 && registrationInfo.Challenge == null)
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
                    progress.AddInfo("Please wait between 30 seconds and a few minutes.");
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

                if (challenge == null)
                {
                    var existingSubDomain = registrationInfo.SubDomains.FirstOrDefault(x => x.SubDomain.StartsWith(setupInfo.LocalNodeTag + ".", StringComparison.OrdinalIgnoreCase));
                    if (existingSubDomain != null && new HashSet<string>(existingSubDomain.Ips).SetEquals(setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses))
                    {
                        progress.AddInfo("DNS update started successfully, since current node (" + setupInfo.LocalNodeTag + ") DNS record didn't change, not waiting for full DNS propagation."); 
                        return;
                    }
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
                            response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id,
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

        public static async Task AssertLocalNodeCanListenToEndpoints(SetupInfo setupInfo, ServerStore serverStore)
        {
            var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
            var localIps = new List<IPEndPoint>();

            // Because we can get from user either an ip or a hostname, we resolve the hostname and get the actual ips it is mapped to 
            foreach (var hostnameOrIp in localNode.Addresses)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port)); 
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
            }

            var requestedEndpoints = localIps.ToArray();
            var currentServerEndpoints = serverStore.Server.ListenEndpoints.Addresses.Select(ip => new IPEndPoint(ip, serverStore.Server.ListenEndpoints.Port)).ToArray();

            var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] activeTcpListeners;
            try
            {
                activeTcpListeners = ipProperties.GetActiveTcpListeners();
            }
            catch (Exception)
            {
                // If GetActiveTcpListeners is not supported, skip the validation
                // See https://github.com/dotnet/corefx/issues/30909
                return;
            }
            
            foreach (var requestedEndpoint in requestedEndpoints)
            {
                if (activeTcpListeners.Contains(requestedEndpoint))
                {
                    if (currentServerEndpoints.Contains(requestedEndpoint))
                        continue; // OK... used by the current server

                    throw new InvalidOperationException(
                        $"The requested endpoint '{requestedEndpoint.Address}:{requestedEndpoint.Port}' is already in use by another process. You may go back in the wizard, change the settings and try again.");
                }
            }
            
        }

        public static async Task ValidateServerCanRunWithSuppliedSettings(SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode, CancellationToken token)
        {
            var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
            var localIps = new List<IPEndPoint>();

            foreach (var hostnameOrIp in localNode.Addresses)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
            }

            var serverCert = setupInfo.GetX509Certificate();

            var localServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, setupInfo.LocalNodeTag, localNode.Port, localNode.TcpPort, out _, out _);

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == localNode.Port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses;
                    if (localIps.Count == 0 && currentIps.Length == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this 

                    if (localIps.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                if (setupMode == SetupMode.LetsEncrypt)
                {
                    // In case an external ip was specified, this is the ip we update in the dns records. (not the one we bind to)
                    var ips = localNode.ExternalIpAddress == null
                        ? localIps.ToArray()
                        : new[] { new IPEndPoint(IPAddress.Parse(localNode.ExternalIpAddress), localNode.ExternalPort) };

                    await AssertDnsUpdatedSuccessfully(localServerUrl, ips, token);
                }

                // Here we send the actual ips we will bind to in the local machine.
                await SimulateRunningServer(serverCert, localServerUrl, setupInfo.LocalNodeTag, localIps.ToArray(), localNode.Port, serverStore.Configuration.ConfigPath, setupMode, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
            }
        }

        public static async Task ValidateServerCanRunOnThisNode(BlittableJsonReaderObject settingsJsonObject, X509Certificate2 cert, ServerStore serverStore, string nodeTag, CancellationToken token)
        {
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);
            
            var serverUrls = serverUrl.Split(";");
            var port = new Uri(serverUrls[0]).Port;
            var hostnamesOrIps = serverUrls.Select(url => new Uri(url).DnsSafeHost).ToArray();

            var localIps = new List<IPEndPoint>();

            foreach (var hostnameOrIp in hostnamesOrIps)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), port));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), port));
            }

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses;
                    if (localIps.Count == 0 && currentIps.Length == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this 

                    if (localIps.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                if (setupMode == SetupMode.LetsEncrypt)
                {
                    var ips = string.IsNullOrEmpty(externalIp)
                        ? localIps.ToArray()
                        : new[] { new IPEndPoint(IPAddress.Parse(externalIp), port) };

                    await AssertDnsUpdatedSuccessfully(publicServerUrl, ips, token);
                }

                // Here we send the actual ips we will bind to in the local machine.
                await SimulateRunningServer(cert, publicServerUrl, nodeTag, localIps.ToArray(), port, serverStore.Configuration.ConfigPath, setupMode, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + publicServerUrl, e);
            }
        }

        public static async Task ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            if (SetupParameters.Get(serverStore).IsDocker)
            {
                if (setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses.Any(ip => ip.StartsWith("127.")))
                {
                    throw new InvalidOperationException("When the server is running in Docker, you cannot bind to ip 127.X.X.X, please use the hostname instead.");
                }
            }

            if (setupMode == SetupMode.LetsEncrypt)
            {
                if (setupInfo.NodeSetupInfos.ContainsKey(setupInfo.LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{setupInfo.LocalNodeTag}'.");
                if (IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid email address.");
                if (IsValidDomain(setupInfo.Domain + "." + setupInfo.RootDomain) == false)
                    throw new ArgumentException("Invalid domain name.");
            }

            if (setupMode == SetupMode.Secured && string.IsNullOrWhiteSpace(setupInfo.Certificate))
                throw new ArgumentException($"{nameof(setupInfo.Certificate)} is a mandatory property for a secured setup");

            foreach (var node in setupInfo.NodeSetupInfos)
            {
                Leader.ValidateNodeTag(node.Key);

                if (node.Value.Port == 0)
                    setupInfo.NodeSetupInfos[node.Key].Port = 443;
                
                if (node.Value.TcpPort == 0)
                    setupInfo.NodeSetupInfos[node.Key].TcpPort = 38888;

                if (setupMode == SetupMode.LetsEncrypt &&
                    setupInfo.NodeSetupInfos[node.Key].Addresses.Any(ip => ip.Equals("0.0.0.0")) &&
                    string.IsNullOrWhiteSpace(setupInfo.NodeSetupInfos[node.Key].ExternalIpAddress))
                {
                    throw new ArgumentException("When choosing 0.0.0.0 as the ip address, you must provide an external ip to update in the DNS records.");
                }
            }

            await AssertLocalNodeCanListenToEndpoints(setupInfo, serverStore);
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

        public static string IndentJsonString(string json)
        {
            using (var stringReader = new StringReader(json))
            using (var stringWriter = new StringWriter())
            {
                var jsonReader = new JsonTextReader(stringReader);
                var jsonWriter = new JsonTextWriter(stringWriter) { Formatting = Formatting.Indented };
                jsonWriter.WriteToken(jsonReader);
                return stringWriter.ToString();
            }
        }

        public static void WriteSettingsJsonLocally(string settingsPath, string json)
        {
            var tmpPath = string.Empty;
            try
            {
                tmpPath = settingsPath + ".tmp";
                using (var file = SafeFileStream.Create(tmpPath, FileMode.Create))
                using (var writer = new StreamWriter(file))
                {
                    writer.Write(json);
                    writer.Flush();
                    file.Flush(true);
                }
            }       
            catch (Exception e) when (e is UnauthorizedAccessException || e is SecurityException)
            {
                throw new UnsuccessfulFileAccessException(e, tmpPath, FileAccess.Write);
            }

            try
            {
                File.Replace(tmpPath, settingsPath, settingsPath + ".bak");
                if (PlatformDetails.RunningOnPosix)
                    Syscall.FsyncDirectoryFor(settingsPath);
            }
            catch (UnauthorizedAccessException e)
            {
                throw new UnsuccessfulFileAccessException(e, settingsPath, FileAccess.Write);
            }
        }

        private static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, int tcpPort, out string publicTcpUrl, out string domain)
        {
            publicTcpUrl = null;
            var node = setupInfo.NodeSetupInfos[nodeTag];

            var cn = cert.GetNameInfo(X509NameType.SimpleName, false);
            if (cn[0] == '*')
            {
                var parts = cn.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{cn} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                publicTcpUrl = node.ExternalTcpPort != 0 
                    ? $"tcp://{nodeTag.ToLower()}.{domain}:{node.ExternalTcpPort}" 
                    : $"tcp://{nodeTag.ToLower()}.{domain}:{tcpPort}";

                if (setupInfo.NodeSetupInfos[nodeTag].ExternalPort != 0)
                    return $"https://{nodeTag.ToLower()}.{domain}:{node.ExternalPort}";

                return port == 443 
                    ? $"https://{nodeTag.ToLower()}.{domain}" 
                    : $"https://{nodeTag.ToLower()}.{domain}:{port}";
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

            if (node.ExternalPort != 0)
                url += ":" + node.ExternalPort;
            else if (port != 443)
                url += ":" + port;

            publicTcpUrl = node.ExternalTcpPort != 0 
                ? $"tcp://{domain}:{node.ExternalTcpPort}" 
                : $"tcp://{domain}:{tcpPort}";

            node.PublicServerUrl = url;
            node.PublicTcpServerUrl = publicTcpUrl;

            return url;
        }

        public static IEnumerable<string> GetCertificateAlternativeNames(X509Certificate2 cert)
        {
            // If we have alternative names, find the appropriate url using the node tag
            var sanNames = cert.Extensions["2.5.29.17"];

            if (sanNames == null)
                yield break;

            var generalNames = GeneralNames.GetInstance(Asn1Object.FromByteArray(sanNames.RawData));

            foreach (var certHost in generalNames.GetNames())
            {
                yield return certHost.Name.ToString();
            }
        }

        private static async Task CompleteConfigurationForNewNode(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            ContinueSetupInfo continueSetupInfo,
            BlittableJsonReaderObject settingsJsonObject,
            byte[] serverCertBytes,
            X509Certificate2 serverCert,
            X509Certificate2 clientCert,
            ServerStore serverStore,
            string firstNodeTag,
            Dictionary<string, string> otherNodesUrls,
            License license)
        {
            try
            {
                serverStore.Engine.SetNewState(RachisState.Passive, null, serverStore.Engine.CurrentTerm, "During setup wizard, " +
                                                                                                          "making sure there is no cluster from previous installation.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
            }

            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePath), out string certificateFileName);

            serverStore.Server.Certificate = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes, certPassword, serverStore);
            
            if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
            {
                serverStore.EnsureNotPassive(publicServerUrl, firstNodeTag);

                await DeleteAllExistingCertificates(serverStore);

                if (setupMode == SetupMode.LetsEncrypt && license != null)
                    await serverStore.LicenseManager.Activate(license, skipLeaseLicense: false);

                foreach (var url in otherNodesUrls)
                {
                    progress.AddInfo($"Adding node '{url.Key}' to the cluster.");
                    onProgress(progress);

                    try
                    {
                        await serverStore.AddNodeToClusterAsync(url.Value, url.Key, validateNotInTopology: false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{continueSetupInfo.NodeTag}' to the cluster.", e);
                    }
                }
            }

            progress.AddInfo("Registering client certificate in the local server.");
            onProgress(progress);
            var certDef = new CertificateDefinition
            {
                Name = $"{clientCert.SubjectName.Name}",
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(clientCert.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = clientCert.Thumbprint,
                NotAfter = clientCert.NotAfter
            };

            try
            {
                if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
                {
                    var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + clientCert.Thumbprint, certDef));
                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                }
                else
                {
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var certificate = ctx.ReadObject(certDef.ToJson(), "Client/Certificate/Definition"))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        serverStore.Cluster.PutLocalState(ctx, Constants.Certificates.Prefix + clientCert.Thumbprint, certificate);
                        tx.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to register client certificate in the local server.", e);
            }

            if (continueSetupInfo.RegisterClientCert)
            {
                RegisterClientCertInOs(onProgress, progress, clientCert);
                progress.AddInfo("Registering admin client certificate in the OS personal store.");
                onProgress(progress);
            }

            var certPath = Path.Combine(AppContext.BaseDirectory, certificateFileName);

            try
            {
                progress.AddInfo($"Saving server certificate at {certPath}.");
                onProgress(progress);

                using (var certfile = SafeFileStream.Create(certPath, FileMode.Create))
                {
                    var certBytes = serverCertBytes;
                    certfile.Write(certBytes, 0, certBytes.Length);
                    certfile.Flush(true);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save server certificate at {certPath}.", e);
            }

            try
            {
                progress.AddInfo($"Saving configuration at {serverStore.Configuration.ConfigPath}.");
                onProgress(progress);

                var indentedJson = IndentJsonString(settingsJsonObject.ToString());
                WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save configuration at {serverStore.Configuration.ConfigPath}.", e);
            }
            
            try
            {
                progress.Readme = CreateReadmeText(continueSetupInfo.NodeTag, publicServerUrl, true, continueSetupInfo.RegisterClientCert); 
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the readme text.", e);
            }
        }

        private static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            try
            {
                var settingsPath = serverStore.Configuration.ConfigPath;
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        progress.AddInfo("Loading and validating server certificate.");
                        onProgress(progress);
                        byte[] serverCertBytes;
                        X509Certificate2 serverCert;
                        string domainFromCert;
                        string publicServerUrl;

                        try
                        {
                            var base64 = setupInfo.Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            serverCert = new X509Certificate2(serverCertBytes, setupInfo.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                            var localNodeTag = setupInfo.LocalNodeTag;
                            publicServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, localNodeTag, setupInfo.NodeSetupInfos[localNodeTag].Port,
                                setupInfo.NodeSetupInfos[localNodeTag].TcpPort, out _, out domainFromCert);

                            try
                            {
                                serverStore.Engine.SetNewState(RachisState.Passive, null, serverStore.Engine.CurrentTerm, "During setup wizard, " +
                                                                                                                          "making sure there is no cluster from previous installation.");
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
                            }

                            serverStore.EnsureNotPassive(publicServerUrl, setupInfo.LocalNodeTag);

                            await DeleteAllExistingCertificates(serverStore);

                            if (setupMode == SetupMode.LetsEncrypt)
                                await serverStore.LicenseManager.Activate(setupInfo.License, skipLeaseLicense: false);

                            serverStore.Server.Certificate =
                                SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes, setupInfo.Password, serverStore);

                            foreach (var node in setupInfo.NodeSetupInfos)
                            {
                                if (node.Key == setupInfo.LocalNodeTag)
                                    continue;

                                progress.AddInfo($"Adding node '{node.Key}' to the cluster.");
                                onProgress(progress);

                                setupInfo.NodeSetupInfos[node.Key].PublicServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, node.Value.Port,
                                    node.Value.TcpPort, out _, out _);

                                try
                                {
                                    await serverStore.AddNodeToClusterAsync(setupInfo.NodeSetupInfos[node.Key].PublicServerUrl, node.Key, validateNotInTopology: false);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException($"Failed to add node '{node.Key}' to the cluster.", e);
                                }
                            }
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
                            var clientCertificateName = $"{name}.client.certificate";
                            certBytes = await GenerateCertificateTask(clientCertificateName, serverStore);
                            clientCert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
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

                            // Structure of external attributes field: https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute/14727#14727
                            // The permissions go into the most significant 16 bits of an int
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            using (var entryStream = entry.Open())
                            {
                                var export = clientCert.Export(X509ContentType.Pfx);
                                entryStream.Write(export, 0, export.Length);
                            }
                            AdminCertificatesHandler.WriteCertificateAsPem($"admin.client.certificate.{name}", certBytes, null, archive);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }

                        BlittableJsonReaderObject settingsJson;
                        using (var fs = SafeFileStream.Create(settingsPath, FileMode.Open, FileAccess.Read))
                        {
                            settingsJson = context.ReadForMemory(fs, "settings-json");
                        }

                        settingsJson.Modifications = new DynamicJsonValue(settingsJson);

                        if (setupMode == SetupMode.LetsEncrypt)
                        {
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = setupInfo.Email;

                            try
                            {
                                var licenseString = JsonConvert.SerializeObject(setupInfo.License, Formatting.Indented);

                                var entry = archive.CreateEntry("license.json");
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                using (var entryStream = entry.Open())
                                using (var writer = new StreamWriter(entryStream))
                                {
                                    writer.Write(licenseString);
                                    writer.Flush();
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to write license.json in zip archive.", e);
                            }
                        }

                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString();

                        if (setupInfo.EnableExperimentalFeatures)
                        {
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Experimental;
                        }

                        if (setupInfo.Environment != StudioConfiguration.StudioEnvironment.None)
                        {
                            var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(new ServerWideStudioConfiguration
                            {
                                Disabled = false,
                                Environment = setupInfo.Environment
                            }));
                            await serverStore.Cluster.WaitForIndexNotification(res.Index);
                        }

                        var certificateFileName = $"cluster.server.certificate.{name}.pfx";

                        if (setupInfo.ModifyLocalServer)
                        {
                            var certPath = Path.Combine(AppContext.BaseDirectory, certificateFileName);
                            using (var certfile = SafeFileStream.Create(certPath, FileMode.Create))
                            {
                                certfile.Write(serverCertBytes, 0, serverCertBytes.Length);
                                certfile.Flush(true);
                            }// we'll be flushing the directory when we'll write the settings.json
                        }

                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificateFileName;
                        if (string.IsNullOrEmpty(setupInfo.Password) == false)
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = setupInfo.Password;

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            var currentNodeSettingsJson = settingsJson.Clone(context);

                            progress.AddInfo($"Creating settings file 'settings.json' for node {node.Key}.");
                            onProgress(progress);

                            if (node.Value.Addresses.Count != 0)
                            {
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = string.Join(";", node.Value.Addresses.Select(ip => IpAddressToUrl(ip, node.Value.Port)));
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = string.Join(";", node.Value.Addresses.Select(ip => IpAddressToTcpUrl(ip, node.Value.TcpPort)));
                            }

                            var httpUrl = GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, node.Value.Port,
                                node.Value.TcpPort, out var tcpUrl, out var _);

                            if (string.IsNullOrEmpty(node.Value.ExternalIpAddress) == false)
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = node.Value.ExternalIpAddress;

                            currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = string.IsNullOrEmpty(node.Value.PublicServerUrl)
                                ? httpUrl
                                : node.Value.PublicServerUrl;

                            currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = string.IsNullOrEmpty(node.Value.PublicTcpServerUrl)
                                ? tcpUrl
                                : node.Value.PublicTcpServerUrl;

                            var modifiedJsonObj = context.ReadObject(currentNodeSettingsJson, "modified-settings-json");

                            var indentedJson = IndentJsonString(modifiedJsonObj.ToString());
                            if (node.Key == setupInfo.LocalNodeTag && setupInfo.ModifyLocalServer)
                            {
                                try
                                {
                                    WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
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
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                using (var entryStream = entry.Open())
                                using (var writer = new StreamWriter(entryStream))
                                {
                                    writer.Write(indentedJson);
                                    writer.Flush();
                                }

                                // we save this multiple times on each node, to make it easier 
                                // to deploy by just copying the node
                                entry = archive.CreateEntry($"{node.Key}/{certificateFileName}");
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

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
                        string readmeString = CreateReadmeText(setupInfo.LocalNodeTag, publicServerUrl, setupInfo.NodeSetupInfos.Count > 1, setupInfo.RegisterClientCert);

                        progress.Readme = readmeString;
                        try
                        {
                            var entry = archive.CreateEntry("readme.txt");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

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
                        
                        progress.AddInfo("Adding setup.json file to zip archive.");
                        onProgress(progress);

                        try
                        {
                            var settings = new SetupSettings
                            {
                                Nodes = setupInfo.NodeSetupInfos.Select(tag => new SetupSettings.Node
                                {
                                    Tag = tag.Key
                                }).ToArray()
                            };
                            
                            var modifiedJsonObj = context.ReadObject(settings.ToJson(), "setup-json");

                            var indentedJson = IndentJsonString(modifiedJsonObj.ToString());
                            
                            var entry = archive.CreateEntry("setup.json");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;
                            
                            using (var entryStream = entry.Open())
                            using (var writer = new StreamWriter(entryStream))
                            {
                                writer.Write(indentedJson);
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write setup.json to zip archive.", e);
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

        private static string IpAddressToTcpUrl(string address, int port)
        {
            var url = "tcp://" + address;
            if (port != 0)
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

        private static string CreateReadmeText(string nodeTag, string publicServerUrl, bool isCluster, bool registerClientCert)
        {
            var str =
                string.Format(WelcomeMessage.AsciiHeader, Environment.NewLine) + Environment.NewLine + Environment.NewLine +
                "Your RavenDB cluster settings, certificate and configuration are contained in this zip file." 
                + Environment.NewLine;

            str += Environment.NewLine +
                   $"The new server is available at: {publicServerUrl}"
                   + Environment.NewLine;


            str += $"The current node ('{nodeTag}') has already been configured and requires no further action on your part." +
                   Environment.NewLine;

            str += Environment.NewLine;
            if (registerClientCert && PlatformDetails.RunningOnPosix == false)
            {
                str +=
                    $"An administrator client certificate has been installed on this machine ({Environment.MachineName})." +
                    Environment.NewLine +
                    $"You can now restart the server and access the studio at {publicServerUrl}." +
                    Environment.NewLine +
                    "Chrome will let you select this certificate automatically. " +
                    Environment.NewLine +
                    "If it doesn't, you will get an authentication error. Please restart all instances of Chrome to make sure nothing is cached." +
                    Environment.NewLine;
            }
            else
            {
                str +=
                    $"An administrator client certificate has been generated and is located in the zip file." +
                    Environment.NewLine +
                    $"However, the certificate was not installed on this machine ({Environment.MachineName}), this can be done manually." +
                    Environment.NewLine;
            }

            str +=
                "If you are using Firefox (or Chrome under Linux), the certificate must be imported manually to the browser." +
                Environment.NewLine +
                "You can do that via: Tools > Options > Advanced > 'Certificates: View Certificates'." +
                Environment.NewLine;

            if (PlatformDetails.RunningOnPosix)
                str +=
                    "In Linux, importing the client certificate to the browser might fail for 'Unknown Reasons'." +
                    Environment.NewLine +
                    "If you encounter this bug, use the RavenCli command 'generateClientCert' to create a new certificate with a password." +
                    Environment.NewLine +
                    "For more information on this workaround, read the security documentation in 'ravendb.net'." +
                    Environment.NewLine;

            str +=
                Environment.NewLine +
                "It is recommended to generate additional certificates with reduced access rights for applications and users." +
                Environment.NewLine +
                "This can be done using the RavenDB Studio, in the 'Manage Server' > 'Certificates' page." +
                Environment.NewLine;

            if (isCluster)
            {
                str +=
                    Environment.NewLine +
                    "You are setting up a cluster. The cluster topology and node addresses have already been configured." +
                    Environment.NewLine +
                    "The next step is to download a new RavenDB server for each of the other nodes." +
                    Environment.NewLine +
                    Environment.NewLine +
                    "When you enter the setup wizard on a new node, please choose 'Continue Existing Cluster Setup'." +
                    Environment.NewLine +
                    "Do not try to start a new setup process again in this new node, it is not supported." +
                    Environment.NewLine +
                    "You will be asked to upload the zip file which was just downloaded." +
                    Environment.NewLine +
                    "The new server node will join the already existing cluster." +
                    Environment.NewLine +
                    Environment.NewLine +
                    "When the wizard is done and the new node was restarted, the cluster will automatically detect it. " +
                    Environment.NewLine +
                    "There is no need to manually add it again from the studio. Simply access the 'Cluster' view and " +
                    Environment.NewLine +
                    "observe the topology being updated." +
                    Environment.NewLine;
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

        public static async Task SimulateRunningServer(X509Certificate2 serverCertificate, string serverUrl, string nodeTag, IPEndPoint[] addresses, int port, string settingsPath, SetupMode setupMode, CancellationToken token)
        {
            var configuration = RavenConfiguration.CreateForServer(null, settingsPath);
            configuration.Initialize();
            var guid = Guid.NewGuid().ToString();

            IWebHost webHost = null;
            try
            {
                try
                {
                    var responder = new UniqueResponseResponder(guid);

                    var webHostBuilder = new WebHostBuilder()
                        .CaptureStartupErrors(captureStartupErrors: true)
                        .UseKestrel(options =>
                        {
                            if (addresses.Length == 0)
                            {
                                var defaultIp = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port);
                                options.Listen(defaultIp,
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"List of ip addresses for node '{nodeTag}' is empty. Webhost listening to {defaultIp}");
                            }

                            foreach (var addr in addresses)
                            {
                                options.Listen(addr,
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                            }
                        })
                        .UseSetting(WebHostDefaults.ApplicationKey, "Setup simulation")
                        .ConfigureServices(collection => { collection.AddSingleton(typeof(IStartup), responder); })
                        .UseShutdownTimeout(TimeSpan.FromMilliseconds(150));

                    if (configuration.Http.UseLibuv)
                        webHostBuilder = webHostBuilder.UseLibuv();

                    webHost = webHostBuilder.Build();

                    await webHost.StartAsync(token);
                }
                catch (Exception e)
                {
                    string linuxMsg = null;
                    if (PlatformDetails.RunningOnPosix && (port == 80 || port == 443))
                    {
                        var ravenPath = typeof(RavenServer).Assembly.Location;
                        if (ravenPath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                            ravenPath = ravenPath.Substring(ravenPath.Length - 4);

                        linuxMsg = $"It can happen if port '{port}' is not allowed for the non-root RavenDB process." +
                                   $"Try using setcap to allow it: sudo setcap CAP_NET_BIND_SERVICE=+eip {ravenPath}";
                    }

                    var also = linuxMsg == null ? string.Empty : "also";
                    var externalIpMsg = setupMode == SetupMode.LetsEncrypt
                        ? $"It can {also} happen if the ip is external (behind a firewall, docker). If this is the case, try going back to the previous screen and add the same ip as an external ip."
                        : string.Empty;
                    
                    throw new InvalidOperationException($"Failed to start webhost on node '{nodeTag}'. The specified ip address might not be reachable due to network issues. {linuxMsg}{Environment.NewLine}{externalIpMsg}{Environment.NewLine}" +
                                                        $"Settings file:{settingsPath}.{Environment.NewLine}" +
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
                                                                $"Are you blocked by a firewall? Make sure the port is open.{Environment.NewLine}" +
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
            // First we'll try to resolve the hostname through google's public dns api
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
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Request failed with status {response.StatusCode}.{Environment.NewLine}{responseString}");

                    dynamic dnsResult = JsonConvert.DeserializeObject(responseString);

                    // DNS response format: https://developers.google.com/speed/public-dns/docs/dns-over-https

                    if (dnsResult.Status != 0)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Got a DNS failure response:{Environment.NewLine}{responseString}" +
                                                            Environment.NewLine + "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");

                    JArray answers = dnsResult.Answer;
                    var googleIps = answers.Select(answer => answer["data"].ToString()).ToHashSet();

                    if (googleIps.SetEquals(expectedIps) == false)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Expected to get these ips: {string.Join(", ", expectedIps)} while Google's actual result was: {string.Join(", ", googleIps)}"
                                                            + Environment.NewLine + "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");
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
                        $"Cannot resolve '{hostname}' locally but succeeded resolving the address using Google's api ({GoogleDnsApi})."
                        + Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).", e);
                }

                if (expectedIps.SetEquals(actualIps) == false)
                    throw new InvalidOperationException(
                        $"Tried to resolve '{hostname}' locally but got an outdated result."
                        + Environment.NewLine + $"Expected to get these ips: {string.Join(", ", expectedIps)} while the actual result was: {string.Join(", ", actualIps)}"
                        + Environment.NewLine + $"If we try resolving through Google's api ({GoogleDnsApi}), it works well."
                        + Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).");
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
