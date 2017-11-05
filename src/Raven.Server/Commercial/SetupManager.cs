using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Certes;
using Certes.Acme;
using Certes.Pkcs;
using Lextm.SharpSnmpLib.Messaging;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public const string SettingsPath = "settings.json";
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public static readonly Uri LetsEncryptServer = WellKnownServers.LetsEncryptStaging;

        /*  TODO
            handle thread safety
            Change priority of certificate selection
            Remove one of the server certificates
            call token ThrowIfCancellationRequested() in proper places
         */
        
        public static async Task<Uri> LetsEncryptAgreement(string email)
        {
            if (IsValidEmail(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);

            using (var acmeClient = new AcmeClient(LetsEncryptServer))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 1
            };
            progress.AddInfo("Setting up RavenDB in secured mode.");
            progress.AddInfo("Creating new RavenDB configuration settings.");
            onProgress(progress);

            ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo);

            try
            {
                progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(token, SetupMode.Secured, setupInfo);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Setup failed. Could not create configuration file(s). Exception:{Environment.NewLine}{e}", e);
            }

            progress.Processed++;
            progress.AddInfo("Successfully created new configuration settings.");
            onProgress(progress);
            return progress;
        }

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };
            progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
            onProgress(progress);

            ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo);

            progress.AddInfo($"Getting challenge from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
            onProgress(progress);

            try
            {
                using (var acmeClient = new AcmeClient(LetsEncryptServer))
                {
                    var dictionary = new Dictionary<string, Task<Challenge>>();
                    Dictionary<string, string> map = null;
                    try
                    {
                        var account = await acmeClient.NewRegistraton("mailto:" + setupInfo.Email);
                        account.Data.Agreement = account.GetTermsOfServiceUri();
                        await acmeClient.UpdateRegistration(account);

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            var host = $"{node.Key}.{setupInfo.Domain}";
                            var fullHost = host + ".dbs.local.ravendb.net";
                            var authz = acmeClient.NewAuthorization(new AuthorizationIdentifier
                            {
                                Type = AuthorizationIdentifierTypes.Dns,
                                Value = fullHost
                            }).ContinueWith(t =>
                            {
                                return t.Result.Data.Challenges.First(c => c.Type == ChallengeTypes.Dns01);
                            }, token);
                            dictionary[host] = authz;
                        }

                        await Task.WhenAll(dictionary.Values.ToArray());
                        map = dictionary.ToDictionary(x => x.Key, x => acmeClient.ComputeDnsValue(x.Value.Result));
                    }
                    catch (Exception e)
                    {
                        LogErrorAndThrow(onProgress, progress, $"Failed to receive challenge(s) information from Let's Encrypt. Exception:{Environment.NewLine}{e}", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully received challenge(s) information from Let's Encrypt.");
                    progress.AddInfo("updating DNS record(s) and challenge(s) in dbs.local.ravendb.net. This operation may take a long time, between 30 seconds and " +
                                     "a few minutes, depending on the number of domains(nodes)");
                    onProgress(progress);

                    try
                    {
                        await UpdataDnsRecordsTask(onProgress, token, map);
                    }
                    catch (Exception e)
                    {
                        LogErrorAndThrow(onProgress, progress, $"Failed to update DNS record(s) and challenge(s) in dbs.local.ravendb.net. Exception:{Environment.NewLine}{e}", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully updated DNS record(s) and challenge(s) in dbs.local.ravendb.net.");
                    progress.AddInfo($"Completing Let's Encrypt challenge(s) for {setupInfo.Domain}.dbs.local.ravendb.net.");
                    onProgress(progress);

                    AcmeCertificate cert = null;
                    try
                    {
                        var tasks = new List<Task>();
                        foreach (var kvp in dictionary)
                        {
                            tasks.Add(CompleteAuthorizationFor(acmeClient, kvp.Value.Result, token));
                        }
                        await Task.WhenAll(tasks);

                        var csr = new CertificationRequestBuilder();
                        csr.AddName("CN", "my.dbs.local.ravendb.net");
                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            csr.SubjectAlternativeNames.Add($"{node.Key}.{setupInfo.Domain}.dbs.local.ravendb.net");
                        }
                        cert = await acmeClient.NewCertificate(csr);
                    }
                    catch (Exception e)
                    {
                        LogErrorAndThrow(onProgress, progress, $"Failed to aquire certificate from Let's Encrypt. Exception:{Environment.NewLine}{e}", e);
                    }

                    try
                    {
                        var pfxBuilder = cert.ToPfx();

                        var certBytes = pfxBuilder.Build(setupInfo.Domain + " cert", "");
                        setupInfo.NodeSetupInfos[LocalNodeTag].Certificate = Convert.ToBase64String(certBytes);
                    }
                    catch (Exception e)
                    {
                        LogErrorAndThrow(onProgress, progress, $"Failed to build certificate from Let's Encrypt. Exception:{Environment.NewLine}{e}", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
                    progress.AddInfo("Creating new RavenDB configuration settings.");
                    onProgress(progress);

                    try
                    {
                        progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(token, SetupMode.LetsEncrypt, setupInfo);
                    }
                    catch (Exception e)
                    {
                        LogErrorAndThrow(onProgress, progress, $"Failed to create configuration settings. Exception:{Environment.NewLine}{e}", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully created new configuration settings.");
                    onProgress(progress);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Setting up RavenDB in Let's Encrypt security mode failed.", e);
            }
            return progress;
        }

        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }

        // Update DNS record(s) and set the let's encrypt challenge(s) in dbs.local.ravendb.net
        private static async Task UpdataDnsRecordsTask(Action<IOperationProgress> onProgress, CancellationToken token, Dictionary<string, string> map)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var serializeObject = JsonConvert.SerializeObject(map);
                HttpResponseMessage response;
                try
                {
                    response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                string responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }

                try
                {
                    RegistrationResult registrationResult;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/registration-result",
                                    new StringContent(responseString, Encoding.UTF8, "application/json"), cts.Token)
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

                        if (registrationResult.Status == RegistrationStatus.Error)
                        {
                            throw new InvalidOperationException($"api.ravendb.net returned an error: {registrationResult.Message}");
                        }

                    } while (registrationResult.Status == RegistrationStatus.Pending);
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new System.TimeoutException("Request failed due to a timeout error", e);
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
                    if(cts.IsCancellationRequested)
                        throw new System.TimeoutException("Timeout expired on completion of ACME authorization");

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

        public static async Task<IOperationResult> SetupValidateTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 1
            };

            progress.AddInfo("Validating that RavenDB can start with the new configuration settings.");
            onProgress(progress);

            try
            {
                var localNode = setupInfo.NodeSetupInfos[LocalNodeTag];
                // can only do this for local cert
                if (PlatformDetails.RunningOnPosix)
                    AdminCertificatesHandler.ValidateCaExistsInOsStores(localNode.Certificate, "local certificate", serverStore);

                var ips = localNode.Ips.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();

                X509Certificate2 localNodeCert;
                byte[] localCertBytes;
                try
                {
                    localCertBytes = Convert.FromBase64String(localNode.Certificate);
                    localNodeCert = new X509Certificate2(localCertBytes, localNode.Password);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Validation failed.Could not load the provided certificate for the local node '{LocalNodeTag}'.", e);
                }

                string localServerUrl = null;
                if (setupMode == SetupMode.LetsEncrypt)
                    localServerUrl = $"https://{LocalNodeTag}.dbs.local.ravendb.net:{localNode.Port}";
                else if (setupMode == SetupMode.Secured)
                {
                    var cn = localNodeCert.SubjectName.Name;
                    localServerUrl = $"https://{cn}:{localNode.Port}";
                }

                await AssertServerCanStartSecured(localNodeCert, localServerUrl, ips, SettingsPath, token, setupInfo);

                // Load the certificate in the local server, so we can generate client certificates later
                serverStore.Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(localNode.Certificate, "Setup Validation", localNodeCert, localCertBytes);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Validation failed. Exception:{Environment.NewLine}{e}", e);
            }

            progress.Processed++;
            progress.AddInfo("Validations successful.");
            onProgress(progress);

            return progress;
        }

        public static void ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo)
        {
            try
            {
                if (setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{LocalNodeTag}'.");
                if (IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid domain name.");
                if (IsValidDomain(setupInfo.Domain) == false)
                    throw new ArgumentException("Invalid domain name.");

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    if (string.IsNullOrWhiteSpace(node.Value.Certificate) && setupMode == SetupMode.Secured)
                        throw new ArgumentException($"{nameof(node.Value.Certificate)} is a mandatory property for a secured setup");

                    if (string.IsNullOrWhiteSpace(node.Key))
                        throw new ArgumentException("Node Tag is a mandatory property for a secured setup");

                    foreach (var ip in node.Value.Ips)
                    {
                        if (IsValidIp(ip) == false)
                            throw new ArgumentException($"Invalid IP: '{ip}' in node '{node.Key}'");
                    }
                }
            }
            catch (Exception e)
            {
                throw new FormatException("Validation of setup information failed. ", e);
            }
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

        private static bool IsValidIp(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip))
                return false;

            var octets = ip.Split('.');
            return octets.Length == 4 && octets.All(o => byte.TryParse(o, out _));
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

        private static async Task<byte[]> CreateSettingsZipAndOptionallyWriteToLocalServer(CancellationToken token, SetupMode setupMode, SetupInfo setupInfo)
        {
            try
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var originalSettings = File.ReadAllText(SettingsPath);
                        dynamic jsonObj = JsonConvert.DeserializeObject(originalSettings);
                        jsonObj["Setup.Mode"] = setupMode.ToString();

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            var nodeServerUrl = string.Empty;
                            if (setupMode == SetupMode.Secured)
                            {
                                try
                                {
                                    var nodeCert = new X509Certificate2(node.Value.Certificate, node.Value.Password);
                                    var cn = nodeCert.SubjectName.Name;
                                    nodeServerUrl = $"https://{cn}:{node.Value.Port}";
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException($"Setup failed.Could not load the provided certificate for node '{node.Key}'.", e);
                                }
                            }
                            else if(setupMode == SetupMode.LetsEncrypt)
                            {
                                nodeServerUrl = $"https://{node.Key}.dbs.local.ravendb.net:{node.Value.Port}";
                            }

                            jsonObj["ServerUrl"] = nodeServerUrl;

                            if (setupMode == SetupMode.LetsEncrypt)
                            {
                                jsonObj["Security.Certificate.Base64"] = setupInfo.NodeSetupInfos[LocalNodeTag].Certificate;
                            }
                            else if (setupMode == SetupMode.Secured)
                            {
                                jsonObj["Security.Certificate.Base64"] = node.Value.Certificate;
                                jsonObj["Security.Certificate.Password"] = node.Value.Password;
                            }

                            var jsonString = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                            if (node.Key == LocalNodeTag && setupInfo.ModifyLocalServer)
                            {
                                try
                                {
                                    WriteSettingsJsonLocally(SettingsPath, jsonString);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException($"Failed to update {SettingsPath} for local node '{node.Key}' with new configuration.", e);
                                }
                            }

                            try
                            {
                                var entry = archive.CreateEntry($"{node.Key}.settings.json");
                                using (var entryStream = entry.Open())

                                using (var writer = new StreamWriter(entryStream))
                                {
                                    writer.Write(jsonString);
                                    writer.Flush();
                                    await entryStream.FlushAsync(token);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Failed to to create zip archive '{node.Key}.settings.json'.", e);
                            }
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

        private class UniqueResponseResponder : IStartup
        {
            private readonly string _response;

            public UniqueResponseResponder(string response)
            {
                _response = response;
            }

            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
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

        public static async Task AssertServerCanStartSecured(X509Certificate2 serverCertificate, string serverUrl, IPEndPoint[] addresses, string settingsPath, CancellationToken token, SetupInfo setupInfo)
        {
            var configuration = new RavenConfiguration(null, ResourceType.Server, settingsPath);
            configuration.Initialize();
            var guid = Guid.NewGuid().ToString();

            try
            {
                var responder = new UniqueResponseResponder(guid);

                var webHost = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(options =>
                    {
                        var port = setupInfo.NodeSetupInfos[LocalNodeTag].Port;
                        if (addresses.Length == 0)
                        {
                            var defaultIp = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port);
                            options.Listen(defaultIp, listenOptions => listenOptions.UseHttps(serverCertificate));
                            if (Logger.IsInfoEnabled)
                                Logger.Info($"List of ip addresses for node {LocalNodeTag} is empty. Webhost listening to {defaultIp}");
                        }

                        foreach (var addr in addresses)
                        {
                            options.Listen(addr, listenOptions => listenOptions.UseHttps(serverCertificate));
                        }
                    })
                    .ConfigureServices(collection =>
                    {
                        collection.AddSingleton(typeof(IStartup), responder);
                    })
                    .UseShutdownTimeout(TimeSpan.FromMilliseconds(150))
                    .Build();

                webHost.Start();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to start webhost with node '{LocalNodeTag}' configuration.{Environment.NewLine}" +
                                                    $"Settings file:{settingsPath}.{Environment.NewLine} " +
                                                    $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.", e);
            }

            using (var client = new HttpClient
            {
                BaseAddress = new Uri(serverUrl)
            })
            {
                HttpResponseMessage response = null;
                string result = null;
                try
                {
                    var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(2).Token);  //2 seconds enough?
                    response = await client.GetAsync("/are-you-there?", cts.Token);
                    response.EnsureSuccessStatusCode();
                    result = await response.Content.ReadAsStringAsync();
                    if (result != guid)
                    {
                        throw new InvalidOperationException($"Expected result guid:{guid} but got {result}.");
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to start contact server {serverUrl}.{Environment.NewLine}" +
                                                        $"Settings file:{settingsPath}.{Environment.NewLine}" +
                                                        $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.{Environment.NewLine}" +
                                                        $"Response: {response?.StatusCode}.{Environment.NewLine}{result}", e);
                }
            }
        }

        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal stripped from authz checks, used by an unauthenticated client during setup only
        public static async Task<byte[]> GenerateCertificateTask(CertificateDefinition certificate,  ServerStore serverStore)
        {
            if (string.IsNullOrWhiteSpace(certificate.Name))
                throw new ArgumentException($"{nameof(certificate.Name)} is a required field in the certificate definition");

            if (serverStore.Server.ClusterCertificateHolder?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' becuase the server certificate is not loaded.");

            if (PlatformDetails.RunningOnPosix)
            {
                AdminCertificatesHandler.ValidateCaExistsInOsStores(certificate.Certificate, certificate.Name, serverStore);
            }

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificate.Name, serverStore.Server.ClusterCertificateHolder);
            
            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint,
                new CertificateDefinition
                {
                    Name = certificate.Name,
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                    Permissions = certificate.Permissions,
                    SecurityClearance = certificate.SecurityClearance,
                    Thumbprint = selfSignedCertificate.Thumbprint
                }));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);
        }

        public static Task RenewLetsEncryptCertificate(ServerStore serverStore)
        {
            var serverCertificate = serverStore.Server.ClusterCertificateHolder.Certificate;
            if (serverCertificate != null && (serverCertificate.NotAfter - DateTime.Today).TotalDays > 31)
                return Task.CompletedTask;

            // Need to renew:
            // 1. read license from cluster
            // 2. contact grisha and ask for email
            // 3. extract the domain from current certificate
            // 4. create new LetsEncryptSetupInfo and call FetchCertificateTask

            return Task.CompletedTask;
        }
    }
}
