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
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
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
        public static string SettingsPath = Path.Combine(AppContext.BaseDirectory, "settings.json");
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public static readonly Uri LetsEncryptServer = WellKnownServers.LetsEncryptStaging;

        /*  TODO
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

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 1
            };
            progress.AddInfo("Setting up RavenDB in secured mode.");
            progress.AddInfo("Creating new RavenDB configuration settings.");
            onProgress(progress);

            ValidateSetupInfo(SetupMode.Secured, setupInfo);

            try
            {
                progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, progress, token, SetupMode.Secured, setupInfo, serverStore);
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

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress,  CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };
            progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
            onProgress(progress);

            ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo);

            progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
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
                            dictionary[node.Key] = authz;
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
                        await UpdateDnsRecordsTask(onProgress, progress, token, map, setupInfo);
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
                        var lowerDomain = setupInfo.Domain.ToLower();
                        csr.AddName($"CN=a.{lowerDomain}.dbs.local.ravendb.net");
                        // we need at least one SAN, browsers today require this
                        csr.SubjectAlternativeNames.Add($"{LocalNodeTag}.{setupInfo.Domain}.dbs.local.ravendb.net");
                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            if (node.Key == LocalNodeTag)
                                continue;
                            csr.SubjectAlternativeNames.Add($"{node.Key.ToLower()}.{lowerDomain}.dbs.local.ravendb.net");
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
                        progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, progress, token, SetupMode.LetsEncrypt, setupInfo, serverStore);
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
        private static async Task UpdateDnsRecordsTask(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, CancellationToken token, Dictionary<string, string> map, SetupInfo setupInfo)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var domainAndChallenge in map)
                {
                    progress.AddInfo($"Creating Dns record/challenge for node {domainAndChallenge.Key}.");
                    onProgress(progress);
                    var regNodeInfo = new RegistrationNodeInfo()
                    {
                        SubDomain = domainAndChallenge.Key,
                        Challenge = domainAndChallenge.Value,
                        Ips = setupInfo.NodeSetupInfos[domainAndChallenge.Key].Ips
                    };
                    registrationInfo.SubDomains.Add(regNodeInfo);
                }
                
                var serializeObject = JsonConvert.SerializeObject(registrationInfo);
                HttpResponseMessage response;
                try
                {
                    progress.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                    progress.AddInfo("Waiting...");
                    onProgress(progress);
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

                var id = JsonConvert.DeserializeObject<Dictionary<string,string>>(responseString).First().Value;

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
                    progress.AddInfo("Got successful response from api.ravendb.net.");
                    progress.AddInfo("Waiting...");
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
                try
                {
                    var localCertBytes = Convert.FromBase64String(localNode.Certificate);
                    localNodeCert = localNode.Password == null
                        ? new X509Certificate2(localCertBytes)
                        : new X509Certificate2(localCertBytes, localNode.Password);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Validation failed.Could not load the provided certificate for the local node '{LocalNodeTag}'.", e);
                }

                // TODO go over all occurences of node tag, use lower everywhere...
                var localServerUrl = (setupMode == SetupMode.LetsEncrypt)
                    ? $"https://{LocalNodeTag.ToLower()}.dbs.local.ravendb.net:{localNode.Port}"
                    : setupInfo.NodeSetupInfos[LocalNodeTag].ServerUrl;

                await AssertServerCanStartSecured(localNodeCert, localServerUrl, ips, SettingsPath, token, setupInfo);

                // Load the certificate in the local server, so we can generate client certificates later
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
                if (setupMode == SetupMode.LetsEncrypt)
                {
                    if (setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                        throw new ArgumentException($"At least one of the nodes must have the node tag '{LocalNodeTag}'.");
                    if (IsValidEmail(setupInfo.Email) == false)
                        throw new ArgumentException("Invalid email address.");
                    if (IsValidDomain(setupInfo.Domain) == false)
                        throw new ArgumentException("Invalid domain name.");
                }

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

        private static async Task<byte[]> CreateSettingsZipAndOptionallyWriteToLocalServer(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, CancellationToken token, SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            try
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var originalSettings = File.ReadAllText(SettingsPath);
                        dynamic jsonObj = JsonConvert.DeserializeObject(originalSettings);

                        progress.AddInfo("Loading and validating server certificate.");
                        onProgress(progress);
                        byte[] serverCertBytes;

                        try
                        {
                            var base64 = setupInfo.NodeSetupInfos[LocalNodeTag].Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            var serverCert = setupInfo.NodeSetupInfos[LocalNodeTag].Password == null
                                ? new X509Certificate2(serverCertBytes)
                                : new X509Certificate2(serverCertBytes, setupInfo.NodeSetupInfos[LocalNodeTag].Password);
                            serverStore.Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(base64, "Setup", serverCert, serverCertBytes, setupInfo.NodeSetupInfos[LocalNodeTag].Password);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not load the certificate for node '{LocalNodeTag}' in the local server.", e);
                        }

                        progress.AddInfo("Generaing the client certificate.");
                        onProgress(progress);
                        X509Certificate2 clientCert;

                        try
                        {
                            clientCert = await GenerateCertificateTask($"{setupInfo.Domain}.client.certificate", serverStore);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not generate a client certificate '{LocalNodeTag}' in the local server.", e);
                        }

                        progress.AddInfo("Writing certificates to zip archive.");
                        onProgress(progress);
                        try
                        {
                            var entry = archive.CreateEntry($"{setupInfo.Domain}.server.certificate.pfx");
                            using (var entryStream = entry.Open())
                            using (var writer = new BinaryWriter(entryStream))
                            {
                                writer.Write(serverCertBytes);
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                            entry = archive.CreateEntry($"{setupInfo.Domain}.client.certificate.pfx");
                            using (var entryStream = entry.Open())
                            using (var writer = new BinaryWriter(entryStream))
                            {
                                writer.Write(clientCert.Export(X509ContentType.Pfx));
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                            entry = archive.CreateEntry($"{setupInfo.Domain}.client.certificate.pem");
                            using (var entryStream = entry.Open())
                            using (var writer = new StreamWriter(entryStream))
                            {
                                var builder = new StringBuilder();
                                builder.AppendLine("-----BEGIN CERTIFICATE-----");
                                builder.AppendLine(Convert.ToBase64String(clientCert.Export(X509ContentType.Cert), Base64FormattingOptions.InsertLineBreaks));
                                builder.AppendLine("-----END CERTIFICATE-----");

                                writer.Write(builder.ToString());
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }
                        
                        jsonObj["Setup.Mode"] = setupMode.ToString();

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            progress.AddInfo($"Creating settings file 'settings.josn' for node {node.Key}.");
                            onProgress(progress);

                            if (setupMode == SetupMode.Secured)
                            {
                                jsonObj["ServerUrl"] = node.Value.ServerUrl;
                                jsonObj["Security.Certificate.Base64"] = node.Value.Certificate;
                                jsonObj["Security.Certificate.Password"] = node.Value.Password;
                            }
                            else if(setupMode == SetupMode.LetsEncrypt)
                            {
                                jsonObj["ServerUrl"] = $"https://{node.Key.ToLower()}.{setupInfo.Domain}.dbs.local.ravendb.net:{node.Value.Port}";
                                jsonObj["Security.Certificate.Base64"] = setupInfo.NodeSetupInfos[LocalNodeTag].Certificate;
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
                                    throw new InvalidOperationException("Failed to write settings file 'settings.josn' for the local sever.", e);
                                }
                            }

                            progress.AddInfo($"Adding settings file '{node.Key}.settings.json' to zip archive.");
                            onProgress(progress);
                            try
                            {
                                var entry = archive.CreateEntry($"{node.Key}\\settings.json");
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
                    .UseSetting(WebHostDefaults.ApplicationKey, "Simulation")
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
        public static async Task<X509Certificate2> GenerateCertificateTask(string name,  ServerStore serverStore)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException($"{nameof(name)} is a required field in the certificate definition");

            if (serverStore.Server.ClusterCertificateHolder?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' becuase the server certificate is not loaded.");
            
            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.ClusterCertificateHolder);

            var newCertDef = new CertificateDefinition
            {
                Name = name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint
            };

            if (PlatformDetails.RunningOnPosix)
            {
                AdminCertificatesHandler.ValidateCaExistsInOsStores(newCertDef.Certificate, newCertDef.Name, serverStore);
            }

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint, newCertDef));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return selfSignedCertificate;
        }
    }
}
