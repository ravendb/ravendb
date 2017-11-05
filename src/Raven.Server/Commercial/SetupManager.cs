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
    public class SetupManager : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public const string SettingsPath = "settings.json";
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public static readonly Uri LetsEncryptServer = WellKnownServers.LetsEncryptStaging;

        /*  TODO
         *  review timeouts, decide on values
            handle thread safety
            Change priority of certificate selection
            Remove one of the server certificates
            call token ThrowIfCancellationRequested() in proper places
         */


        private readonly ServerStore _serverStore;
        public Timer CertificateRenewalTimer { get; set; }

        private SetupStage _lastSetupStage = SetupStage.Initial;
        private string _email;
        private SetupInfo _setupInfo;
        private string _localServerUrl;
        private X509Certificate2 _localCertificate;
        private byte[] _localCertificateBytes;
        private string _localCertificateBase64;
        private string _clientCertKey;
        private string _originalSettings;

        public SetupManager(ServerStore serverStore)
        {
            _serverStore = serverStore;

            if (_serverStore.Configuration.Core.SetupMode == SetupMode.Initial)
            {
                _lastSetupStage = SetupStage.Initial;
                _email = null;
                _setupInfo = null;
                _localServerUrl = null;
                _localCertificate = null;
                _localCertificateBytes = null;
                _localCertificateBase64 = null;
            }

            if (_serverStore.Configuration.Core.SetupMode == SetupMode.LetsEncrypt)
            {
                // TODO If we are the leader, start the timer with the renew task
            }
        }
        
        public async Task<Uri> LetsEncryptAgreement(string email)
        {
            MoveToStage(SetupStage.Agreement);
            if (IsValidEmail(email) == false)   
                throw new ArgumentException("Invalid e-mail format" + email);

            _email = email;

            using (var acmeClient = new AcmeClient(LetsEncryptServer))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 1
            };
            progress.AddInfo("Setting up RavenDB in secured mode.");
            progress.AddInfo("Creating new RavenDB configuration settings.");
            onProgress(progress);
            
            MoveToStage(SetupStage.Setup);
			_setupInfo = setupInfo;
            ValidateSetupInfo(SetupMode.LetsEncrypt);

            var localNodeInfo = _setupInfo.NodeSetupInfos[LocalNodeTag];
            _localCertificateBase64 = localNodeInfo.Certificate;

            try
            {
                _localCertificate = new X509Certificate2(_localCertificateBase64, localNodeInfo.Password);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Setup failed. Could not load the provided certificate for node '{LocalNodeTag}'. Exception:{Environment.NewLine}{e}", e);
            }

            var cn = _localCertificate.SubjectName.Name;
            _localServerUrl = $"https://{cn}:{localNodeInfo.Port}";
            
            try
            {
                progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, token, SetupMode.Secured);
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
        
        public async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };
            progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
            onProgress(progress);

            MoveToStage(SetupStage.Setup);
            ValidateSetupInfo(SetupMode.LetsEncrypt);
            _setupInfo = setupInfo;
            
            progress.AddInfo($"Getting challenge from Let's Encrypt. Using e-mail: {_email}.");
            onProgress(progress);

            try
            {
                using (var acmeClient = new AcmeClient(LetsEncryptServer))
                {
                    var dictionary = new Dictionary<string, Task<Challenge>>();
                    Dictionary<string, string> map = null;
                    try
                    {
                        var account = await acmeClient.NewRegistraton("mailto:" + _email);
                        account.Data.Agreement = account.GetTermsOfServiceUri();
                        await acmeClient.UpdateRegistration(account);
                        
                        foreach (var node in _setupInfo.NodeSetupInfos)
                        {
                            var host = $"{node.Key}.{_setupInfo.Domain}";
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
                        await UpdataDnsRecordsTask(onProgress, token, map, progress);
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
                        foreach (var node in _setupInfo.NodeSetupInfos)
                        {
                            csr.SubjectAlternativeNames.Add($"{node.Key}.{_setupInfo.Domain}.dbs.local.ravendb.net");
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

                        _localCertificateBytes = pfxBuilder.Build(setupInfo.Domain + " cert", "");
                        _localCertificateBase64 = Convert.ToBase64String(_localCertificateBytes);
                        _localCertificate = new X509Certificate2(_localCertificateBytes);
                        _localServerUrl = $"https://a.dbs.local.ravendb.net:{_setupInfo.NodeSetupInfos[LocalNodeTag].Port}";

                        progress.AddInfo("Preparing congifuratin file(s).");
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
                        progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, token, SetupMode.LetsEncrypt);
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
        private async Task UpdataDnsRecordsTask(Action<IOperationProgress> onProgress, CancellationToken token, Dictionary<string, string> map, SetupProgressAndResult progress)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(120000).Token);  //120 seconds enough? what if there are lots of nodes?
            HttpResponseMessage response;
            try
            {
                ApiHttpClient.Instance.Timeout = TimeSpan.FromSeconds(10); // what timeout to set? default is 100 seconds
                response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/register",
                    new StringContent(JsonConvert.SerializeObject(map), Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Registration request to dbs.local.ravendb.net failed. Map:{Environment.NewLine}{map}", e);
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw new InvalidOperationException($"Got unsuccessful response from registration request:{response.StatusCode}.{Environment.NewLine}{responseString}");
            }
            
            RegistrationResult registrationResult;

            do
            {
                await Task.Delay(1000, cts.Token);
                try
                {
                    response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/registration-result",
                            new StringContent(JsonConvert.SerializeObject("what to send here?"), Encoding.UTF8, "application/json"),cts.Token)
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration-result request to dbs.local.ravendb.net failed.", e); //add the object we tried to send to error
                }

                registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));

                if (registrationResult.Status == RegistrationStatus.Error)
                {
                    throw new InvalidOperationException($"dbs.local.ravendb.net returned an error: {registrationResult.Message}");
                }

            } while (registrationResult.Status == RegistrationStatus.Pending);
        }

        private static async Task CompleteAuthorizationFor(AcmeClient client, Challenge dnsChallenge, CancellationToken token)
        {
            var challenge = await client.CompleteChallenge(dnsChallenge);

            var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(10000).Token);  // what is the timeout?
            while (true)
            {
                cts.Token.ThrowIfCancellationRequested();

                var authz = await client.GetAuthorization(challenge.Location);
                if (authz.Data.Status == EntityStatus.Pending)
                {
                    await Task.Delay(250, cts.Token);
                    continue;
                }

                if (authz.Data.Status == EntityStatus.Valid)
                    return;

                throw new InvalidOperationException("Failed to authorize certificate: " + authz.Data.Status);
            }
        }

        public async Task<IOperationResult> SetupValidateTask(Action<IOperationProgress> onProgress, CancellationToken token)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 1
            };

            progress.AddInfo("Validating that RavenDB can start with the new configuration settings.");
            onProgress(progress);

            MoveToStage(SetupStage.Validation);

            try
            {
                // can only do this for local cert
                if (PlatformDetails.RunningOnPosix)
                    AdminCertificatesHandler.ValidateCaExistsInOsStores(_localCertificateBase64, "local certificate", _serverStore);

                var localNode = _setupInfo.NodeSetupInfos[LocalNodeTag];
                var ips = localNode.Ips.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();
                
                await AssertServerCanStartSecured(_localCertificate, _localServerUrl, ips, SettingsPath, token);

                // Load the certificate in the local server, so we can generate client certificates later
                _serverStore.Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(_localCertificateBase64, "Setup Validation", _localCertificate, _localCertificateBytes);
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

        public void ValidateSetupInfo(SetupMode setupMode)
        {
            try
            {
                if (_setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{LocalNodeTag}'.");
                if (IsValidDomain(_setupInfo.Domain) == false)
                    throw new ArgumentException("Invalid domain name.");

                foreach (var node in _setupInfo.NodeSetupInfos)
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
        
        public bool IsValidEmail(string email)
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

        private bool IsValidIp(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip))
                return false;
            
            var octets = ip.Split('.');
            return octets.Length == 4 && octets.All(o => byte.TryParse(o, out _));
        }

        private bool IsValidDomain(string domain)
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

        private async Task<byte[]> CreateSettingsZipAndOptionallyWriteToLocalServer(Action<IOperationProgress> onProgress, CancellationToken token, SetupMode setupMode)
        {
            //TODO handle progress, logs and errors
            try
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        _originalSettings = File.ReadAllText(SettingsPath);
                        dynamic jsonObj = JsonConvert.DeserializeObject(_originalSettings);
                        jsonObj["Setup.Mode"] = setupMode.ToString();

                        foreach (var node in _setupInfo.NodeSetupInfos)
                        {
                            jsonObj["ServerUrl"] = _localServerUrl;

                            if (setupMode == SetupMode.LetsEncrypt)
                            {
                                jsonObj["Security.Certificate.Base64"] = _localCertificateBase64;
                            }
                            else if (setupMode == SetupMode.Secured)
                            {
                                jsonObj["Security.Certificate.Base64"] = node.Value.Certificate;
                                jsonObj["Security.Certificate.Password"] = node.Value.Password;
                            }
                            
                            var jsonString = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                            if (node.Key == LocalNodeTag && _setupInfo.ModifyLocalServer)
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
                throw new InvalidOperationException("Failed to create setting file(s).");
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

        public async Task AssertServerCanStartSecured(X509Certificate2 serverCertificate, string serverUrl, IPEndPoint[] addresses, string settingsPath, CancellationToken token)
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
                        var port = _setupInfo.NodeSetupInfos[LocalNodeTag].Port;
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
        public async Task<byte[]> GenerateCertificateTask(CertificateDefinition certificate)
        {
            MoveToStage(SetupStage.GenarateCertificate);

            if (string.IsNullOrWhiteSpace(certificate.Name))
                throw new ArgumentException($"{nameof(certificate.Name)} is a required field in the certificate definition");

            if (_serverStore.Server.ClusterCertificateHolder?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' becuase the server certificate is not loaded.");

            if (PlatformDetails.RunningOnPosix)
            {
                AdminCertificatesHandler.ValidateCaExistsInOsStores(certificate.Certificate, certificate.Name, _serverStore);
            }

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificate.Name, _serverStore.Server.ClusterCertificateHolder);

            // save the key so we can delete the file if cleanup is necessary
            _clientCertKey = Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint;

            var res = await _serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint,
                new CertificateDefinition
                {
                    Name = certificate.Name,
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                    Permissions = certificate.Permissions,
                    SecurityClearance = certificate.SecurityClearance,
                    Thumbprint = selfSignedCertificate.Thumbprint
                }));
            await _serverStore.Cluster.WaitForIndexNotification(res.Index);

            return selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);
        }

        public Task RenewLetsEncryptCertificate()
        {
            var serverCertificate = _serverStore.Server.ClusterCertificateHolder.Certificate;
            if (serverCertificate != null && (serverCertificate.NotAfter - DateTime.Today).TotalDays > 31)
                return Task.CompletedTask;

            // Need to renew:
            // 1. read license from cluster
            // 2. contact grisha and ask for email
            // 3. extract the domain from current certificate
            // 4. create new LetsEncryptSetupInfo and call FetchCertificateTask

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            CertificateRenewalTimer?.Dispose();
        }

        public void MoveToStage(SetupStage stage)
        {
            _lastSetupStage = stage;
        }

        public async Task CleanCurrentStage()
        {
            switch (_lastSetupStage)
            {
                case SetupStage.Agreement:
                    CleanAgreementStage();
                    break;
                case SetupStage.Setup:
                    CleanSetupStage();
                    break;
                case SetupStage.Validation:
                    CleanValidationStage();
                    break;
                case SetupStage.GenarateCertificate:
                    await CleanCertificateStage();
                    break;
                case SetupStage.Initial:
                    throw new InvalidOperationException("Cannot go back from Initial stage.");
            }
        }

        public async Task GoToPreviousStage()
        {
            await CleanCurrentStage();
        }

        public async Task RestartSetup()
        {
            CleanAgreementStage();
            CleanSetupStage();
            CleanValidationStage();
            await CleanCertificateStage();
            _lastSetupStage = SetupStage.Initial;
        }

        private async Task CleanCertificateStage()
        {
            try
            {
                if (_lastSetupStage < SetupStage.GenarateCertificate)
                    return;

                var deleteResult = await _serverStore.SendToLeaderAsync(new DeleteCertificateFromClusterCommand
                {
                    Name = _clientCertKey
                });
                await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);

                _clientCertKey = null;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Setup: error when deleting certificate from old setup.", e);
            }
        }

        private void CleanValidationStage()
        {
            if (_lastSetupStage < SetupStage.Validation)

                return;
            _serverStore.Server.ClusterCertificateHolder = null;
        }

        private void CleanSetupStage()
        {
            if (_lastSetupStage < SetupStage.Setup)
                return;

            if (_setupInfo.ModifyLocalServer && _originalSettings != null)
            {
                try
                {
                    WriteSettingsJsonLocally(SettingsPath, _originalSettings);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to revert {SettingsPath} to its original configuration.", e);
                }
            }

            _setupInfo = null;
            _localServerUrl = null;
            _localCertificate = null;
            _localCertificateBytes = null;
            _localCertificateBase64 = null;
            _originalSettings = null;
        }

        private void CleanAgreementStage()
        {
            if (_lastSetupStage < SetupStage.Agreement)
                return;

            _email = null;
        }
    }
}
