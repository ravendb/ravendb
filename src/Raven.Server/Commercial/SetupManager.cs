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
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Commercial
{
    public class SetupManager : IDisposable
    {
        public const string SettingsPath = "settings.json";
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public static readonly Uri LetsEncryptServer = WellKnownServers.LetsEncryptStaging;

        /*  TODO
            Go over everything line by line, add proper errors messages, exceptions and logs.
            Handle timeouts and proper error hanling for requests to ravendb api
            fix progress messages
            Change priority of certificate selection
            Remove one of the server certificates
            call token ThrowIfCancellationRequested() in proper places

            Marcin: handler for getting cn from cert -> why is this needed
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
            AssertCorrectSetupStage(SetupStage.Agreement);
            // validate email
            _email = email;

            using (var acmeClient = new AcmeClient(LetsEncryptServer))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            //TODO handle progress, logs and errors
            AssertCorrectSetupStage(SetupStage.Setup);
            ValidateSetupInfo(SetupMode.LetsEncrypt);

            _setupInfo = setupInfo;

            var localNodeInfo = _setupInfo.NodeSetupInfos[LocalNodeTag];
            _localCertificateBase64 = localNodeInfo.Certificate;
            try
            {
                _localCertificate = new X509Certificate2(_localCertificateBase64, localNodeInfo.Password);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            var cn = _localCertificate.SubjectName.Name;
            _localServerUrl = $"https://{cn}:{localNodeInfo.Port}";

            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 2
            };
            
            
            try
            {
                progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, token, SetupMode.Secured);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create RavenDB with the provided certificate(s).", e);
            }

            progress.Processed++;
            progress.AddInfo("Stage 2: Finished.");
            progress.AddInfo("Finished setting up RavenDB in secured mode.");
            onProgress(progress);
            return progress;
        }

        public void AssertCorrectSetupStage(SetupStage currentStage)
        {
            if (_lastSetupStage >= currentStage)
                throw new InvalidOperationException($"Tried to initiate stage {currentStage.ToString()} of the setup process but setup has already completed stage {_lastSetupStage.ToString()}. " +
                                                    $"To restart the setup process, please set \"SetupMode\": \"Initial\" in {SettingsPath} and restart the server.");
            _lastSetupStage++;
        }

        public async Task CleanLastStage()
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
                case SetupStage.Finish:
                    throw new InvalidOperationException("Cannot go back, setup has finished.");
            }
            _lastSetupStage--;
        }

        public async Task ResetSetup()
        {
            CleanAgreementStage();
            CleanSetupStage();
            CleanValidationStage();
            await CleanCertificateStage();
            _lastSetupStage = SetupStage.Initial;
        }

        private async Task CleanCertificateStage()
        {
            var deleteResult = await _serverStore.SendToLeaderAsync(new DeleteCertificateFromClusterCommand
            {
                Name = _clientCertKey
            });
            await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);

            _clientCertKey = null;
        }

        private void CleanValidationStage()
        {
            _serverStore.Server.ClusterCertificateHolder = null;
        }

        private void CleanSetupStage()
        {
            if (_setupInfo.ModifyLocalServer && _originalSettings != null)
            {
                try
                {
                    WriteSettingsJsonLocally(SettingsPath, _originalSettings);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to update {SettingsPath} for local node \"{LocalNodeTag}\" with new configuration.", e);
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
            _email = null;
        }

        public async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo)
        {
            //TODO handle progress, logs and errors

            AssertCorrectSetupStage(SetupStage.Setup);
            ValidateSetupInfo(SetupMode.LetsEncrypt);
            _setupInfo = setupInfo;

            // Total of 3 stages in this operation
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 3
            };


            progress.AddInfo($"Stage1: Getting challenge from Let's Encrypt. Using e-mail: {_email}.");
            onProgress(progress);

            try
            {
                using (var acmeClient = new AcmeClient(LetsEncryptServer))
                {
                    var account = await acmeClient.NewRegistraton("mailto:" + _email);
                    account.Data.Agreement = account.GetTermsOfServiceUri();
                    await acmeClient.UpdateRegistration(account);

                    var dictionary = new Dictionary<string, Task<Challenge>>();
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
                    var map = dictionary.ToDictionary(x => x.Key, x => acmeClient.ComputeDnsValue(x.Value.Result));

                    progress.Processed++;
                    progress.AddInfo("Stage1: Finished.");
                    progress.AddInfo($"Stage2: Claiming {_setupInfo.Domain} from {RavenDbDomain}. This may take a long time, 30-90 seconds.");
                    onProgress(progress);


                    // TODO fix this entire process of contacting ravendb.net. need to set proper timeout, and organize the error handling
                    await UpdataDnsRecordsTask(onProgress, token, map, progress);

                    progress.Processed++;
                    progress.AddInfo("Stage2: Finished.");
                    progress.AddInfo($"Stage3: Completing Let's Encrypt challenge for domain {setupInfo.Domain}.");
                    onProgress(progress);

                    var tasks = new List<Task>();
                    foreach (var kvp in dictionary)
                    {
                        tasks.Add(CompleteAuthorizationFor(acmeClient, kvp.Value.Result));
                    }
                    await Task.WhenAll(tasks);

                    try
                    {
                        var csr = new CertificationRequestBuilder();
                        csr.AddName("CN", "my.dbs.local.ravendb.net");
                        foreach (var node in _setupInfo.NodeSetupInfos)
                        {
                            csr.SubjectAlternativeNames.Add($"{node.Key}.{_setupInfo.Domain}.dbs.local.ravendb.net");
                        }
                        var cert = await acmeClient.NewCertificate(csr);

                        var pfxBuilder = cert.ToPfx();

                        _localCertificateBytes = pfxBuilder.Build(setupInfo.Domain + " cert", "");
                        _localCertificateBase64 = Convert.ToBase64String(_localCertificateBytes);
                        _localCertificate = new X509Certificate2(_localCertificateBytes);
                        _localServerUrl = $"https://a.dbs.local.ravendb.net:{_setupInfo.NodeSetupInfos[LocalNodeTag].Port}";

                        progress.SettingsZipFile = await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, token, SetupMode.LetsEncrypt);
                    }
                    catch (Exception e)
                    {
                        progress.AddError("Stage3: Failed to save certificate from Let's Encrypt." + e);
                        onProgress(progress);
                        return null;
                    }
                        
                    progress.Processed++;
                    progress.AddInfo("Stage3: Finished.");
                    progress.AddInfo("Finished setting up RavenDB with a Let's Encrypt certificate.");
                    onProgress(progress);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to complete dns challenge from Let's Encrypt.", e);
            }
            return progress;
        }

        private async Task UpdataDnsRecordsTask(Action<IOperationProgress> onProgress, CancellationToken token, Dictionary<string, string> map, SetupProgressAndResult progress)
        {            
            // TODO fix this entire process of contacting ravendb.net. need to set proper timeout + add logs,exceptions,error messages
            HttpResponseMessage response;
            try
            {
                // Update DNS record in dbs.local.ravendb.net and set the let's encrypt challenge
                ApiHttpClient.Instance.Timeout = TimeSpan.FromSeconds(10);
                response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/register",
                    new StringContent(JsonConvert.SerializeObject(map), Encoding.UTF8, "application/json"), token).ConfigureAwait(false);


                if (response.IsSuccessStatusCode == false)
                {
                    var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    progress.AddError($"Stage2: Failed. Cannot complete setup: {response.StatusCode}.\r\n{responseString}");
                    onProgress(progress);
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e); //cannot reach server...
                throw;
            }

            var i = 0;
            while (true)
            {
                await Task.Delay(1000, token);
                try
                {
                    response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/registration-result",
                            new StringContent(JsonConvert.SerializeObject("what to send here?"), Encoding.UTF8, "application/json"), token)
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }


                var registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));

                if (registrationResult.Status == RegistrationStatus.Error)
                {
                    var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    progress.AddError("Stage2: Failed. Cannot complete setup. " + responseString);
                    onProgress(progress);
                    return;
                }
                if (registrationResult.Status == RegistrationStatus.Pending)
                {
                    switch (i)
                    {
                        case 60:
                            progress.AddInfo("Stage2: Still Waiting... hang in there, just a few more seconds.");
                            break;
                        case 120:
                            progress.AddInfo("Stage2: Something is wrong. You may wait a little longer but you might as well abort and try again.");
                            break;
                        default:
                            progress.AddInfo("Stage2: Waiting...");
                            break;
                    }
                    onProgress(progress);
                }
                else if (registrationResult.Status == RegistrationStatus.Done)
                    break;
                i++;
            }
            return;
        }

        private static async Task CompleteAuthorizationFor(AcmeClient client, Challenge dnsChallenge)
        {
            var challenge = await client.CompleteChallenge(dnsChallenge);

            for (int i = 0; i < 15; i++)
            {
                var authz = await client.GetAuthorization(challenge.Location);
                if (authz.Data.Status == EntityStatus.Pending)
                {
                    await Task.Delay(250);
                    continue;
                }

                if (authz.Data.Status == EntityStatus.Valid)
                    return;

                throw new InvalidOperationException("Failed to authorize certificate: " + authz.Data.Status);
            }
        }

        public async Task<IOperationResult> SetupValidateTask(Action<IOperationProgress> onProgress, CancellationToken token)
        {
            AssertCorrectSetupStage(SetupStage.Setup);

            //TODO handle progress, logs and errors
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 3
            };

            progress.AddInfo("...");
            onProgress.Invoke(progress);

            try
            {
                // can only do this for local cert
                if (PlatformDetails.RunningOnPosix)
                    AdminCertificatesHandler.ValidateCaExistsInOsStores(_localCertificateBase64, "Let's Encrypt certificate", _serverStore);

                var localNode = _setupInfo.NodeSetupInfos[LocalNodeTag];
                var ips = localNode.Ips.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();

                AssertServerCanStartSecured(_localCertificate, _localServerUrl, ips, SettingsPath);

                // Load the certificate in the local server, so we can generate client certificates later
                _serverStore.Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(_localCertificateBase64, "Setup Vslidation", _localCertificate, _localCertificateBytes);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to start local server with the new configuration {SettingsPath}.", e);
            }
            return new SetupProgressAndResult();
        }

        public void ValidateSetupInfo(SetupMode setupMode)
        {
            try
            {
                if (_setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                    throw new InvalidOperationException($"At least one of the nodes must have the node tag \"{LocalNodeTag}\".");

                foreach (var node in _setupInfo.NodeSetupInfos)
                {
                    if (string.IsNullOrWhiteSpace(node.Value.Certificate) && setupMode == SetupMode.Secured)
                        throw new ArgumentException($"{nameof(node.Value.Certificate)} is a mandatory property for a secured setup");

                    if (string.IsNullOrWhiteSpace(node.Key))
                        throw new ArgumentException("Node Tag is a mandatory property for a secured setup");
                }
                    
                // TODO Validate format of all ips, urls, domains, tags, ports, email
            }
            catch (Exception e)
            {
                throw new FormatException("Validation of setup information failed. ", e);
            }
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
                                    throw new InvalidOperationException($"Failed to update {SettingsPath} for local node \"{node.Key}\" with new configuration.", e);
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
                                
                            }
                        }
                    }
                    return ms.ToArray();
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create setting file(s) .");
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

        public async void AssertServerCanStartSecured(X509Certificate2 serverCertificate, string serverUrl, IPEndPoint[] addresses, string settingsPath)
        {
            var configuration = new RavenConfiguration(null, ResourceType.Server, settingsPath);
            configuration.Initialize();

            var guid = Guid.NewGuid().ToString();
            var responder = new UniqueResponseResponder(guid);

            var webHost = new WebHostBuilder()
                .CaptureStartupErrors(captureStartupErrors: true)
                .UseKestrel(options =>
                {
                    var port = _setupInfo.NodeSetupInfos[LocalNodeTag].Port;
                    if (addresses.Length == 0)
                        options.Listen(new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port), listenOptions => listenOptions.UseHttps(serverCertificate));

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

            using (var client = new HttpClient
            {
                BaseAddress = new Uri(serverUrl)
            })
            {
                // wrap errors
                var response = await client.GetAsync("/are-you-there?");
                response.EnsureSuccessStatusCode();
                var result = await response.Content.ReadAsStringAsync();
                if (result != guid)
                {
                    throw new InvalidOperationException("Not a match");
                }
            }
        }

        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal stripped from authz checks, used by an unauthenticated client during setup only
        public async Task<byte[]> GenerateCertificateTask(CertificateDefinition certificate)
        {
            AssertCorrectSetupStage(SetupStage.GenarateCertificate);

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
            // 0. Assert not in setup mode
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

        public string GetServerUrl()
        {
            return _localServerUrl;
        }
    }
}
