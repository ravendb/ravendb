using System;
using System.Collections.Generic;
using System.IO;
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
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Web.Authentication;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Commercial
{
    public class SetupManager : IDisposable
    {
        public const string SettingsFileName = "settings.json";
        public const string RavenDbDomain = "dbs.local.ravendb.net";

        private readonly ServerStore _serverStore;
        public Timer CertificateRenewalTimer { get; set; }

        public SetupManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            
            // TODO If we are the leader (and in lets encrypt setup mode), start the timer with the renew task
        }
        
        public async Task<Uri> LetsEncryptAgreement(string email)
        {
            using (var acmeClient = new AcmeClient(WellKnownServers.LetsEncryptStaging))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public async Task<IOperationResult> FetchCertificateTask(Action<IOperationProgress> onProgress, CancellationToken token, SecuredSetupInfo setupInfo)
        {
            // Total of 3 stages in this operation
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 3
            };
            progress.AddInfo("Setting up RavenDB with a Let's Encrypt certificate.");
            progress.AddInfo($"Stage1: Getting challenge from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
            onProgress(progress);

            try
            {
                using (var acmeClient = new AcmeClient(WellKnownServers.LetsEncryptStaging))
                {
                    var account = await acmeClient.NewRegistraton("mailto:" + setupInfo.Email);
                    account.Data.Agreement = account.GetTermsOfServiceUri();
                    await acmeClient.UpdateRegistration(account);

                    var authz = await acmeClient.NewAuthorization(new AuthorizationIdentifier
                    {
                        Type = AuthorizationIdentifierTypes.Dns,
                        Value = setupInfo.Domain + RavenDbDomain
                    }); 

                    var challenge = authz.Data.Challenges.First(c => c.Type == ChallengeTypes.Dns01);

                    setupInfo.Challenge = acmeClient.ComputeDnsValue(challenge);

                    progress.Processed++;
                    progress.AddInfo("Stage1: Finished.");
                    progress.AddInfo($"Stage2: Claiming {setupInfo.Domain} from {RavenDbDomain}. This may take a long time, 30-90 seconds.");
                    onProgress(progress);


                    // Update DNS record in dbs.local.ravendb.net and set the let's encrypt challenge
                    var response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/register",
                            new StringContent(JsonConvert.SerializeObject(setupInfo), Encoding.UTF8, "application/json"), token).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        progress.AddError($"Stage2: Failed. Cannot complete setup: {response.StatusCode}.\r\n{responseString}");
                        onProgress(progress);
                        return null;
                    }

                    var i = 0;
                    while (true)
                    {
                        await Task.Delay(1000, token);
                        response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/registration-result",
                                new StringContent(JsonConvert.SerializeObject(setupInfo), Encoding.UTF8, "application/json"), token)
                            .ConfigureAwait(false);

                        var registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                        
                        if (registrationResult.Status == RegistrationStatus.Error)
                        {
                            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                            progress.AddError("Stage2: Failed. Cannot complete setup. " + responseString);
                            onProgress(progress);
                            return null;
                        }
                        if (registrationResult.Status == RegistrationStatus.Pending)
                        {
                            switch (i)
                            {
                                case 30:
                                    progress.AddInfo("Stage2: Still Waiting...");
                                    break;
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

                    progress.Processed++;
                    progress.AddInfo("Stage2: Finished.");
                    progress.AddInfo($"Stage3: Completing Let's Encrypt challenge for domain {setupInfo.Domain}.");
                    onProgress(progress);
                    
                    var challengeResult = await acmeClient.CompleteChallenge(challenge);

                    for (i = 0; i < 15; i++)
                    {
                        authz = await acmeClient.GetAuthorization(challengeResult.Location);
                        if (authz?.Data.Status != EntityStatus.Pending)
                            break;

                        await Task.Delay(250, token);
                    }

                    if (authz != null && authz.Data.Status != EntityStatus.Valid)
                    {
                        progress.AddError($"Stage3: Failed to authorize with Let\'s Encrypt: {authz.Data.Status}\r\n{authz.Json}"); 
                        onProgress(progress);
                        return null;
                    }

                    try
                    {
                        var csr = new CertificationRequestBuilder();
                        csr.AddName($"CN={setupInfo.Domain}");
                        csr.SubjectAlternativeNames.Add(setupInfo.Domain);
                        
                        // TODO make sure this is allowed, otherwise we need a challenge for every single node seperately
                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            csr.SubjectAlternativeNames.Add($"{node.NodeTag}.{setupInfo.Domain}");
                        }
                        var cert = await acmeClient.NewCertificate(csr);

                        var pfxBuilder = cert.ToPfx();
                        var certBytes = pfxBuilder.Build(setupInfo.Domain + " cert", "");
                        var base64Cert = Convert.ToBase64String(certBytes);

                        if (PlatformDetails.RunningOnPosix)
                            AdminCertificatesHandler.ValidateCaExistsInOsStores(base64Cert, "Let's Encrypt certificate", _serverStore);
                        

                        // Prepare settings.json files, and write the local one to disk
                        var settingsPath = SettingsFileName;
                        var jsons = new Dictionary<string, string>();
                        SecuredSetupInfo.NodeInfo localNode = null;

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            try
                            {
                                if (node.NodeTag == "A")
                                {
                                    jsons.Add(node.NodeTag, WriteSettingsJsonFile(node.Certificate, node.PublicServerUrl, node.ServerUrl, settingsPath, SetupMode.LetsEncrypt, modifyLocalServer: true));
                                    localNode = node;
                                }
                                else
                                    jsons.Add(node.NodeTag, WriteSettingsJsonFile(node.Certificate, node.PublicServerUrl, node.ServerUrl, settingsPath, SetupMode.Secured, modifyLocalServer: false));
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Failed to update {settingsPath} with new configuration.", e);
                            }
                        }

                        // Need to return the jsons to the caller, so they can zip and send them to the studio

                        try
                        {
                            var ips = localNode?.Ips.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();
                            var x509Certificate2 = new X509Certificate2(certBytes);
                            AssertServerCanStartSecured(x509Certificate2, localNode?.ServerUrl, ips, settingsPath);

                            // Load the certificate in the local server, so we can generate client certs later
                            _serverStore.Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(localNode?.Certificate, "Setup", x509Certificate2, certBytes);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Failed to start server with the new configuration {settingsPath}.", e);
                        }

                    }
                    catch (Exception e)
                    {
                        progress.AddError("Stage3: Failed to save certificate from Let's Encrypt. " + e);
                        onProgress(progress);
                        return null;
                    }
                        
                    progress.Processed++;
                    progress.AddInfo("Stage3: Finished.");
                    progress.AddInfo("Finished setting up RavenDB with a Let's Encrypt certificate. Server will now restart with the new settings.");
                    onProgress(progress);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to complete dns challenge from Let's Encrypt.", e);
            }
            return progress;
        }

        public static string WriteSettingsJsonFile(string base64Cert, string publicUrl, string serverUrl, string settingsPath, SetupMode setupMode, bool modifyLocalServer)
        {
            var json = File.ReadAllText(settingsPath);
            dynamic jsonObj = JsonConvert.DeserializeObject(json);

            jsonObj["ServerUrl"] = serverUrl;
            jsonObj["PublicServerUrl"] = null;
            if (string.IsNullOrEmpty(publicUrl))
            {
                jsonObj["PublicServerUrl"] = publicUrl;                
            }
            jsonObj["Security.Certificate.Base64"] = base64Cert;
            jsonObj["Setup.Mode"] = setupMode.ToString(); //TODO: setup.mode vs setup: { "mode": .... }

            string output = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

            if (modifyLocalServer)
            {
                var tmpPath = settingsPath + ".tmp";
                using (var file = new FileStream(tmpPath, FileMode.Create))
                using (var writer = new StreamWriter(file))
                {
                    writer.Write(output);
                    writer.Flush();
                    file.Flush(true);
                }

                File.Replace(tmpPath, settingsPath, settingsPath + ".bak");
                if (PlatformDetails.RunningOnPosix)
                    Syscall.FsyncDirectoryFor(settingsPath);
            }

            return output;
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
                    if (addresses.Length == 0)
                        options.Listen(new IPEndPoint(IPAddress.Parse("0.0.0.0"), 8080), listenOptions => listenOptions.UseHttps(serverCertificate));

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
    }
}
