// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public partial class AuthenticationLetsEncryptTests : ClusterTestBase
    {
        [SetupWizardFact]
        public async Task CanGetLetsEncryptCertificateAndRenewIt()
        {
            var acmeStaging = "https://acme-staging.api.letsencrypt.org/directory";
            Server.Configuration.Core.AcmeUrl = acmeStaging;
            Server.ServerStore.Configuration.Core.SetupMode = SetupMode.Initial;

            var domain = "RavenCertTest"; //change domain before PR so that first claim will be by the scratch machine license
            string email;
            string rootDomain;

            Server.ServerStore.EnsureNotPassive();
            var license = Server.ServerStore.LoadLicense();
            
            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new ClaimDomainCommand(store.Conventions, context, new ClaimDomainInfo
                {
                    Domain = domain,
                    License = license
                });

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.RootDomains.Length > 0);
                rootDomain = command.Result.RootDomains[0];
                email = command.Result.Email;
            }

            var setupInfo = new SetupInfo
            {
                Domain = domain,
                RootDomain = rootDomain,
                ModifyLocalServer = false, // N/A here
                RegisterClientCert = false, // N/A here
                Password = null,
                Certificate = null,
                License = license,
                Email = email,
                NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
                {
                    ["A"] = new SetupInfo.NodeInfo
                    {
                        Port = 8080,
                        Addresses = new List<string>
                        {
                            "127.0.0.1"
                        }
                    }
                }
            };

            X509Certificate2 serverCert;
            string firstServerCertThumbprint;
            BlittableJsonReaderObject settingsJsonObject;

            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new SetupLetsEncryptCommand(store.Conventions, context, setupInfo);

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.Length > 0);

                var zipBytes = command.Result;

                try
                {
                    settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A", context, out serverCert, out _, out _, out _);
                    firstServerCertThumbprint = serverCert.Thumbprint;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                }
            }

            // Finished the setup wizard, need to restart the server. (TODO add restart server option to tests infrastructure)
            // Since cannot restart we'll create a new server loaded with the new certificate and settings and use the server cert to connect to it

            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

            var tempFileName = Path.GetTempFileName();
            byte[] certData = serverCert.Export(X509ContentType.Pfx);
            File.WriteAllBytes(tempFileName, certData);

            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = publicServerUrl,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                [RavenConfiguration.GetKey(x => x.Core.AcmeUrl)] = acmeStaging
            };
            
            DoNotReuseServer(customSettings);
            UseNewLocalServer();
            Servers = new List<RavenServer>{_localServer};

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCert,
                ClientCertificate = serverCert
            }))
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                Server.ServerStore.EnsureNotPassive();
                Assert.Equal(firstServerCertThumbprint, Server.Certificate.Certificate.Thumbprint);

                Server.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(80);

                var mre = new ManualResetEventSlim();

                Server.ServerCertificateChanged += (sender, args) => mre.Set();

                var command = new ForceRenewCertCommand(store.Conventions, context);

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                mre.Wait(TimeSpan.FromMinutes(2));

                Assert.NotEqual(firstServerCertThumbprint, Server.Certificate.Certificate.Thumbprint);
            }
        }
    }
}
