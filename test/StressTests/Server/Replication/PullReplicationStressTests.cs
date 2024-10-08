using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Replication
{
    public class PullReplicationStressTests : ReplicationTestBase
    {
        public PullReplicationStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PullExternalReplicationWithCertificateShouldWork()
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates);

            var sinkCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: false);
            var sinkCerts = Certificates.SetupServerAuthentication(sinkSettings, certificates: sinkCertificates);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: false);
            var pullReplicationCertificate = X509CertificateLoader.LoadPkcs12FromFile(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = hubCerts.ServerCertificate.Value,
                Server = hubServer,
                ModifyDatabaseName = _ => hubDB
            }))
            using (var sinkStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkCerts.ServerCertificate.Value,
                Server = sinkServer,
                ModifyDatabaseName = _ => sinkDB
            }))
            {
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullReplicationName, new ReplicationHubAccess
                {
                    Name = pullReplicationCertificate.Thumbprint,
                    CertificateBase64 = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert))
                }));

                var configurationResult = await SetupPullReplicationAsync(pullReplicationName, sinkStore, pullReplicationCertificate, hubStore);
                var sinkTaskId = configurationResult[0].TaskId;
                using (var hubSession = hubStore.OpenSession())
                {
                    hubSession.Store(new User(), "foo/bar");
                    hubSession.SaveChanges();
                }
                
                var timeout = 5000;
                Assert.True(WaitForDocument(sinkStore, "foo/bar", timeout), sinkStore.Identifier);
                
                // test if certificate is retained when we don't send one
                // sending null as cert - but it should copy old one
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    TaskId = sinkTaskId,
                    Name = pullReplicationName,
                    HubName = pullReplicationName,
                    ConnectionStringName = "ConnectionString-" + hubStore.Database
                }));
                
                using (var hubSession = hubStore.OpenSession())
                {
                    hubSession.Store(new User(), "foo/bar2");
                    hubSession.SaveChanges();
                }
                
                var sinkDBInstance = await sinkServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(sinkStore.Database);
                Assert.True(WaitForDocument(sinkStore, "foo/bar2", timeout),
                    $"incoming handlers on sink {sinkStore.Identifier}: {sinkDBInstance.ReplicationLoader.IncomingRejectionStats.FirstOrDefault().Value?.FirstOrDefault()?.Reason}");
            }
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, X509Certificate2 certificate, params DocumentStore[] hub)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in hub)
            {
                var pull = new PullReplicationAsSink(store.Database, $"ConnectionString-{store.Database}", remoteName);
                if (certificate != null)
                {
                    pull.CertificateWithPrivateKey = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));
                }
                tasks.Add(AddWatcherToReplicationTopology(sink, pull, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
        }
    }
}
