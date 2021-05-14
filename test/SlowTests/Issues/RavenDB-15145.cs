using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Security;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15145 : ReplicationTestBase
    {
        public RavenDB_15145(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PullReplicationWithoutPrivateKey()
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertificates = GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = SetupServerAuthentication(hubSettings, certificates: hubCertificates);

            var sinkCertificates = GenerateAndSaveSelfSignedCertificate(createNew: true);
            var sinkCerts = SetupServerAuthentication(sinkSettings, certificates: sinkCertificates);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

            var dummy = GenerateAndSaveSelfSignedCertificate(createNew: true);
            var pullReplicationCertificate = new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
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
                var pull = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", pullReplicationName);
                pull.CertificateWithPrivateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert));

                await Assert.ThrowsAsync<AuthorizationException>(async () => await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pull)));
            }
        }
    }
}
