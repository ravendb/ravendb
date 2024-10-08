using System;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
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

            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates);

            var sinkCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var sinkCerts = Certificates.SetupServerAuthentication(sinkSettings, certificates: sinkCertificates);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
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
                var pull = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", pullReplicationName);
                pull.CertificateWithPrivateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert));

                await Assert.ThrowsAsync<AuthorizationException>(async () => await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pull)));
            }
        }
    }
}
