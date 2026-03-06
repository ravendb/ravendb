using System;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Issues
{
    public class RavenDB_15145 : ReplicationTestBase
    {
        public RavenDB_15145(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PullReplicationWithoutPrivateKey(bool with2Eku)
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates, with2Eku: with2Eku);

            var sinkCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var sinkCerts = Certificates.SetupServerAuthentication(sinkSettings, certificates: sinkCertificates, with2Eku: with2Eku);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
#pragma warning disable SYSLIB0057
            var pullReplicationCertificate = new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = hubCerts.ServerCertificateForCommunication.Value,
                Server = hubServer,
                ModifyDatabaseName = _ => hubDB
            }))
            using (var sinkStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkCerts.ServerCertificateForCommunication.Value,
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
