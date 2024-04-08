using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class PullReplicationWithAuthenticationTest : ReplicationTestBase
    {
        public PullReplicationWithAuthenticationTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DeleteReplicationAccess(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCert = certificates.ClientCertificate2.Value;
            await store.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "Yard Replication Hub",
                WithFiltering = true
            }));
            await store.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("Yard Replication Hub",
                new ReplicationHubAccess
                {
                    Name = "Test",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "*" }
                }));

            await store.Maintenance.SendAsync(new UnregisterReplicationHubAccessOperation("Yard Replication Hub", pullCert.Thumbprint));
        }
    }
}
