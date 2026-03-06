using System;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_15807 : ReplicationTestBase
    {
        public RavenDB_15807(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ChangeCertificateTypeInPullReplication(bool with2Eku)
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate();
            Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates, with2Eku: with2Eku);

            var sinkCertificates = Certificates.GenerateAndSaveSelfSignedCertificate();
            var sinkCerts = Certificates.SetupServerAuthentication(sinkSettings, certificates: sinkCertificates, with2Eku: with2Eku);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

#pragma warning disable SYSLIB0057
            var ownCertificate = new X509Certificate2(sinkCertificates.ClientCertificate1Path, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
            Assert.True(ownCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkCerts.ServerCertificateForCommunication.Value,
                Server = hubServer,
                ModifyDatabaseName = _ => hubDB,
                ModifyDocumentStore = s => s.Conventions.DisposeCertificate = false
            }))
            using (var sinkStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkCerts.ServerCertificateForCommunication.Value,
                Server = sinkServer,
                ModifyDatabaseName = _ => sinkDB,
                ModifyDocumentStore = s => s.Conventions.DisposeCertificate = false
            }))
            {
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullReplicationName, new ReplicationHubAccess
                {
                    Name = "foo",
                    CertificateBase64 = Convert.ToBase64String(ownCertificate.Export(X509ContentType.Cert))
                }));

                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = $"ConnectionString-{hubStore.Database}",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));

                long taskId;
                // sink with client cert 2 - unauthorized to access
                var json = "{\"PullReplicationAsSink\":{\"TaskId\": null,\"Database\":\"" + hubStore.Database + "\",\"ConnectionStringName\": \"ConnectionString-" + hubStore.Database + "\",\"HubName\": \"" + pullReplicationName + "\",\"Mode\": \"HubToSink\",\"AccessName\": null,\"CertificateWithPrivateKey\":\" " + Convert.ToBase64String(sinkCertificates.ClientCertificate2.Value.Export(X509ContentType.Pfx)) + "\",  \"AllowedHubToSinkPaths\": null ,\"AllowedSinkToHubPaths\": null }}";
                
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    BlittableJsonReaderObject reader = ctx.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");
                    
                    var result = await sinkServer.ServerStore.UpdatePullReplicationAsSink(sinkStore.Database, reader, Guid.NewGuid().ToString(), out PullReplicationAsSink pullReplication);
                    taskId = result.Index;
                    Assert.NotNull(pullReplication.CertificateWithPrivateKey);
                }

                using (var hubSession = hubStore.OpenSession())
                {
                    hubSession.Store(new User(), "foo/bar");
                    hubSession.SaveChanges();
                }

                Assert.False(WaitForDocument(sinkStore, "foo/bar", 3000));

                // sink with null certificate => use server certificate - authorized
                json = "{\"PullReplicationAsSink\":{\"TaskId\": \"" + taskId + "\",\"Database\":\"" + hubStore.Database + "\",\"ConnectionStringName\": \"ConnectionString-" + hubStore.Database + "\",\"HubName\": \"" + pullReplicationName + "\",\"Mode\": \"HubToSink\",\"AccessName\": null,\"CertificateWithPrivateKey\": null,  \"AllowedHubToSinkPaths\": null ,\"AllowedSinkToHubPaths\": null }}";

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    BlittableJsonReaderObject reader = ctx.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");
                    
                    await sinkServer.ServerStore.UpdatePullReplicationAsSink(sinkStore.Database, reader, Guid.NewGuid().ToString(), out PullReplicationAsSink pullReplication);
                    Assert.Null(pullReplication.CertificateWithPrivateKey);
                }

                using (var hubSession = hubStore.OpenSession())
                {
                    hubSession.Store(new User(), "foo/bar2");
                    hubSession.SaveChanges();
                }

                Assert.True(WaitForDocument(sinkStore, "foo/bar2", 3000));
            }
        }
    }
}
