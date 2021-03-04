using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15807 : ReplicationTestBase
    {
        public RavenDB_15807(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ChangeCertificateTypeInPullReplication()
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertificates = GenerateAndSaveSelfSignedCertificate(createNew: true);
            SetupServerAuthentication(hubSettings, certificates: hubCertificates);

            var sinkCertificates = GenerateAndSaveSelfSignedCertificate(createNew: true);
            var sinkCerts = SetupServerAuthentication(sinkSettings, certificates: sinkCertificates);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });
            var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = sinkSettings, RegisterForDisposal = true });

            var ownCertificate = new X509Certificate2(sinkCertificates.ClientCertificate1Path, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            Assert.True(ownCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkCerts.ServerCertificate.Value,
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
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    Certificates = new Dictionary<string, string>
#pragma warning restore CS0618 // Type or member is obsolete
                    {
                        [sinkCerts.ServerCertificate.Value.Thumbprint] = Convert.ToBase64String(ownCertificate.Export(X509ContentType.Cert))
                    }
                }));
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = $"ConnectionString-{hubStore.Database}",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));

                long taskId;
                // sink with ownCertificate - unauthorized to access
                var json = "{\"PullReplicationAsSink\":{\"TaskId\": null,\"Database\":\"" + hubStore.Database + "\",\"ConnectionStringName\": \"ConnectionString-" + hubStore.Database + "\",\"HubName\": \"" + pullReplicationName + "\",\"Mode\": \"HubToSink\",\"AccessName\": null,\"CertificateWithPrivateKey\":\" " + Convert.ToBase64String(ownCertificate.Export(X509ContentType.Pfx)) + "\",  \"AllowedHubToSinkPaths\": null ,\"AllowedSinkToHubPaths\": null }}";

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    BlittableJsonReaderObject reader = ctx.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");

                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var task = sinkServer.ServerStore.UpdatePullReplicationAsSink(sinkStore.Database, reader, Guid.NewGuid().ToString(), out PullReplicationAsSink pullReplication);
                        Task.WaitAll(task);
                        taskId = task.Result.Index;
                        Assert.NotNull(pullReplication.CertificateWithPrivateKey);
                    }
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

                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var task = sinkServer.ServerStore.UpdatePullReplicationAsSink(sinkStore.Database, reader, Guid.NewGuid().ToString(), out PullReplicationAsSink pullReplication);
                        Task.WaitAll(task);
                        Assert.Null(pullReplication.CertificateWithPrivateKey);
                    }
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
