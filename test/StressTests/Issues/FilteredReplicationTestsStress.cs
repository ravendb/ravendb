using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class FilteredReplicationTestsStress : ReplicationTestBase
    {
        public FilteredReplicationTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        private class Propagation
        {
#pragma warning disable 649
            public bool FromHub;
            public bool FromSink1;
            public bool FromSink2;
            public bool Completed;
#pragma warning restore 649
            public string Source;
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector_with_conflicts()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink1"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink2"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Hub"
                }, "common");
                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1, hubStore);

            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2, hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2, sinkStore1);

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Source == "Hub"));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink2GlobalCv.ToChangeVector().Length);
            }

            await EnsureNoReplicationLoop(Server, hubStore.Database);
            await EnsureNoReplicationLoop(Server, sinkStore1.Database);
            await EnsureNoReplicationLoop(Server, sinkStore2.Database);

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector_with_conflicts2()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink1"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink2"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Hub"
                }, "common");
                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1, hubStore);

            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2, hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2, sinkStore1);

            await WaitForConflict(hubStore, "common");
            await WaitForConflict(sinkStore1, "common");
            await WaitForConflict(sinkStore2, "common");

            await UpdateConflictResolver(hubStore, resolveToLatest: true);

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Source == "Hub"));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink2GlobalCv.ToChangeVector().Length);
            }

            await EnsureNoReplicationLoop(Server, hubStore.Database);
            await EnsureNoReplicationLoop(Server, sinkStore1.Database);
            await EnsureNoReplicationLoop(Server, sinkStore2.Database);

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }
    }
}
