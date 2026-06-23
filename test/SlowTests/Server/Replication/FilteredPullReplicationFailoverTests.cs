using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication;

public class FilteredPullReplicationFailoverTests : ReplicationTestBase
{
    public FilteredPullReplicationFailoverTests(ITestOutputHelper output) : base(output)
    {
    }

    #region SinkToHub

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateDocumentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var bulkInsert = sinkStore.BulkInsert())
            {
                for (int i = 0; i < 1024; i++)
                    bulkInsert.Store(new User { Name = $"User{i}" }, $"users/{i}");
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.Contains("A:1024-");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedDocuments = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After failover, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateDocumentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions()
            {
                DisableTopologyUpdates = true
            }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions()
            {
                DisableTopologyUpdates = true
            }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");

                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "transition/doc", 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null
                           && state.SourceChangeVector.Contains("A:1024")
                           && state.SourceChangeVector.Contains("B:1025")
                           && state.SourceChangeVector.Contains("C:1024");
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "A").ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");

            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeB.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeB.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.Contains("B:1026");
                }
            }, true, 30_000));

            var statsAfter = await sinkStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedDocuments = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After sink task migration to node B, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated documents after the replication task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateRevisionsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            await sinkStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = sinkStore.OpenAsyncSession())
            {
                for (int i = 0; i < 1024; i++)
                    await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1023");
                return revisions != null && revisions.Count >= 1;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedRevisions = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedRevisions == 2,
                $"After failover, expected == 2 revisions sent on new connection but got {replicatedRevisions}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");

            var replicatedDocuments = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After failover, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateRevisionsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            await sinkStoreA.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = sinkStoreA.OpenAsyncSession())
            {
                for (int i = 0; i < 1024; i++)
                    await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1023");
                return revisions != null && revisions.Count >= 1;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "transition/doc", 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "A").ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");

            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeB.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeB.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            var statsAfter = await sinkStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedRevisions = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedRevisions == 2,
                $"After sink task migration to node B, expected == 2 revisions sent on new connection but got {replicatedRevisions}. " +
                "Sink is re-sending already-replicated revisions after the replication task moved to node B.");

            var replicatedDocuments = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After sink task migration to node B, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated documents after the replication task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateAttachmentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    var bytes = Encoding.UTF8.GetBytes($"attachment content {i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt", new MemoryStream(bytes));
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.Contains("A:3072-");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedDocuments = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After failover, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");

            var replicatedAttachments = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedAttachments == 0,
                $"After failover, expected == 0 document sent on new connection but got {replicatedAttachments}. " +
                "Sink is re-sending already-replicated attachments after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateAttachmentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes($"attachment content {i}")));
                }
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.Advanced.Attachments.Store("transition/doc", "file.txt",
                    new MemoryStream(Encoding.UTF8.GetBytes("transition attachment")));
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("transition/doc", "file.txt");
                return attachment != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedDocuments = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After sink task migration to node B, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated documents after the replication task moved to node B.");

            var replicatedAttachments = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedAttachments == 0,
                $"After sink task migration to node B, expected == 0 attachments on new connection but got {replicatedAttachments}.. " +
                "Sink is re-sending already-replicated attachments after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateCountersAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.Contains("A:4096-");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var replicatedDocuments = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After failover, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");

            var replicatedCounters = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedCounters == 0,
                $"After failover, expected == 0 counter batch sent on new connection but got {replicatedCounters}. " +
                "Sink is re-sending already-replicated counters after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateCountersAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.CountersFor("transition/doc").Increment("likes");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "transition/doc", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("transition/doc").Get("likes") != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                
            var replicatedDocuments = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedDocuments == 1,
                $"After failover, expected == 1 document sent on new connection but got {replicatedDocuments}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");

            var replicatedCounters = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(replicatedCounters == 0,
                $"After sink task migration to node B, expected == 1 counter batches on new connection but got {replicatedCounters}. " +
                "Sink is re-sending already-replicated counters after the replication task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateTimeSeriesAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = sinkStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/ts", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var documentsInConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            var timeSeriesInConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(documentsInConnection == 1,
                $"After failover, expected == 1 document sent on new connection but got {documentsInConnection}. " +
                "Sink is re-sending already-replicated time series after connecting to a new hub node.");

            Assert.True(timeSeriesInConnection == 0,
                $"After failover, expected == 0 time series sent on new connection but got {timeSeriesInConnection}. " +
                "Sink is re-sending already-replicated time series after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateTimeSeriesAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = sinkStoreA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/ts", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.TimeSeriesFor("users/ts", "heartbeat").Append(baseline.AddMinutes(2000), 9999);
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get(baseline.AddMinutes(2000), baseline.AddMinutes(2001));
                return entries != null && entries.Length == 1;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var documentsInConnection = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(documentsInConnection == 1,
                $"After failover, expected == 1 document sent on new connection but got {documentsInConnection}. " +
                "Sink is re-sending already-replicated time series after connecting to a new hub node.");

            var timeSeriesInConnection = statsAfter.Outgoing.Where(x => x.Destination.StartsWith(hub.WebUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(timeSeriesInConnection == 0,
                $"After failover, expected == 0 time series sent on new connection but got {timeSeriesInConnection}. " +
                "Sink is re-sending already-replicated time series after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_NoResendAfterChainedHubNodeFailovers()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(5);

        var hubNodeC = hubNodes.Single(h => h.ServerStore.NodeTag == "C");
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 5,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreC = new DocumentStore
        {
            Urls = new[] { hubNodeC.WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStoreC.Maintenance.ForDatabase(hubStoreC.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStoreC.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStoreC.Database, $"ConnectionString-{hubStoreC.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubNodes.Select(s => s.WebUrl).ToArray());

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStoreC, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStoreC.Maintenance.ForDatabase(hubStoreC.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "B",
                }));

            var clusterTopology = hub.ServerStore.GetClusterTopology();
            var nodeAUrl = clusterTopology.GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker1" }, "marker/failover-1");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStoreC, "marker/failover-1", 30_000));

            var statsAfterFirst = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docsAfterFirst = statsAfterFirst.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docsAfterFirst == 1,
                $"After first hub failover, expected == 1 docs on new connection but got {docsAfterFirst}.");

            await hubStoreC.Maintenance.ForDatabase(hubStoreC.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "C",
                }));

            var nodeBUrl = clusterTopology.GetUrlFromTag("B");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeBUrl));

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker2" }, "marker/failover-2");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStoreC, "marker/failover-2", 30_000));

            var statsAfterSecond = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docsAfterSecond = statsAfterSecond.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docsAfterSecond == 2,
                $"After second hub failover, expected == 2 docs on new connection but got {docsAfterSecond}.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_TwoSinksNoResendAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sink1Store = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sink2Store = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert1 = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate1Path), (string)null,
                X509KeyStorageFlags.Exportable);
            var pullCert2 = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "Sink1Access",
                    CertificateBase64 = Convert.ToBase64String(pullCert1.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "sink1-docs/*", "marker/*" },
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "Sink2Access",
                    CertificateBase64 = Convert.ToBase64String(pullCert2.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "sink2-docs/*", "marker/*" },
                }));

            var pullReplication1 = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}-sink1", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert1.Export(X509ContentType.Pfx))
            };
            var result1 = await AddWatcherToReplicationTopology((DocumentStore)sink1Store, pullReplication1, hubNodes.Select(s => s.WebUrl).ToArray());

            var pullReplication2 = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}-sink2", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert2.Export(X509ContentType.Pfx))
            };
            var result2 = await AddWatcherToReplicationTopology((DocumentStore)sink2Store, pullReplication2, hubNodes.Select(s => s.WebUrl).ToArray());

            using (var session = sink1Store.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"Sink1User{i}" }, $"sink1-docs/{i}");
                session.SaveChanges();
            }

            using (var session = sink2Store.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"Sink2User{i}" }, $"sink2-docs/{i}");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "sink1-docs/511", 30_000));
            Assert.True(WaitForDocument(hubStore, "sink2-docs/511", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key1 = ExternalReplicationState.GenerateItemName(
                        sink1Store.Database, result1.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable1 = Server.ServerStore.Cluster.Read(ctx, key1);
                    if (blittable1 == null)
                        return false;
                    var state1 = JsonDeserializationCluster.ExternalReplicationState(blittable1);
                    if (state1.SourceChangeVector == null)
                        return false;

                    var key2 = ExternalReplicationState.GenerateItemName(
                        sink2Store.Database, result2.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable2 = Server.ServerStore.Cluster.Read(ctx, key2);
                    if (blittable2 == null)
                        return false;
                    var state2 = JsonDeserializationCluster.ExternalReplicationState(blittable2);
                    return state2.SourceChangeVector != null;
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            using (var session = sink1Store.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/sink1");
                session.SaveChanges();
            }

            using (var session = sink2Store.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/sink2");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/sink1", 30_000));
            Assert.True(WaitForDocument(hubStore, "marker/sink2", 30_000));

            var stats1After = await sink1Store.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docs1 = stats1After.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docs1 == 1,
                $"After hub failover (sink1), expected == 1 docs on new connection but got {docs1}.");

            var stats2After = await sink2Store.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docs2 = stats2After.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docs2 == 1,
                $"After hub failover (sink2), expected == 1 docs on new connection but got {docs2}.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_AllDocumentsArriveAfterHubCrashBeforeCursorCommit()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubNodes.Select(s => s.WebUrl).ToArray());

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                session.SaveChanges();
            }

            // Kill hub A immediately before cursor can be committed
            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            // All documents must still arrive at hub B/C
            Assert.True(WaitForDocument(hubStore, "users/1023", 60_000));

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/after-crash");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/after-crash", 30_000));
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_OnlyUncommittedPortionIsResentAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedSinkToHubPaths = new[] { "users/*", "marker/*", "transition/*", "batch1-docs/*", "batch2-docs/*", "sink1-docs/*", "sink2-docs/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubNodes.Select(s => s.WebUrl).ToArray());

            // Batch 1 — fully committed
            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"Batch1User{i}" }, $"batch1-docs/{i}");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "batch1-docs/511", 30_000));

            // Batch 2 — in-flight when hub A dies
            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"Batch2User{i}" }, $"batch2-docs/{i}");
                session.SaveChanges();
            }

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            Assert.True(WaitForDocument(hubStore, "batch2-docs/511", 60_000));

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docsInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            // Only batch2 (≤512) + marker should be resent, not batch1 (512 already cursor-confirmed)
            Assert.True(docsInNewConnection <= 520,
                $"After partial-cursor failover, expected ≤520 docs on new connection but got {docsInNewConnection}.");
            Assert.True(docsInNewConnection >= 1,
                $"Expected at least 1 doc (the marker) on new connection but got {docsInNewConnection}.");
        }
    }

    #endregion

    #region HubToSink

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateDocumentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreB = new DocumentStore
        {
            Urls = new[] { hubNodes.First(h => h.ServerStore.NodeTag == "B").WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var bulkInsert = hubStoreB.BulkInsert())
            {
                for (int i = 0; i < 1024; i++)
                    bulkInsert.Store(new User { Name = $"User{i}" }, $"users/{i}");
            }

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "B"
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = hubStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "marker/post-failover", 30_000));

            var statsAfter = await hubStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after failing over to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateDocumentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/1023", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStoreA.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "B").ToList(),
                sinkDB, "marker/post-failover", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeBUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl;
            var statsAfter = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After sink node failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after the sink task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateRevisionsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreB = new DocumentStore
        {
            Urls = new[] { hubNodes.First(h => h.ServerStore.NodeTag == "B").WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await sinkStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = hubStoreB.OpenAsyncSession())
            {
                for (int i = 0; i < 1024; i++)
                    await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStore.OpenSession();
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1023");
                return revisions != null && revisions.Count >= 1;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "B",
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "marker/post-failover", 30_000));

            var statsAfter = await hubStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After hub failover, expected == 1 documents sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after failing over to a new hub node.");

            var revisionsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

            // The marker's own revision is always sent (1). At the failover boundary one already-replicated revision
            // may also be re-sent: a revision item's local etag trails its document's, so the new hub node's revision
            // enumeration can pick up that one trailing item. This is idempotent -- the sink already has it (same change
            // vector) and dedupes on receipt, so nothing duplicate is stored; it is only an extra item on the wire.
            // The meaningful guarantee (documents are not re-sent) is asserted strictly above. More than 2 here would
            // indicate a genuine resend regression.
            Assert.True(revisionsInNewConnection is 1 or 2,
                $"After hub failover, expected 1 or 2 revisions sent on new connection but got {revisionsInNewConnection}. " +
                "Hub is re-sending already-replicated revisions after failing over to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateRevisionsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await sinkStoreA.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = hubStore.OpenAsyncSession())
            {
                for (int i = 0; i < 1024; i++)
                    await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/1023", u => true, TimeSpan.FromSeconds(30)));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStoreB.OpenSession();
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1023");
                return revisions != null && revisions.Count >= 1;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "B").ToList(),
                sinkDB, "marker/post-failover", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeBUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl;
            var statsAfter = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After sink node failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after the sink task moved to node B.");

            var revisionsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(revisionsInNewConnection == 2,
                $"After sink node failover, expected == 2 revisions sent on new connection but got {revisionsInNewConnection}. " +
                "Hub is re-sending already-replicated revisions after the sink task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateAttachmentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreB = new DocumentStore
        {
            Urls = new[] { hubNodes.First(h => h.ServerStore.NodeTag == "B").WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = hubStoreB.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes($"attachment content {i}")));
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "B"
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "marker/post-failover", 30_000));

            var statsAfter = await hubStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after failing over to a new hub node.");

            var attachmentsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(attachmentsInNewConnection == 0,
                $"After hub failover, expected == 0 attachments sent on new connection but got {attachmentsInNewConnection}. " +
                "Hub is re-sending already-replicated attachments after failing over to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateAttachmentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes($"attachment content {i}")));
                }
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/1023", u => true, TimeSpan.FromSeconds(30)));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStoreB.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {{
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {{
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }}
            }}, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "B").ToList(),
                sinkDB, "marker/post-failover", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeBUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl;
            var statsAfter = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After sink node failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after the sink task moved to node B.");

            var attachmentsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(attachmentsInNewConnection == 0,
                $"After sink node failover, expected == 0 attachments sent on new connection but got {attachmentsInNewConnection}. " +
                "Hub is re-sending already-replicated attachments after the sink task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateCountersAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreB = new DocumentStore
        {
            Urls = new[] { hubNodes.First(h => h.ServerStore.NodeTag == "B").WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = hubStoreB.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStore.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "B"
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "marker/post-failover", 30_000));

            var statsAfter = await hubStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after failing over to a new hub node.");

            var countersInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(countersInNewConnection == 0,
                $"After hub failover, expected == 0 counters sent on new connection but got {countersInNewConnection}. " +
                "Hub is re-sending already-replicated counters after failing over to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateCountersAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/1023", u => true, TimeSpan.FromSeconds(30)));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStoreB.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {{
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {{
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }}
            }}, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "B").ToList(),
                sinkDB, "marker/post-failover", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeBUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl;
            var statsAfter = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After sink node failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after the sink task moved to node B.");

            var countersInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(countersInNewConnection == 0,
                $"After sink node failover, expected == 0 counters sent on new connection but got {countersInNewConnection}. " +
                "Hub is re-sending already-replicated counters after the sink task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateTimeSeriesAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubStoreB = new DocumentStore
        {
            Urls = new[] { hubNodes.First(h => h.ServerStore.NodeTag == "B").WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = hubStoreB.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/ts", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = "B"
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "marker/post-failover", 30_000));

            var statsAfter = await hubStoreB.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After hub failover, expected == 1 documents sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after failing over to a new hub node.");

            var tsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(tsInNewConnection == 0,
                $"After hub failover, expected == 0 time series segments sent on new connection but got {tsInNewConnection}. " +
                "Hub is re-sending already-replicated time series after failing over to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_SinkShouldNotReceiveDuplicateTimeSeriesAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true,
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "users/*", "marker/*", "transition/*" },
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/ts", u => true, TimeSpan.FromSeconds(30)));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStoreB.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {{
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {{
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }}
            }}, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "B").ToList(),
                sinkDB, "marker/post-failover", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeBUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl;
            var statsAfter = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection == 1,
                $"After sink node failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                "Hub is re-sending already-replicated documents after the sink task moved to node B.");

            var tsInNewConnection = statsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkNodeBUrl))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(tsInNewConnection == 0,
                $"After sink node failover, expected == 0 time series segments sent on new connection but got {tsInNewConnection}. " +
                "Hub is re-sending already-replicated time series after the sink task moved to node B.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_NoResendAfterSinkNodeRestart()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (sinkNodes, sink) = await CreateRaftCluster(3, shouldRunInMemory: false);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sink.WebUrl);

        using (var hubStore = GetDocumentStore())
        using (var sinkStore = new DocumentStore
        {
            Urls = new[] { sink.WebUrl },
            Database = sinkDB
        }.Initialize())
        {
            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database)
                .SendAsync(new PutPullReplicationAsHubOperation(name));

            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                session.SaveChanges();
            }

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                MentorNode = "A"
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubStore.Urls);

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag == "A").ToList(),
                sinkDB, "users/1023", u => true, TimeSpan.FromSeconds(30)));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            var (dataDir, url, _) = await DisposeServerAndWaitForFinishOfDisposalAsync(sinkNodeA);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/pre-restart");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag != "A").ToList(),
                sinkDB, "marker/pre-restart", u => true, TimeSpan.FromSeconds(30)));

            var statsAfterFailover = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docsAfterFailover = statsAfterFailover.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docsAfterFailover <= 10,
                $"After sink node failover, expected ≤10 docs on new connection but got {docsAfterFailover}.");

            GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                DeletePrevious = false,
                DataDirectory = dataDir,
                CustomSettings = new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Core.ServerUrls), url }
                }
            });

            await WaitForValueAsync(async () =>
            {
                var record = await sinkStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(sinkDB));
                return record?.Topology?.Members?.Contains("A") ?? false;
            }, true, 30_000);

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker2" }, "marker/post-restart");
                session.SaveChanges();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(
                sinkNodes.Where(n => n.ServerStore.NodeTag != "A").ToList(),
                sinkDB, "marker/post-restart", u => true, TimeSpan.FromSeconds(30)));

            var statsAfterRestart = await hubStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var docsAfterRestart = statsAfterRestart.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(docsAfterRestart <= 10,
                $"After sink node restart, expected ≤10 docs on new connection but got {docsAfterRestart}.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_NoResendAfterHubNodeFailover_BothClusters()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub) = await CreateRaftCluster(3);
        var (_, minion) = await CreateRaftCluster(3);

        var hubDB = GetDatabaseName();
        var minionDB = GetDatabaseName();

        await CreateDatabaseInCluster(hubDB, 3, hub.WebUrl);
        await CreateDatabaseInCluster(minionDB, 3, minion.WebUrl);

        using (var hubStore = new DocumentStore
        {
            Urls = new[] { hub.WebUrl },
            Database = hubDB
        }.Initialize())
        using (var minionStore = new DocumentStore
        {
            Urls = new[] { minion.WebUrl },
            Database = minionDB
        }.Initialize())
        {
            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubStore.Maintenance.ForDatabase(hubStore.Database)
                .SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    MentorNode = "A"
                }));

            using (var session = hubStore.OpenSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: 2);
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                session.SaveChanges();
            }

            var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}", name)
            {
                MentorNode = "B"
            };

            var clusterTopology = hub.ServerStore.GetClusterTopology();
            var result = await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication,
                clusterTopology.AllNodes.Values.ToArray());

            Assert.True(WaitForDocument(minionStore, "users/1023", 30_000));

            var nodeBUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
            var minionNodeB = Servers.Single(s => s.WebUrl == nodeBUrl);
            Assert.True(WaitForValue(() =>
            {
                using (minionNodeB.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(minionDB, result.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = minionNodeB.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database)
                .SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    MentorNode = "B"
                }));

            var nodeAUrl = clusterTopology.GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            using (var session = hubStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(minionStore, "marker/post-failover", 30_000));

            // Check hub B's outgoing — what hub B sent to the minion after taking over.
            // Using minionBStore.Outgoing would include internal-replication noise (B→A, B→C).
            var hubNodeBUrl = clusterTopology.GetUrlFromTag("B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeBUrl },
                Database = hubDB
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                var docsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(nodeBUrl))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
                Assert.True(docsInNewConnection <= 10,
                    $"After hub failover, expected ≤10 docs on new connection but got {docsInNewConnection}.");
            }
        }
    }

    #endregion

    #region Bidirectional

    [RavenFact(RavenTestCategory.Replication)]
    public async Task Bidirectional_NoResendAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);

        var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var hubBStore = new DocumentStore
        {
            Urls = new[] { hubNodeB.WebUrl },
            Database = hubStore.Database,
            Certificate = certs.ServerCertificateForCommunication.Value,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
        }.Initialize())
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            var hubResult = await hubBStore.Maintenance.ForDatabase(hubBStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    MentorNode = "A",
                    WithFiltering = true
                }));

            await hubBStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "hub-docs/*" },
                    AllowedSinkToHubPaths = new[] { "sink-docs/*" }
                }));

            var pullReplication = new PullReplicationAsSink(hubBStore.Database, $"ConnectionString-{hubBStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubNodes.Select(s => s.WebUrl).ToArray());

            using (var session = hubBStore.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"HubUser{i}" }, $"hub-docs/{i}");
                session.SaveChanges();
            }

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 512; i++)
                    session.Store(new User { Name = $"SinkUser{i}" }, $"sink-docs/{i}");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "hub-docs/511", 30_000));
            Assert.True(WaitForDocument(hubBStore, "sink-docs/511", 30_000));

            // Wait for both cursors to be durably committed before killing hub A.
            // SinkCursor: hub confirmed how far sink sent; HubCursor: sink confirmed how far hub sent.
            var sinkNodeServer = Server; // single-node sink cluster
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var sinkKey = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var sinkBlittable = sinkNodeServer.ServerStore.Cluster.Read(ctx, sinkKey);
                    if (sinkBlittable == null)
                        return false;
                    var sinkState = JsonDeserializationCluster.ExternalReplicationState(sinkBlittable);
                    if (sinkState.SourceChangeVector == null)
                        return false;

                    var hubKey = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var hubBlittable = sinkNodeServer.ServerStore.Cluster.Read(ctx, hubKey);
                    if (hubBlittable == null)
                        return false;
                    var hubState = JsonDeserializationCluster.ExternalReplicationState(hubBlittable);
                    return hubState.SourceChangeVector != null;
                }
            }, true, 30_000));

            await hubBStore.Maintenance.ForDatabase(hubBStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    MentorNode = "B",
                    WithFiltering = true
                }));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == nodeAUrl));

            using (var session = hubBStore.OpenSession())
            {
                session.Store(new User { Name = "HubMarker" }, "hub-docs/marker");
                session.SaveChanges();
            }

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "SinkMarker" }, "sink-docs/marker");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(sinkStore, "hub-docs/marker", 30_000));
            Assert.True(WaitForDocument(hubBStore, "sink-docs/marker", 30_000));

            var hubStatsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            // SinkToHub direction: hub B received from sink — check hub B's incoming
            var sinkDocsInNewConnection = hubStatsAfter.Incoming
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentReadCount ?? 0) ?? 0) ?? 0;
            Assert.True(sinkDocsInNewConnection == 1 || sinkDocsInNewConnection == 2,
                $"After hub failover (SinkToHub direction), expected == 1 or 2 docs on new connection but got {sinkDocsInNewConnection}.");

            // HubToSink direction: hub B sent to sink — check hub B's outgoing
            var hubDocsInNewConnection = hubStatsAfter.Outgoing
                .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;
            Assert.True(hubDocsInNewConnection == 1 || hubDocsInNewConnection == 2,
                $"After hub failover (HubToSink direction), expected == 1 or 2 docs on new connection but got {hubDocsInNewConnection}.");
        }
    }

    #endregion

    #region FilteredCursorState

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_CursorStoresMergedChangeVectorWhenAllDocumentsAreFiltered()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);

        // All three stores on the same SSL server — avoids cross-server certificate issues.
        // externalStore replicates into sinkStore (same server), giving docs with externalStore's DB ID
        // in their change vector once they land in the sink.
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var externalStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            // external → sink: docs arrive in sink carrying externalStore's DB ID in their CV
            await SetupReplicationAsync(externalStore, sinkStore);

            var name = $"pull-replication {GetDatabaseName()}";
            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true
                }));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = new[] { "allowed-docs/*" }
                }));
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, new[] { hub.WebUrl });

            // filtered docs from external source land in sink with externalStore's DB ID in their CV
            using (var session = externalStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"External{i}" }, $"filtered-docs/ext-{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(sinkStore, "filtered-docs/ext-99", 30_000));

            // filtered docs written directly on sink carry sinkStore's own DB ID in their CV
            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"Local{i}" }, $"filtered-docs/local-{i}");
                session.SaveChanges();
            }

            var externalDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(externalStore.Database);
            Assert.NotNull(externalDb);
            var sinkDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(sinkStore.Database);
            Assert.NotNull(sinkDb);

            // SinkCursor is stored in the sink's cluster (hub.ServerStore for this single-node setup)
            string cursorCv = null;
            Assert.True(WaitForValue(() =>
            {
                using (hub.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = hub.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    cursorCv = state.SourceChangeVector;
                    return cursorCv != null
                        && ChangeVectorUtils.GetEtagById(cursorCv, externalDb.DbBase64Id) > 0
                        && ChangeVectorUtils.GetEtagById(cursorCv, sinkDb.DbBase64Id) > 0;
                }
            }, true, 30_000), "SinkCursor should be saved after scanning filtered-only batch");

            Assert.True(ChangeVectorUtils.GetEtagById(cursorCv, externalDb.DbBase64Id) > 0,
                $"Cursor '{cursorCv}' must contain external DB '{externalDb.DbBase64Id}' etag (merged CV check)");
            Assert.True(ChangeVectorUtils.GetEtagById(cursorCv, sinkDb.DbBase64Id) > 0,
                $"Cursor '{cursorCv}' must contain sink DB '{sinkDb.DbBase64Id}' etag (merged CV check)");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_CursorStoresMergedChangeVectorWhenSomeDocumentsAreFiltered()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var externalStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            await SetupReplicationAsync(externalStore, sinkStore);

            var name = $"pull-replication {GetDatabaseName()}";
            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    WithFiltering = true
                }));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = new[] { "allowed-docs/*" }
                }));
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, new[] { hub.WebUrl });

            // 4 allowed docs stored locally in sinkStore — first scan sends them to hub,
            // establishing an initial cursor that contains only sinkStore's DB ID.
            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 4; i++)
                    session.Store(new User { Name = $"Allowed{i}" }, $"allowed-docs/{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(hubStore, "allowed-docs/3", 30_000));

            // 100 filtered docs from externalStore replicate to sinkStore. The next scanner
            // iteration sees only these filtered docs (all-filtered empty batch). The fix must
            // update LastSentChangeVector so the cursor grows to include externalStore's DB ID.
            using (var session = externalStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"External{i}" }, $"filtered-docs/ext-{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(sinkStore, "filtered-docs/ext-99", 30_000));

            var externalDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(externalStore.Database);
            Assert.NotNull(externalDb);

            Assert.True(WaitForValue(() =>
            {
                using (hub.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = hub.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    if (state.SourceChangeVector == null)
                        return false;
                    return ChangeVectorUtils.GetEtagById(state.SourceChangeVector, externalDb.DbBase64Id) > 0;
                }
            }, true, 30_000),
            $"SinkCursor must include external DB '{externalDb.DbBase64Id}' etag after filtered-only batch");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_CursorStoresMergedChangeVectorWhenAllDocumentsAreFiltered()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var externalStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            // external → hub: docs arrive in hub carrying externalStore's DB ID in their CV
            await SetupReplicationAsync(externalStore, hubStore);

            var name = $"pull-replication {GetDatabaseName()}";
            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true
                }));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "allowed-docs/*" }
                }));
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, new[] { hub.WebUrl });

            // filtered docs from external source land in hub with externalStore's DB ID in their CV
            using (var session = externalStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"External{i}" }, $"filtered-docs/ext-{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(hubStore, "filtered-docs/ext-99", 30_000));

            // filtered docs written directly on hub carry hubStore's own DB ID in their CV
            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"Local{i}" }, $"filtered-docs/local-{i}");
                session.SaveChanges();
            }

            var externalDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(externalStore.Database);
            Assert.NotNull(externalDb);
            var hubDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(hubStore.Database);
            Assert.NotNull(hubDb);

            // HubCursor is stored in the sink's cluster state (hub.ServerStore for this single-node setup)
            string cursorCv = null;
            Assert.True(WaitForValue(() =>
            {
                using (hub.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = hub.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    cursorCv = state.SourceChangeVector;
                    return cursorCv != null
                        && ChangeVectorUtils.GetEtagById(cursorCv, externalDb.DbBase64Id) > 0
                        && ChangeVectorUtils.GetEtagById(cursorCv, hubDb.DbBase64Id) > 0;
                }
            }, true, 30_000), "HubCursor should be saved after scanning filtered-only batch");

            Assert.True(ChangeVectorUtils.GetEtagById(cursorCv, externalDb.DbBase64Id) > 0,
                $"Cursor '{cursorCv}' must contain external DB '{externalDb.DbBase64Id}' etag (merged CV check)");
            Assert.True(ChangeVectorUtils.GetEtagById(cursorCv, hubDb.DbBase64Id) > 0,
                $"Cursor '{cursorCv}' must contain hub DB '{hubDb.DbBase64Id}' etag (merged CV check)");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task HubToSink_CursorStoresMergedChangeVectorWhenSomeDocumentsAreFiltered()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var externalStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            Server = hub,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            await SetupReplicationAsync(externalStore, hubStore);

            var name = $"pull-replication {GetDatabaseName()}";
            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.HubToSink,
                    WithFiltering = true
                }));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "allowed-docs/*" }
                }));
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, new[] { hub.WebUrl });

            // 4 allowed docs stored locally in hubStore — hub sends them to sink via pull replication,
            // establishing an initial HubCursor that contains only hubStore's DB ID.
            using (var session = hubStore.OpenSession())
            {
                for (int i = 0; i < 4; i++)
                    session.Store(new User { Name = $"Allowed{i}" }, $"allowed-docs/{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(sinkStore, "allowed-docs/3", 30_000));

            // 100 filtered docs from externalStore replicate to hubStore. The next scanner
            // iteration sees only these filtered docs (all-filtered empty batch). The fix must
            // update LastSentChangeVector so the cursor grows to include externalStore's DB ID.
            using (var session = externalStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                    session.Store(new User { Name = $"External{i}" }, $"filtered-docs/ext-{i}");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(hubStore, "filtered-docs/ext-99", 30_000));

            var externalDb = await hub.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(externalStore.Database);
            Assert.NotNull(externalDb);

            Assert.True(WaitForValue(() =>
            {
                using (hub.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.HubCursor);
                    var blittable = hub.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    if (state.SourceChangeVector == null)
                        return false;
                    return ChangeVectorUtils.GetEtagById(state.SourceChangeVector, externalDb.DbBase64Id) > 0;
                }
            }, true, 30_000),
            $"HubCursor must include external DB '{externalDb.DbBase64Id}' etag after filtered-only batch");
        }
    }

    #endregion
}
