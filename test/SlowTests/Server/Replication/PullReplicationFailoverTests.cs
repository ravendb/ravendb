using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication;

public class PullReplicationFailoverTests : ReplicationTestBase
{
    public PullReplicationFailoverTests(ITestOutputHelper output) : base(output)
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var bulkInsert = hubStore.BulkInsert())
            {
                for (int i = 0; i < 1024; i++)
                    bulkInsert.Store(new User { Name = $"User{i}" }, $"users/{i}");
            }

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
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

            var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeB.WebUrl },
                Database = hubStore.Database,
                Certificate = certs.ServerCertificateForCommunication.Value,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

                var docsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(docsInNewConnection == 1,
                    $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                    "Hub is re-sending already-replicated documents after failing over to a new hub node.");
            }
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
                    Mode = PullReplicationMode.HubToSink
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await sinkStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = hubStore.OpenAsyncSession())
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

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
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

            var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeB.WebUrl },
                Database = hubStore.Database,
                Certificate = certs.ServerCertificateForCommunication.Value,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

                var docsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(docsInNewConnection == 1,
                    $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                    "Hub is re-sending already-replicated documents after failing over to a new hub node.");

                var revisionsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(revisionsInNewConnection == 1,
                    $"After hub failover, expected == 1 revisions sent on new connection but got {revisionsInNewConnection}. " +
                    "Hub is re-sending already-replicated revisions after failing over to a new hub node.");
            }
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
                    Mode = PullReplicationMode.HubToSink
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

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

            Assert.True(WaitForDocument(sinkStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = sinkStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
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

            var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeB.WebUrl },
                Database = hubStore.Database,
                Certificate = certs.ServerCertificateForCommunication.Value,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

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
                    Mode = PullReplicationMode.HubToSink
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = hubStore.OpenSession())
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

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
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

            var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeB.WebUrl },
                Database = hubStore.Database,
                Certificate = certs.ServerCertificateForCommunication.Value,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

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
                    Mode = PullReplicationMode.HubToSink
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
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
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = hubStore.OpenAsyncSession())
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
    
            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    TaskId = hubResult.TaskId,
                    Mode = PullReplicationMode.HubToSink,
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

            var hubNodeB = hubNodes.Single(h => h.ServerStore.NodeTag == "B");
            using (var hubBStore = new DocumentStore
            {
                Urls = new[] { hubNodeB.WebUrl },
                Database = hubStore.Database,
                Certificate = certs.ServerCertificateForCommunication.Value,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize())
            {
                var statsAfter = await hubBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

                var docsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(docsInNewConnection == 1,
                    $"After hub failover, expected == 1 document sent on new connection but got {docsInNewConnection}. " +
                    "Hub is re-sending already-replicated documents after failing over to a new hub node.");

                var tsInNewConnection = statsAfter.Outgoing
                    .Where(x => x.Destination.StartsWith(sinkStore.Urls[0]))
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(tsInNewConnection == 0,
                    $"After hub failover, expected == 0 time series segments sent on new connection but got {tsInNewConnection}. " +
                    "Hub is re-sending already-replicated time series after failing over to a new hub node.");
            }
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
                    Mode = PullReplicationMode.HubToSink
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
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

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
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

    #endregion
}
