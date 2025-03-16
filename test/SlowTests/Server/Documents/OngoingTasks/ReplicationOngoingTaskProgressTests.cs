using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.OngoingTasks
{
    public class ReplicationOngoingTaskProgressTests : ReplicationTestBase
    {
        private const string UserId = "users/shiran";

        public ReplicationOngoingTaskProgressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task GetExternalReplicationTaskProgressShouldWork(Options options)
        {
            using var source = GetDocumentStore(options);
            using var destination = GetDocumentStore(options);

            // we want the first result to show unprocessed items
            // so, we define an external replication task and break it immediately

            var sourceDb = await GetDocumentDatabaseInstanceForAsync(source, options.DatabaseMode, UserId);

            await SetupReplicationAsync(source, destination);
            var replication = await BreakReplication(Server.ServerStore, sourceDb.Name);

            await StoreData(source);

            // since we broke replication, we expect incomplete results with items to process

            await VerifyReplicationProgressAsync(source, sourceDb, ReplicationNode.ReplicationType.External, isCompleted: false);

            // continue the replication and let the items replicate to the destination

            replication.Mend();
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(destination, UserId, TimeSpan.FromSeconds(10)));

            // now we should have values for the last sent Etag and change vectors, so we retrieve them to verify they are correct

            await VerifyReplicationProgressAsync(source, sourceDb, ReplicationNode.ReplicationType.External, isCompleted: true);

            // break the replication again to perform deletion and check tombstone items

            replication.Break();

            await DeleteUserDocument(source);

            await VerifyReplicationProgressAsync(source, sourceDb, ReplicationNode.ReplicationType.External, isCompleted: false, hasTombstones: true);

            // continue the replication and check if all tombstones are processed

            replication.Mend();

            Assert.True(WaitForDocumentDeletion(destination, UserId));

            await VerifyReplicationProgressAsync(source, sourceDb, ReplicationNode.ReplicationType.External, isCompleted: true, hasTombstones: true);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task GetPullReplicationAsHubTaskProgressShouldWork(Options options)
        {
            using var hub = GetDocumentStore(options);
            using var sink = GetDocumentStore(options);

            // we want the first result to show unprocessed items
            // so, we define pull replication task and break it immediately

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation("hub"));
            await PullReplicationTests.SetupPullReplicationAsync("hub", sink, hub);

            var replication = await BreakReplication(Server.ServerStore, hub.Database);

            await StoreData(hub);

            // since we broke replication, we expect incomplete results with items to process

            var hubDatabase = await GetDocumentDatabaseInstanceForAsync(hub.Database);

            await VerifyReplicationProgressAsync(hub, hubDatabase, ReplicationNode.ReplicationType.PullAsHub, isCompleted: false);

            // continue the replication and let the items replicate to the sink

            replication.Mend();
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(sink, UserId, TimeSpan.FromSeconds(10)));

            // now we should have values for the last sent Etag and change vectors, so we retrieve them to verify they are correct

            await VerifyReplicationProgressAsync(hub, hubDatabase, ReplicationNode.ReplicationType.PullAsHub, isCompleted: true);

            // break the replication again to perform deletion and check tombstone items

            replication.Break();

            await DeleteUserDocument(hub);

            await VerifyReplicationProgressAsync(hub, hubDatabase, ReplicationNode.ReplicationType.PullAsHub, isCompleted: false, hasTombstones: true);

            // continue the replication and check if all tombstones are processed

            replication.Mend();
            Assert.True(WaitForDocumentDeletion(sink, UserId));

            await VerifyReplicationProgressAsync(hub, hubDatabase, ReplicationNode.ReplicationType.PullAsHub, isCompleted: true, hasTombstones: true);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task GetPullReplicationAsHubTaskProgressShouldWork_TwoSinks(Options options)
        {
            using var hub = GetDocumentStore(new Options(options) { ModifyDatabaseName = _ => "HubDB" });
            using var sink1 = GetDocumentStore(new Options(options) { ModifyDatabaseName = _ => "Sink1DB" });
            using var sink2 = GetDocumentStore(new Options(options) { ModifyDatabaseName = _ => "Sink2DB" });

            // we want the first result to show unprocessed items
            // so, we define pull replication task and break it immediately

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation("hub"));
            await PullReplicationTests.SetupPullReplicationAsync("hub", sink1, hub);
            await PullReplicationTests.SetupPullReplicationAsync("hub", sink2, hub);

            var replication = await BreakReplication(Server.ServerStore, hub.Database);

            await StoreData(hub);

            // since we broke replication, we expect incomplete results with items to process

            var hubDatabase = await GetDatabase(hub.Database);

            await VerifyPullAsHubReplicationProgress(hub, hubDatabase, isCompleted: false);

            // continue the replication and let the items replicate to the sink

            replication.Mend();
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(sink1, UserId, TimeSpan.FromSeconds(10)));
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(sink2, UserId, TimeSpan.FromSeconds(10)));

            await VerifyPullAsHubReplicationProgress(hub, hubDatabase, isCompleted: true);

            // break the replication again to perform deletion and check tombstone items

            replication.Break();

            await DeleteUserDocument(hub);

            await VerifyPullAsHubReplicationProgress(hub, hubDatabase, isCompleted: false, hasTombstones: true);

            // continue the replication and check if all tombstones are processed

            replication.Mend();
            Assert.True(WaitForDocumentDeletion(sink1, UserId));
            Assert.True(WaitForDocumentDeletion(sink2, UserId));

            await VerifyPullAsHubReplicationProgress(hub, hubDatabase, isCompleted: true, hasTombstones: true);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task GetPullReplicationAsSinkTaskProgressShouldWork(Options options)
        {
            var (_, leader, certificates) = await CreateRaftClusterWithSsl(1);

            using var hub = GetDocumentStore(new Options(options)
            {
                Server = leader,
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => "HubDB",
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options(options)
            {
                Server = leader,
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => "SinkDB",
                CreateDatabase = true
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = ["*"],
                AllowedSinkToHubPaths = ["*"]
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hub.Database,
                Name = hub.Database + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));

            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = hub.Database + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both"
            }));

            await StoreData(sink);

            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(hub, UserId, TimeSpan.FromSeconds(10)));

            var sinkDatabase = await GetDatabase(leader, sink.Database);

            await VerifyReplicationProgressAsync(sink, sinkDatabase, ReplicationNode.ReplicationType.PullAsSink, isCompleted: true, server: leader);

            await DeleteUserDocument(sink);
            Assert.True(WaitForDocumentDeletion(hub, UserId));

            await VerifyReplicationProgressAsync(sink, sinkDatabase, ReplicationNode.ReplicationType.PullAsSink, isCompleted: true, hasTombstones: true, server: leader);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task GetInternalReplicationProgressShouldWork(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using var store = GetDocumentStore(new Options(options)
            {
                Server = leader,
                ReplicationFactor = 3,
                CreateDatabase = true
            });

            await StoreData(store);

            var db = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, UserId, leader);

            // validate that the document has replicated to all cluster nodes
            Assert.True(await WaitForDocumentInClusterAsync<User>(nodes, db.Name, UserId, u => u.Name == "shiran", timeout: TimeSpan.FromSeconds(10)));

            // validate replication progress on each node
            foreach (var server in nodes)
            {
                await VerifyInternalReplicationProgress(store, db, server, nodes);
            }

            await DeleteUserDocument(store);
            Assert.True(await WaitForDocumentDeletionInClusterAsync(nodes, db.Name, UserId, timeout: TimeSpan.FromSeconds(10)));

            foreach (var server in nodes)
            {
                await VerifyInternalReplicationProgress(store, db, server, nodes, hasTombstones: true);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldGetErrorForExternalReplicationTask(Options options)
        {
            using var source = GetDocumentStore(options);

            var sourceDb = await GetDocumentDatabaseInstanceForAsync(source, options.DatabaseMode, UserId);

            var databaseWatcher = new ExternalReplication("DestinationDB", $"ConnectionString-{source.Identifier}");
            var res = await AddWatcherToReplicationTopology(source, databaseWatcher, source.Urls);

            await StoreData(source);

            var op = new GetOngoingTaskInfoOperation(res.TaskId, OngoingTaskType.Replication);
            var result = (OngoingTaskReplication)source.Maintenance.ForDatabase(sourceDb.Name).Send(op);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Error);
            Assert.Contains("DatabaseDoesNotExistException", result.Error);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldGetErrorForPullReplicationAsSinkTask(Options options)
        {
            using var hub = GetDocumentStore(options);
            using var sink = GetDocumentStore(options);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation("hub"));
            var pull = new PullReplicationAsSink(hub.Database, $"ConnectionString-{hub.Database}", "hub") { Url = sink.Urls[0] };
            var res = await AddWatcherToReplicationTopology(sink, pull);

            await StoreData(hub);

            var hubDatabase = await GetDatabase(hub.Database);
            hubDatabase.Dispose();

            await AssertWaitForValueAsync(async () =>
            {
                var result = await sink.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(res.TaskId, OngoingTaskType.PullReplicationAsSink));
                var pullAsSinkResult = result as OngoingTaskPullReplicationAsSink;
                return pullAsSinkResult != null && string.IsNullOrEmpty(pullAsSinkResult.Error) == false;
            }, true);
        }

        private async Task<IReplicationTaskProgress[]> GetReplicationProgress(DocumentStore store, string databaseName = null, RavenServer server = null)
        {
            using var commands = store.Commands(databaseName);
            var nodeTag = server?.ServerStore.NodeTag ?? Server.ServerStore.NodeTag;
            var cmd = new GetReplicationOngoingTasksProgressCommand([], nodeTag);
            await commands.ExecuteAsync(cmd);
            return cmd.Result;
        }

        private async Task<IReplicationTaskProgress[]> GetInternalReplicationProgress(DocumentStore store, string databaseName = null, RavenServer server = null)
        {
            using var commands = store.Commands(databaseName);
            var nodeTag = server?.ServerStore.NodeTag ?? Server.ServerStore.NodeTag;
            var cmd = new GetOutgoingInternalReplicationProgressCommand(nodeTag);
            await commands.ExecuteAsync(cmd);
            return cmd.Result;
        }

        private async Task StoreData(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();

            var user = new User { Name = "shiran" };
            await session.StoreAsync(user, UserId);

            session.CountersFor(user).Increment("Likes");
            session.TimeSeriesFor(user, "HeartRate").Append(DateTime.Today, 94);

            using var ms = new MemoryStream(new byte[] { 1, 2, 3 });
            session.Advanced.Attachments.Store(user, "foo", ms);
            await session.SaveChangesAsync();
        }

        private async Task DeleteUserDocument(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();
            session.Delete(UserId);
            await session.SaveChangesAsync();
        }

        private (long lastSentEtag, string sourceChangeVector, string destinationChangeVector) GetReplicationHandlerState(DocumentDatabase db)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var handler = db.ReplicationLoader.OutgoingHandlers.Single();
                var lastSentEtag = handler.LastSentDocumentEtag;
                var destinationChangeVector = handler.LastAcceptedChangeVector;
                var sourceChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                return (lastSentEtag, sourceChangeVector, destinationChangeVector);
            }
        }

        private async Task VerifyReplicationProgressAsync(DocumentStore store, DocumentDatabase database, ReplicationNode.ReplicationType replicationType,
            bool isCompleted = false, bool hasTombstones = false, RavenServer server = null)
        {
            IReplicationTaskProgress[] results = null;
            await WaitForValueAsync(async () =>
            {
                results = await GetReplicationProgress(store, database.Name, server);
                if (results == null || results.Length != 1)
                    return false;

                return true;
            }, true);

            if (isCompleted)
            {
                if (replicationType != ReplicationNode.ReplicationType.PullAsSink)
                {
                    await AssertReplicationBatchCompletedAsync(store, database, server);
                }

                if (hasTombstones)
                    AssertTombstoneItemsProcessed(results);
                else
                    AssertItemsProcessed(results);

                return;
            }

            AssertReplicationStillInProgress(results, replicationType);

            if (hasTombstones)
                AssertPendingTombstoneItemsToProcess(results);
            else
                AssertPendingItemsToProcess(results);
        }

        private async Task VerifyPullAsHubReplicationProgress(DocumentStore store, DocumentDatabase database,
            bool isCompleted = false, bool hasTombstones = false, RavenServer server = null)
        {
            var results = await GetReplicationProgress(store, database.Name, server);

            var result = Assert.Single(results);
            var processesProgress = result.ProcessesProgress;
            Assert.Equal(2, processesProgress.Count); // we should have 2 results, one per each sink
            Assert.NotEqual(processesProgress[0].HandlerId, processesProgress[1].HandlerId);

            if (isCompleted)
            {
                if (hasTombstones)
                {
                    AssertTombstoneItemsProcessed(results, processesProgress[0]);
                    AssertTombstoneItemsProcessed(results, processesProgress[1]);
                }
                else
                {
                    AssertItemsProcessed(results, processesProgress[0]);
                    AssertItemsProcessed(results, processesProgress[1]);
                }

                return;
            }

            AssertReplicationStillInProgress(results, ReplicationNode.ReplicationType.PullAsHub);

            if (hasTombstones)
            {
                AssertPendingTombstoneItemsToProcess(results, processesProgress[0]);
                AssertPendingTombstoneItemsToProcess(results, processesProgress[1]);
            }
            else
            {
                AssertPendingItemsToProcess(results, processesProgress[0]);
                AssertPendingItemsToProcess(results, processesProgress[1]);
            }
        }

        private async Task VerifyInternalReplicationProgress(DocumentStore store, DocumentDatabase database, RavenServer server,
            List<RavenServer> nodes, bool hasTombstones = false)
        {
            var results = await GetInternalReplicationProgress(store, database.Name, server);
            Assert.Equal(2, results.Length);

            var expectedDestinations = nodes
                .Where(n => n.ServerStore.NodeTag != server.ServerStore.NodeTag)
                .Select(n => n.ServerStore.NodeTag)
                .ToList();

            foreach (var result in results)
            {
                Assert.Equal(ReplicationNode.ReplicationType.Internal, result.ReplicationType);

                var internalTaskProgress = Assert.IsType<InternalReplicationTaskProgress>(result);
                Assert.Contains(internalTaskProgress.DestinationNodeTag, expectedDestinations);
                var processProgress = Assert.Single(internalTaskProgress.ProcessesProgress);

                if (hasTombstones)
                    AssertTombstoneItemsProcessed(results, processProgress);
                else
                    AssertItemsProcessed(results, processProgress);
            }
        }

        private void AssertReplicationStillInProgress(IReplicationTaskProgress[] result, ReplicationNode.ReplicationType replicationType)
        {
            Assert.NotEmpty(result);
            Assert.Single(result);

            var progress = result[0];
            Assert.Equal(replicationType, progress.ReplicationType);
            Assert.False(progress.ProcessesProgress[0].Completed);
        }

        private async Task AssertReplicationBatchCompletedAsync(DocumentStore store, DocumentDatabase database, RavenServer server)
        {
            var (lastSentEtag, sourceChangeVector, destinationChangeVector) = GetReplicationHandlerState(database);

            IReplicationTaskProgress[] results = null;
            await WaitForValueAsync(async () =>
            {
                results = await GetReplicationProgress(store, database.Name, server);
                if (results == null || results.Length != 1)
                    return false;

                var result = results[0];
                if (result.ProcessesProgress.Count != 1)
                    return false;

                if (result.ProcessesProgress[0].LastSentEtag != lastSentEtag)
                    return false;

                return true;
            }, true);

            Assert.NotEmpty(results);
            var result = Assert.Single(results);
            var processProgress = Assert.Single(result.ProcessesProgress);

            Assert.Equal(lastSentEtag, processProgress.LastSentEtag);
            Assert.Equal(sourceChangeVector, processProgress.SourceChangeVector);
            Assert.Equal(destinationChangeVector, processProgress.DestinationChangeVector);
            Assert.True(processProgress.Completed);
        }

        private void AssertPendingItemsToProcess(IReplicationTaskProgress[] result, ReplicationProcessProgress processProgress = null,
            long documentsToProcess = 1, long countersToProcess = 1, long timeSeriesSegmentsToProcess = 1, long attachmentsToProcess = 1)
        {
            processProgress ??= result[0].ProcessesProgress[0];

            Assert.Equal(documentsToProcess, processProgress.NumberOfDocumentsToProcess);
            Assert.Equal(countersToProcess, processProgress.NumberOfCounterGroupsToProcess);
            Assert.Equal(timeSeriesSegmentsToProcess, processProgress.NumberOfTimeSeriesSegmentsToProcess);
            Assert.Equal(attachmentsToProcess, processProgress.NumberOfAttachmentsToProcess);
        }

        private void AssertPendingTombstoneItemsToProcess(IReplicationTaskProgress[] result, ReplicationProcessProgress processProgress = null,
            long documentTombstonesToProcess = 1, long timeSeriesDeletedRangesToProcess = 1)
        {
            processProgress ??= result[0].ProcessesProgress[0];

            Assert.Equal(documentTombstonesToProcess, processProgress.NumberOfDocumentTombstonesToProcess);
            Assert.Equal(timeSeriesDeletedRangesToProcess, processProgress.NumberOfTimeSeriesDeletedRangesToProcess);
        }

        private void AssertItemsProcessed(IReplicationTaskProgress[] result, ReplicationProcessProgress processProgress = null,
            long numberOfDocuments = 1, long numberOfCounters = 1, long numberOfTimeSeriesSegments = 1, long numberOfAttachments = 1)
        {
            processProgress ??= result[0].ProcessesProgress[0];

            Assert.Equal(numberOfDocuments, processProgress.TotalNumberOfDocuments);
            Assert.Equal(numberOfCounters, processProgress.TotalNumberOfCounterGroups);
            Assert.Equal(numberOfTimeSeriesSegments, processProgress.TotalNumberOfTimeSeriesSegments);
            Assert.Equal(numberOfAttachments, processProgress.TotalNumberOfAttachments);
        }

        private void AssertTombstoneItemsProcessed(IReplicationTaskProgress[] result, ReplicationProcessProgress processProgress = null,
            long documentTombstones = 1, long timeSeriesDeletedRanges = 1, long attachmentTombstones = 1)
        {
            processProgress ??= result[0].ProcessesProgress[0];

            Assert.Equal(documentTombstones, processProgress.TotalNumberOfDocumentTombstones);
            Assert.Equal(timeSeriesDeletedRanges, processProgress.TotalNumberOfTimeSeriesDeletedRanges);
            Assert.Equal(attachmentTombstones, processProgress.TotalNumberOfAttachmentTombstones);
        }
    }
}
