using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
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

            var result = await GetReplicationProgress(source, sourceDb.Name);

            AssertReplicationProgress(result, ReplicationNode.ReplicationType.External);
            AssertPendingItemsToProcess(result);

            // continue the replication and let the items replicate to the destination

            replication.Mend();
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(destination, UserId, TimeSpan.FromSeconds(10)));

            result = await GetReplicationProgress(source, sourceDb.Name);

            // now we should have values for the last sent Etag and change vectors, so we retrieve them to verify they are correct

            var (lastSentEtag, sourceChangeVector, destinationChangeVector) = GetReplicationHandlerState(sourceDb);

            AssertReplicationCompletion(result, lastSentEtag, sourceChangeVector, destinationChangeVector);
            AssertPendingItemsToProcess(result, documentsToProcess: 0, countersToProcess: 0, timeSeriesSegmentsToProcess: 0, attachmentsToProcess: 0);
            AssertItemsProcessed(result);

            // break the replication again to perform deletion and check tombstone items

            replication.Break();

            await DeleteUserDocument(source);

            result = await GetReplicationProgress(source, sourceDb.Name);

            AssertReplicationProgress(result, ReplicationNode.ReplicationType.External);
            AssertPendingTombstoneItemsToProcess(result);

            // continue the replication and check if all tombstones are processed

            replication.Mend();

            Assert.True(WaitForDocumentDeletion(destination, UserId));

            result = await GetReplicationProgress(source, sourceDb.Name);

            (lastSentEtag, sourceChangeVector, destinationChangeVector) = GetReplicationHandlerState(sourceDb);

            AssertReplicationCompletion(result, lastSentEtag, sourceChangeVector, destinationChangeVector);
            AssertPendingTombstoneItemsToProcess(result, documentTombstonesToProcess: 0, timeSeriesDeletedRangesToProcess: 0);
            AssertTombstoneItemsProcessed(result);
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

            var result = await GetReplicationProgress(hub);

            AssertReplicationProgress(result, ReplicationNode.ReplicationType.PullAsHub);
            AssertPendingItemsToProcess(result);

            // continue the replication and let the items replicate to the sink

            replication.Mend();
            Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(sink, UserId, TimeSpan.FromSeconds(10)));

            result = await GetReplicationProgress(hub);

            // now we should have values for the last sent Etag and change vectors, so we retrieve them to verify they are correct
            var hubDb = await GetDatabase(hub.Database);
            var (lastSentEtag, sourceChangeVector, destinationChangeVector) = GetReplicationHandlerState(hubDb);

            AssertReplicationCompletion(result, lastSentEtag, sourceChangeVector, destinationChangeVector);
            AssertPendingItemsToProcess(result, documentsToProcess: 0, countersToProcess: 0, timeSeriesSegmentsToProcess: 0, attachmentsToProcess: 0);
            AssertItemsProcessed(result);

            // break the replication again to perform deletion and check tombstone items

            replication.Break();

            await DeleteUserDocument(hub);

            result = await GetReplicationProgress(hub);

            AssertReplicationProgress(result, ReplicationNode.ReplicationType.PullAsHub);
            AssertPendingTombstoneItemsToProcess(result);

            // continue the replication and check if all tombstones are processed

            replication.Mend();
            Assert.True(WaitForDocumentDeletion(sink, UserId));

            result = await GetReplicationProgress(hub);

            (lastSentEtag, sourceChangeVector, destinationChangeVector) = GetReplicationHandlerState(hubDb);

            AssertReplicationCompletion(result, lastSentEtag, sourceChangeVector, destinationChangeVector);
            AssertTombstoneItemsProcessed(result);
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

            var result = await GetReplicationProgress(sink, server: leader);

            AssertItemsProcessed(result);

            await DeleteUserDocument(sink);
            Assert.True(WaitForDocumentDeletion(hub, UserId));

            result = await GetReplicationProgress(sink, server: leader);

            AssertTombstoneItemsProcessed(result);
        }

        private async Task<ReplicationTaskProgress[]> GetReplicationProgress(DocumentStore store, string databaseName = null, RavenServer server = null)
        {
            using var commands = store.Commands(databaseName);
            var nodeTag = server?.ServerStore.NodeTag ?? Server.ServerStore.NodeTag;
            var cmd = new GetReplicationOngoingTasksProgressCommand([], nodeTag);
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

        private void AssertReplicationProgress(ReplicationTaskProgress[] result, ReplicationNode.ReplicationType replicationType)
        {
            Assert.NotEmpty(result);
            Assert.Single(result);

            var progress = result[0];
            Assert.Equal(replicationType, progress.ReplicationType);
            Assert.False(progress.ProcessesProgress[0].Completed);
        }

        private void AssertReplicationCompletion(ReplicationTaskProgress[] result, long lastSentEtag, string sourceChangeVector, string destinationChangeVector)
        {
            Assert.NotEmpty(result);
            Assert.Single(result);

            var processProgress = result[0].ProcessesProgress[0];

            Assert.Equal(lastSentEtag, processProgress.LastEtagSent);
            Assert.Equal(sourceChangeVector, processProgress.SourceChangeVector);
            Assert.Equal(destinationChangeVector, processProgress.DestinationChangeVector);
            Assert.True(processProgress.Completed);
        }

        private void AssertPendingItemsToProcess(ReplicationTaskProgress[] result, long documentsToProcess = 1, long countersToProcess = 1, long timeSeriesSegmentsToProcess = 1, long attachmentsToProcess = 1)
        {
            var processProgress = result[0].ProcessesProgress[0];

            Assert.Equal(documentsToProcess, processProgress.NumberOfDocumentsToProcess);
            Assert.Equal(countersToProcess, processProgress.NumberOfCounterGroupsToProcess);
            Assert.Equal(timeSeriesSegmentsToProcess, processProgress.NumberOfTimeSeriesSegmentsToProcess);
            Assert.Equal(attachmentsToProcess, processProgress.NumberOfAttachmentsToProcess);
        }

        private void AssertPendingTombstoneItemsToProcess(ReplicationTaskProgress[] result, long documentTombstonesToProcess = 1, long timeSeriesDeletedRangesToProcess = 1)
        {
            var processProgress = result[0].ProcessesProgress[0];

            Assert.Equal(documentTombstonesToProcess, processProgress.NumberOfDocumentTombstonesToProcess);
            Assert.Equal(timeSeriesDeletedRangesToProcess, processProgress.NumberOfTimeSeriesDeletedRangesToProcess);
        }

        private void AssertItemsProcessed(ReplicationTaskProgress[] result, long numberOfDocuments = 1, long numberOfCounters = 1, long numberOfTimeSeriesSegments = 1, long numberOfAttachments = 1)
        {
            var processProgress = result[0].ProcessesProgress[0];

            Assert.Equal(numberOfDocuments, processProgress.TotalNumberOfDocuments);
            Assert.Equal(numberOfCounters, processProgress.TotalNumberOfCounterGroups);
            Assert.Equal(numberOfTimeSeriesSegments, processProgress.TotalNumberOfTimeSeriesSegments);
            Assert.Equal(numberOfAttachments, processProgress.TotalNumberOfAttachments);
        }

        private void AssertTombstoneItemsProcessed(ReplicationTaskProgress[] result, long documentTombstones = 1, long timeSeriesDeletedRanges = 1, long attachmentTombstones = 1)
        {
            var processProgress = result[0].ProcessesProgress[0];

            Assert.Equal(documentTombstones, processProgress.TotalNumberOfDocumentTombstones);
            Assert.Equal(timeSeriesDeletedRanges, processProgress.TotalNumberOfTimeSeriesDeletedRanges);
            Assert.Equal(attachmentTombstones, processProgress.TotalNumberOfAttachmentTombstones);
        }
    }
}
