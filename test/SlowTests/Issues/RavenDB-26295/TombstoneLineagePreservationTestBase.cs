using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public abstract class TombstoneLineagePreservationTestBase : ReplicationTestBase
{
    protected TombstoneLineagePreservationTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected sealed record DocumentSnapshot(bool Exists, string ChangeVector, DocumentFlags Flags, string Name);

    protected sealed record TombstoneSnapshot(bool Exists, string ChangeVector, DocumentFlags Flags);

    protected sealed record AttachmentSnapshot(bool Exists, string ChangeVector, string Hash, string ContentType);

    protected sealed record TimeSeriesDeletedRangeSnapshot(string Name, string ChangeVector, long Etag);

    protected sealed record RevisionTombstoneSnapshot(
        string RawKey,
        string KeyChangeVector,
        string RowChangeVector);

    protected async Task<LineageLab> CreateLabAsync(Options options)
    {
        (List<RavenServer> hubNodes, RavenServer hubLeader, TestCertificatesHolder certs) =
            await CreateRaftClusterWithSsl(numberOfNodes: 3, watcherCluster: true);

        var databaseName = GetDatabaseName();
        var adjustedOptions = Replication.AdjustOptionsToClusterSize(new Options(options), hubLeader, clusterSize: 3);

        adjustedOptions.AdminCertificate = certs.ServerCertificateForCommunication.Value;
        adjustedOptions.ClientCertificate = certs.ServerCertificateForCommunication.Value;
        adjustedOptions.ModifyDatabaseName = _ => databaseName;
        adjustedOptions.CreateDatabase = true;

        var baseModifyDatabaseRecord = adjustedOptions.ModifyDatabaseRecord;
        adjustedOptions.ModifyDatabaseRecord = record =>
        {
            baseModifyDatabaseRecord?.Invoke(record);
            record.ConflictSolverConfig = new ConflictSolver
            {
                ResolveToLatest = false,
                ResolveByCollection = new Dictionary<string, ScriptResolver>()
            };
        };

        var hubStore = GetDocumentStore(adjustedOptions);

        const string primingDocId = "internal//priming/sentinel";

        var nodeStores = Cluster.GetDocumentStores(
            nodes: [hubNodes[0], hubNodes[1], hubNodes[2]],
            databaseName,
            disableTopologyUpdates: true,
            certificate: certs.ServerCertificateForCommunication.Value);

        var dbA = await GetDocumentDatabaseInstanceForAsync(nodeStores[0], adjustedOptions.DatabaseMode, primingDocId, hubNodes[0]);
        var dbB = await GetDocumentDatabaseInstanceForAsync(nodeStores[1], adjustedOptions.DatabaseMode, primingDocId, hubNodes[1]);
        var dbC = await GetDocumentDatabaseInstanceForAsync(nodeStores[2], adjustedOptions.DatabaseMode, primingDocId, hubNodes[2]);

        var pullCertificate = new X509Certificate2(
            await File.ReadAllBytesAsync(certs.ClientCertificate2Path),
            password: (string)null,
            X509KeyStorageFlags.Exportable);

        var pullCertBase64 = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Cert));
        var pullCertPfxBase64 = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Pfx));

        foreach (var node in new[] { LineageNode.A, LineageNode.B, LineageNode.C })
        {
            var serverIndex = (int)node;
            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = GetHubName(node),
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                MentorNode = hubNodes[serverIndex].ServerStore.NodeTag,
                PinToMentorNode = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(
                GetHubName(node),
                new ReplicationHubAccess
                {
                    Name = $"Access-{node}",
                    CertificateBase64 = pullCertBase64,
                    AllowedHubToSinkPaths = ["tickets/*"],
                    AllowedSinkToHubPaths = ["tickets/*"]
                }));
        }

        var lab = new LineageLab(
            owner: this,
            databaseName: databaseName,
            certs: certs,
            pullCertPfxBase64: pullCertPfxBase64,
            hubStore: hubStore,
            serverA: hubNodes[0],
            serverB: hubNodes[1],
            serverC: hubNodes[2],
            dbA: dbA,
            dbB: dbB,
            dbC: dbC,
            storeA: nodeStores[0],
            storeB: nodeStores[1],
            storeC: nodeStores[2]);

        lab.TrackForDisposal(hubStore);
        lab.TrackForDisposal(pullCertificate);
        foreach (var store in nodeStores)
            lab.TrackForDisposal(store);

        await lab.PrimeAsync();
        lab.EnsureInternalHandlersReady();

        return lab;
    }

    internal static string GetHubName(LineageNode node) =>
        $"hub-lineage-{node.ToString().ToLowerInvariant()}";

    protected sealed class LineageLab : IAsyncDisposable
    {
        private readonly TombstoneLineagePreservationTestBase _owner;
        private readonly string _databaseName;
        private readonly TestCertificatesHolder _certs;
        private readonly string _pullCertPfxBase64;
        private readonly List<IDisposable> _toDispose = [];

        private IDocumentStore StoreA { get; }
        private IDocumentStore StoreB { get; }
        private IDocumentStore StoreC { get; }

        private DocumentDatabase DatabaseA { get; }
        private DocumentDatabase DatabaseB { get; }
        private DocumentDatabase DatabaseC { get; }

        private RavenServer ServerA { get; }
        private RavenServer ServerB { get; }
        private RavenServer ServerC { get; }

        internal LineageLab(
            TombstoneLineagePreservationTestBase owner,
            string databaseName,
            TestCertificatesHolder certs,
            string pullCertPfxBase64,
            IDocumentStore hubStore,
            RavenServer serverA,
            RavenServer serverB,
            RavenServer serverC,
            DocumentDatabase dbA,
            DocumentDatabase dbB,
            DocumentDatabase dbC,
            IDocumentStore storeA,
            IDocumentStore storeB,
            IDocumentStore storeC)
        {
            _owner = owner;
            _databaseName = databaseName;
            _certs = certs;
            _pullCertPfxBase64 = pullCertPfxBase64;

            ServerA = serverA;
            ServerB = serverB;
            ServerC = serverC;

            DatabaseA = dbA;
            DatabaseB = dbB;
            DatabaseC = dbC;

            StoreA = storeA;
            StoreB = storeB;
            StoreC = storeC;

            _ = hubStore;
        }

        internal void TrackForDisposal(IDisposable disposable) => _toDispose.Add(disposable);

        public IDocumentStore CreateIsolatedStore(string storeNamePrefix = null)
        {
            var store = _owner.GetDocumentStore(new Options
            {
                AdminCertificate = _certs.ServerCertificateForCommunication.Value,
                ClientCertificate = _certs.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => storeNamePrefix ?? $"{_databaseName}-isolated-{Guid.NewGuid():N}"
            });

            TrackForDisposal(store);
            return store;
        }

        public IDocumentStore StoreFor(LineageNode node) => node switch
        {
            LineageNode.A => StoreA,
            LineageNode.B => StoreB,
            LineageNode.C => StoreC,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        private DocumentDatabase DatabaseFor(LineageNode node) => node switch
        {
            LineageNode.A => DatabaseA,
            LineageNode.B => DatabaseB,
            LineageNode.C => DatabaseC,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        private RavenServer ServerFor(LineageNode node) => node switch
        {
            LineageNode.A => ServerA,
            LineageNode.B => ServerB,
            LineageNode.C => ServerC,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        internal async Task PrimeAsync()
        {
            foreach (var writer in new[] { LineageNode.A, LineageNode.B, LineageNode.C })
            {
                var docId = $"internal//priming/{writer.ToString().ToLowerInvariant()}";
                using (var session = StoreFor(writer).OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"prime-{writer}" }, docId);
                    await session.SaveChangesAsync();
                }

                foreach (var reader in new[] { LineageNode.A, LineageNode.B, LineageNode.C })
                {
                    if (reader == writer)
                        continue;

                    Assert.True(_owner.WaitForDocument(StoreFor(reader), docId, timeout: 60_000),
                        userMessage: $"Priming: expected '{docId}' from {writer} to reach {reader}.");
                }
            }
        }

        internal void EnsureInternalHandlersReady()
        {
            foreach (var source in new[] { LineageNode.A, LineageNode.B, LineageNode.C })
            {
                var ready = WaitForValue(
                    () => DatabaseFor(source).ReplicationLoader.OutgoingHandlers
                              .Count(handler => handler.Destination is InternalReplication) >= 2,
                    expectedVal: true,
                    timeout: 30_000);

                Assert.True(ready, userMessage: $"Expected internal outgoing handlers to be ready on {source}.");
            }
        }

        public InternalLinkBlocker BlockLink(LineageNode source, LineageNode target)
        {
            var handler = GetInternalHandler(source: source, target: target);
            return new InternalLinkBlocker(handler);
        }

        public async Task WriteAndInjectTicketAsync(string docId, LineageNode sourceNode, LineageNode targetNode)
        {
            using (var session = StoreFor(sourceNode).OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"ticket-{docId}-on-{sourceNode}" }, docId);
                await session.SaveChangesAsync();
            }

            await InjectExistingTicketAsync(docId, sourceNode: sourceNode, targetNode: targetNode);
        }

        public async Task InjectExistingTicketAsync(string docId, LineageNode sourceNode, LineageNode targetNode)
        {
            await BridgeTicketAsync(
                sourceNode: sourceNode,
                targetNode: targetNode,
                bridgeReady: store => _owner.WaitForDocument(store, docId, timeout: 60_000),
                targetReady: store => _owner.WaitForDocument(store, docId, timeout: 60_000),
                bridgeMessage: $"Expected ticket '{docId}' to arrive in bridge store ({sourceNode}->bridge).",
                targetMessage: $"Expected ticket '{docId}' to arrive on target hub {targetNode} via bridge.");
        }

        public async Task WriteSyncMarkerAndWaitAsync(LineageNode sender, params LineageNode[] waitTargets)
        {
            var markerId = $"markers/sync-{Guid.NewGuid():N}";
            using (var session = StoreFor(sender).OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "sync" }, markerId);
                await session.SaveChangesAsync();
            }

            foreach (var waitTarget in waitTargets)
            {
                Assert.True(_owner.WaitForDocument(StoreFor(waitTarget), markerId, timeout: 60_000),
                    userMessage: $"Sync marker '{markerId}' should have reached {waitTarget} from {sender}.");
            }
        }

        public async Task WriteSyncMarkerAndReleaseAsync(LineageNode sender, LineageNode waitTarget, InternalLinkBlocker blocker)
        {
            var markerId = $"markers/sync-{Guid.NewGuid():N}";
            using (var session = StoreFor(sender).OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "sync" }, markerId);
                await session.SaveChangesAsync();
            }

            blocker.Release();

            Assert.True(_owner.WaitForDocument(StoreFor(waitTarget), markerId, timeout: 60_000),
                userMessage: $"Sync marker '{markerId}' should have reached {waitTarget} from {sender}, confirming {sender}->{waitTarget} replication is active.");
        }

        public async Task<IDocumentStore> CreateExternalSinkStoreAsync(
            LineageNode hubNode,
            PullReplicationMode mode,
            string storeNamePrefix = null,
            string[] allowedHubToSinkPaths = null,
            string[] allowedSinkToHubPaths = null)
        {
            var store = _owner.GetDocumentStore(new Options
            {
                AdminCertificate = _certs.ServerCertificateForCommunication.Value,
                ClientCertificate = _certs.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => storeNamePrefix ?? $"{_databaseName}-external-{hubNode}-{Guid.NewGuid():N}"
            });
            TrackForDisposal(store);

            await ConfigureExternalSinkConnectionAsync(
                store,
                hubNode,
                mode,
                allowedHubToSinkPaths,
                allowedSinkToHubPaths);

            return store;
        }

        public async Task ConfigureExternalSinkConnectionAsync(
            IDocumentStore store,
            LineageNode hubNode,
            PullReplicationMode mode,
            string[] allowedHubToSinkPaths = null,
            string[] allowedSinkToHubPaths = null)
        {
            var connectionName = $"ext-{hubNode}-{Guid.NewGuid():N}";
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                new RavenConnectionString
                {
                    Database = _databaseName,
                    Name = connectionName,
                    TopologyDiscoveryUrls = [ServerFor(hubNode).WebUrl]
                }));

            await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(
                new PullReplicationAsSink
                {
                    ConnectionStringName = connectionName,
                    Mode = mode,
                    CertificateWithPrivateKey = _pullCertPfxBase64,
                    HubName = GetHubName(hubNode),
                    AllowedHubToSinkPaths = allowedHubToSinkPaths ?? ["tickets/*"],
                    AllowedSinkToHubPaths = allowedSinkToHubPaths ?? ["tickets/*"]
                }));
        }

        public int GetConflictCount(LineageNode node, string docId) => StoreFor(node).Commands().GetConflictsFor(docId)?.Length ?? 0;

        public bool WaitForDoc(LineageNode node, string docId, int timeout = 60_000) => _owner.WaitForDocument(StoreFor(node), docId, timeout: timeout);

        public bool WaitForDocumentName(LineageNode node, string docId, string expectedName, int timeout = 60_000) => _owner.WaitForDocument<User>(StoreFor(node), docId, user => user.Name == expectedName, timeout: timeout);

        public bool WaitForAttachment(LineageNode node, string docId, string attachmentName, string expectedHash = null, int timeout = 60_000)
        {
            return WaitForValue(
                () =>
                {
                    var attachment = GetAttachmentSnapshot(node, docId, attachmentName);
                    if (attachment.Exists == false)
                        return false;

                    if (expectedHash != null && string.Equals(attachment.Hash, expectedHash, StringComparison.Ordinal) == false)
                        return false;

                    return true;
                },
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForDeletedRange(LineageNode node, string docId, string timeSeriesName, string expectedChangeVector = null, int timeout = 60_000)
        {
            return WaitForValue(
                () =>
                {
                    var deletedRanges = GetDeletedRangeSnapshots(node, docId);
                    foreach (var deletedRange in deletedRanges)
                    {
                        if (string.Equals(deletedRange.Name, timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        if (expectedChangeVector != null &&
                            string.Equals(deletedRange.ChangeVector, expectedChangeVector, StringComparison.Ordinal) == false)
                            continue;

                        return true;
                    }

                    return false;
                },
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForRevisionTombstones(LineageNode node, string docId, int expectedCount = 1, int timeout = 60_000) => WaitForValue(() => GetRevisionTombstoneSnapshots(node, docId).Count >= expectedCount, expectedVal: true, timeout: timeout);

        public Task ConfigureRevisionsAsync(RevisionsConfiguration configuration) => StoreA.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

        public string GetDatabaseChangeVector(LineageNode node) => Read(node, context => DocumentsStorage.GetDatabaseChangeVector(context).AsString());

        public DocumentSnapshot GetDocumentSnapshot(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var document = DatabaseFor(node).DocumentsStorage.Get(context, docId, throwOnConflict: false);
                if (document == null)
                    return new DocumentSnapshot(false, null, default, null);

                try
                {
                    document.Data.TryGet(nameof(User.Name), out string name);
                    return new DocumentSnapshot(true, document.ChangeVector, document.Flags, name);
                }
                finally
                {
                    document.Dispose();
                }
            });
        }

        public TombstoneSnapshot GetDocumentTombstoneSnapshot(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var documentOrTombstone = DatabaseFor(node).DocumentsStorage.GetDocumentOrTombstone(context, docId, throwOnConflict: false);
                try
                {
                    if (documentOrTombstone.Tombstone == null)
                        return new TombstoneSnapshot(false, null, default);

                    return new TombstoneSnapshot(
                        true,
                        documentOrTombstone.Tombstone.ChangeVector,
                        documentOrTombstone.Tombstone.Flags);
                }
                finally
                {
                    documentOrTombstone.Document?.Dispose();
                    documentOrTombstone.Tombstone?.Dispose();
                }
            });
        }

        public AttachmentSnapshot GetAttachmentSnapshot(LineageNode node, string docId, string attachmentName)
        {
            return Read(node, context =>
            {
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, docId, out Slice lowerDocumentId))
                {
                    var attachment = DatabaseFor(node).DocumentsStorage.AttachmentsStorage
                        .GetAttachmentDetailsForDocument(context, lowerDocumentId)
                        .FirstOrDefault(x => string.Equals(x.Name, attachmentName, StringComparison.Ordinal));

                    if (attachment == null)
                        return new AttachmentSnapshot(false, null, null, null);

                    return new AttachmentSnapshot(
                        true,
                        attachment.ChangeVector,
                        attachment.Hash,
                        attachment.ContentType);
                }
            });
        }

        public unsafe TombstoneSnapshot GetAttachmentTombstoneSnapshot(
            LineageNode node,
            string docId,
            string attachmentName,
            string hash,
            string contentType)
        {
            return Read(node, context =>
            {
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, docId, out Slice lowerDocumentId))
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, attachmentName, out Slice lowerAttachmentName))
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, contentType, out Slice lowerContentType))
                using (Slice.From(context.Allocator, hash, out Slice base64Hash))
                using (AttachmentsStorage.AttachmentKey.GetKey(
                           context,
                           lowerDocumentId.Content.Ptr,
                           lowerDocumentId.Size,
                           lowerAttachmentName.Content.Ptr,
                           lowerAttachmentName.Size,
                           base64Hash,
                           lowerContentType.Content.Ptr,
                           lowerContentType.Size,
                           AttachmentType.Document,
                           Slices.Empty,
                           out Slice keySlice))
                {
                    var tombstone = DatabaseFor(node).DocumentsStorage.AttachmentsStorage.GetAttachmentTombstoneByKey(context, keySlice);
                    if (tombstone == null)
                        return new TombstoneSnapshot(false, null, default);

                    try
                    {
                        return new TombstoneSnapshot(true, tombstone.ChangeVector, tombstone.Flags);
                    }
                    finally
                    {
                        tombstone.Dispose();
                    }
                }
            });
        }

        public List<TimeSeriesDeletedRangeSnapshot> GetDeletedRangeSnapshots(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var result = new List<TimeSeriesDeletedRangeSnapshot>();
                foreach (var deletedRange in DatabaseFor(node).DocumentsStorage.TimeSeriesStorage.GetDeletedRangesForDoc(context, docId))
                {
                    using (deletedRange)
                    {
                        TimeSeriesValuesSegment.ParseTimeSeriesKey(deletedRange.Key, context, out _, out var timeSeriesName);
                        result.Add(new TimeSeriesDeletedRangeSnapshot(
                            timeSeriesName.ToString(CultureInfo.InvariantCulture),
                            deletedRange.ChangeVector,
                            deletedRange.Etag));
                    }
                }

                return result;
            });
        }

        public List<RevisionTombstoneSnapshot> GetRevisionTombstoneSnapshots(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var result = new List<RevisionTombstoneSnapshot>();
                foreach (var item in DatabaseFor(node).DocumentsStorage.GetTombstonesFrom(context, etag: 0, revisionTombstonesWithId: true))
                {
                    if (item is not RevisionTombstoneReplicationItem tombstone)
                    {
                        item.Dispose();
                        continue;
                    }

                    using (tombstone)
                    {
                        RevisionTombstoneReplicationItem.TryExtractDocumentIdAndChangeVectorFromKey(
                            tombstone.Id,
                            out var revisionDocId,
                            out var keyChangeVector);

                        if (string.Equals(revisionDocId, docId, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        result.Add(new RevisionTombstoneSnapshot(
                            tombstone.Id.ToString(CultureInfo.InvariantCulture),
                            keyChangeVector,
                            tombstone.ChangeVector));
                    }
                }

                return result;
            });
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var disposable in _toDispose)
                disposable.Dispose();

            await Task.CompletedTask;
        }

        private DatabaseOutgoingReplicationHandler GetInternalHandler(LineageNode source, LineageNode target)
        {
            DatabaseOutgoingReplicationHandler handler = null;
            var found = WaitForValue(
                () =>
                {
                    handler = DatabaseFor(source).ReplicationLoader.OutgoingHandlers.SingleOrDefault(h =>
                        h.Destination is InternalReplication internalReplication &&
                        string.Equals(internalReplication.NodeTag, target.ToString(), StringComparison.OrdinalIgnoreCase));
                    return handler != null;
                },
                expectedVal: true,
                timeout: 30_000);

            Assert.True(found, userMessage: $"Expected internal replication handler {source}->{target} to exist.");
            return handler;
        }

        private TResult Read<TResult>(LineageNode node, Func<DocumentsOperationContext, TResult> read)
        {
            var database = DatabaseFor(node);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
                return read(context);
        }

        private async Task BridgeTicketAsync(
            LineageNode sourceNode,
            LineageNode targetNode,
            Func<IDocumentStore, bool> bridgeReady,
            Func<IDocumentStore, bool> targetReady,
            string bridgeMessage,
            string targetMessage)
        {
            var bridgeName = $"{_databaseName}-bridge-{sourceNode}-{targetNode}-{Guid.NewGuid():N}";
            var bridgeStore = _owner.GetDocumentStore(new Options
            {
                AdminCertificate = _certs.ServerCertificateForCommunication.Value,
                ClientCertificate = _certs.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => bridgeName
            });
            TrackForDisposal(bridgeStore);

            var sourceConnName = $"src-{sourceNode}-{Guid.NewGuid():N}";
            var targetConnName = $"tgt-{targetNode}-{Guid.NewGuid():N}";

            await bridgeStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                new RavenConnectionString
                {
                    Database = _databaseName,
                    Name = sourceConnName,
                    TopologyDiscoveryUrls = [ServerFor(sourceNode).WebUrl]
                }));

            var sourceTask = await bridgeStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(
                new PullReplicationAsSink
                {
                    ConnectionStringName = sourceConnName,
                    Mode = PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = _pullCertPfxBase64,
                    HubName = GetHubName(sourceNode),
                    AllowedHubToSinkPaths = ["tickets/*"]
                }));

            Assert.True(bridgeReady(bridgeStore), userMessage: bridgeMessage);

            await bridgeStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                new RavenConnectionString
                {
                    Database = _databaseName,
                    Name = targetConnName,
                    TopologyDiscoveryUrls = [ServerFor(targetNode).WebUrl]
                }));

            var targetTask = await bridgeStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(
                new PullReplicationAsSink
                {
                    ConnectionStringName = targetConnName,
                    Mode = PullReplicationMode.SinkToHub,
                    CertificateWithPrivateKey = _pullCertPfxBase64,
                    HubName = GetHubName(targetNode),
                    AllowedSinkToHubPaths = ["tickets/*"]
                }));

            Assert.True(targetReady(StoreFor(targetNode)), userMessage: targetMessage);

            await bridgeStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(targetTask.TaskId, OngoingTaskType.PullReplicationAsSink));
            await bridgeStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(sourceTask.TaskId, OngoingTaskType.PullReplicationAsSink));
        }
    }

    protected sealed class InternalLinkBlocker : IDisposable
    {
        private readonly DatabaseOutgoingReplicationHandler _handler;
        private readonly ManualResetEventSlim _gate = new(initialState: false);
        private readonly Action _previous;

        public InternalLinkBlocker(DatabaseOutgoingReplicationHandler handler)
        {
            _handler = handler;
            _previous = handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem;
            handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = Wait;
        }

        public void Release() => _gate.Set();

        public void Dispose()
        {
            _handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = _previous;
            _gate.Set();
            _gate.Dispose();
        }

        private void Wait()
        {
            _previous?.Invoke();
            _gate.Wait();
        }
    }
}

public enum LineageNode
{
    A = 0,
    B = 1,
    C = 2,
    D = 3
}
