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
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Utils;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public abstract class NonDocumentDbCvProtectionTestBase : ReplicationTestBase
{
    protected NonDocumentDbCvProtectionTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected sealed record DocumentSnapshot(bool Exists, string ChangeVector, DocumentFlags Flags, string Name);

    protected sealed record AttachmentSnapshot(bool Exists, string ChangeVector, string Hash, string ContentType);

    protected sealed record TombstoneSnapshot(bool Exists, string ChangeVector, DocumentFlags Flags);

    protected sealed record TimeSeriesDeletedRangeSnapshot(string Name, string ChangeVector, DateTime From, DateTime To, long Etag);

    protected sealed record CounterSnapshot(bool Exists, long Value);

    protected sealed record CounterGroupSnapshot(string DocumentId, string ChangeVector, long Etag);

    protected sealed record TimeSeriesSegmentSnapshot(string DocumentId, string Name, string ChangeVector, long Etag);

    protected sealed record SatelliteMarker(string Name, string Hash = null, string ContentType = null);

    protected async Task<NonDocumentLab> CreateLabAsync(Options options)
    {
        (List<RavenServer> hubNodes, RavenServer hubLeader, TestCertificatesHolder certs) =
            await CreateRaftClusterWithSsl(numberOfNodes: 4, watcherCluster: true);

        var databaseName = GetDatabaseName();
        var adjustedOptions = Replication.AdjustOptionsToClusterSize(new Options(options), hubLeader, clusterSize: 4);

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
                ResolveToLatest = false
            };
        };

        var hubStore = GetDocumentStore(adjustedOptions);
        var primingDocId = "internal//non-doc-cv-priming";

        var nodeStores = Cluster.GetDocumentStores(
            nodes: [hubNodes[0], hubNodes[1], hubNodes[2], hubNodes[3]],
            databaseName,
            disableTopologyUpdates: true,
            certificate: certs.ServerCertificateForCommunication.Value);

        var dbA = await GetDocumentDatabaseInstanceForAsync(nodeStores[0], adjustedOptions.DatabaseMode, primingDocId, hubNodes[0]);
        var dbB = await GetDocumentDatabaseInstanceForAsync(nodeStores[1], adjustedOptions.DatabaseMode, primingDocId, hubNodes[1]);
        var dbC = await GetDocumentDatabaseInstanceForAsync(nodeStores[2], adjustedOptions.DatabaseMode, primingDocId, hubNodes[2]);
        var dbD = await GetDocumentDatabaseInstanceForAsync(nodeStores[3], adjustedOptions.DatabaseMode, primingDocId, hubNodes[3]);

        var pullCertificate = new X509Certificate2(
            await File.ReadAllBytesAsync(certs.ClientCertificate2Path),
            password: (string)null,
            X509KeyStorageFlags.Exportable);

        var pullCertBase64 = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Cert));
        var pullCertPfxBase64 = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Pfx));

        foreach (var node in new[] { LineageNode.A, LineageNode.B, LineageNode.C, LineageNode.D })
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

        var lab = new NonDocumentLab(
            owner: this,
            databaseName: databaseName,
            certs: certs,
            pullCertPfxBase64: pullCertPfxBase64,
            serverA: hubNodes[0],
            serverB: hubNodes[1],
            serverC: hubNodes[2],
            serverD: hubNodes[3],
            dbA: dbA,
            dbB: dbB,
            dbC: dbC,
            dbD: dbD,
            storeA: nodeStores[0],
            storeB: nodeStores[1],
            storeC: nodeStores[2],
            storeD: nodeStores[3]);

        lab.TrackForDisposal(hubStore);
        lab.TrackForDisposal(pullCertificate);
        foreach (var store in nodeStores)
            lab.TrackForDisposal(store);

        await lab.PrimeAsync();
        lab.EnsureInternalHandlersReady();
        return lab;
    }

    protected static async Task StoreUserAsync(NonDocumentLab lab, LineageNode node, string docId, string userName)
    {
        using var session = lab.StoreFor(node).OpenAsyncSession();
        await session.StoreAsync(new User { Name = userName }, docId);
        await session.SaveChangesAsync();
    }

    protected static async Task<SatelliteMarker> AddCounterAsync(NonDocumentLab lab, string docId, string counterName, LineageNode node)
    {
        using var session = lab.StoreFor(node).OpenAsyncSession();
        session.CountersFor(docId).Increment(counterName, delta: 1);
        await session.SaveChangesAsync();
        return new SatelliteMarker(counterName);
    }

    protected static async Task<SatelliteMarker> AddAttachmentAsync(NonDocumentLab lab, string docId, string attachmentName, LineageNode node)
    {
        await lab.StoreFor(node).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([1, 2, 3, 4, 5]),
                "application/octet-stream"));

        var attachment = lab.GetAttachmentSnapshot(node, docId, attachmentName);
        Assert.True(attachment.Exists, userMessage: $"Expected attachment '{attachmentName}' on '{docId}' at {node}.");
        return new SatelliteMarker(attachmentName, attachment.Hash, attachment.ContentType);
    }

    protected static async Task<SatelliteMarker> AddTimeSeriesAsync(
        NonDocumentLab lab,
        string docId,
        string timeSeriesName,
        DateTime baseline,
        LineageNode node)
    {
        using var session = lab.StoreFor(node).OpenAsyncSession();
        session.TimeSeriesFor(docId, timeSeriesName).Append(baseline, 72.0, "bpm");
        await session.SaveChangesAsync();
        return new SatelliteMarker(timeSeriesName);
    }

    protected static async Task<SatelliteMarker> AddTimeSeriesDeletedRangeAsync(
        NonDocumentLab lab,
        string docId,
        string timeSeriesName,
        DateTime baseline,
        LineageNode node)
    {
        using (var session = lab.StoreFor(node).OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Append(baseline, 99.0, "bpm");
            await session.SaveChangesAsync();
        }

        using (var session = lab.StoreFor(node).OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
            await session.SaveChangesAsync();
        }

        return new SatelliteMarker(timeSeriesName);
    }

    protected static async Task<SatelliteMarker> AddAttachmentTombstoneAsync(
        NonDocumentLab lab,
        string docId,
        string attachmentName,
        LineageNode node)
    {
        await lab.StoreFor(node).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7]),
                "application/octet-stream"));

        var attachment = lab.GetAttachmentSnapshot(node, docId, attachmentName);
        Assert.True(attachment.Exists, userMessage: $"Expected attachment '{attachmentName}' on '{docId}' at {node} before deleting it.");

        using (var session = lab.StoreFor(node).OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete(docId, attachmentName);
            await session.SaveChangesAsync();
        }

        var tombstone = lab.GetAttachmentTombstoneSnapshot(node, docId, attachmentName, attachment.Hash, attachment.ContentType);
        Assert.True(tombstone.Exists, userMessage: $"Expected attachment tombstone '{attachmentName}' on '{docId}' at {node}.");
        return new SatelliteMarker(attachmentName, attachment.Hash, attachment.ContentType);
    }

    internal static string GetHubName(LineageNode node) =>
        $"hub-non-document-{node.ToString().ToLowerInvariant()}";

    protected sealed class NonDocumentLab : IAsyncDisposable
    {
        private readonly NonDocumentDbCvProtectionTestBase _owner;
        private readonly string _databaseName;
        private readonly TestCertificatesHolder _certs;
        private readonly string _pullCertPfxBase64;
        private readonly List<IDisposable> _toDispose = [];

        public IDocumentStore StoreA { get; }
        public IDocumentStore StoreB { get; }
        public IDocumentStore StoreC { get; }
        public IDocumentStore StoreD { get; }

        public DocumentDatabase DatabaseA { get; }
        public DocumentDatabase DatabaseB { get; }
        public DocumentDatabase DatabaseC { get; }
        public DocumentDatabase DatabaseD { get; }

        public RavenServer ServerA { get; }
        public RavenServer ServerB { get; }
        public RavenServer ServerC { get; }
        public RavenServer ServerD { get; }

        internal NonDocumentLab(
            NonDocumentDbCvProtectionTestBase owner,
            string databaseName,
            TestCertificatesHolder certs,
            string pullCertPfxBase64,
            RavenServer serverA,
            RavenServer serverB,
            RavenServer serverC,
            RavenServer serverD,
            DocumentDatabase dbA,
            DocumentDatabase dbB,
            DocumentDatabase dbC,
            DocumentDatabase dbD,
            IDocumentStore storeA,
            IDocumentStore storeB,
            IDocumentStore storeC,
            IDocumentStore storeD)
        {
            _owner = owner;
            _databaseName = databaseName;
            _certs = certs;
            _pullCertPfxBase64 = pullCertPfxBase64;

            ServerA = serverA;
            ServerB = serverB;
            ServerC = serverC;
            ServerD = serverD;

            DatabaseA = dbA;
            DatabaseB = dbB;
            DatabaseC = dbC;
            DatabaseD = dbD;

            StoreA = storeA;
            StoreB = storeB;
            StoreC = storeC;
            StoreD = storeD;
        }

        internal void TrackForDisposal(IDisposable disposable) => _toDispose.Add(disposable);

        public IDocumentStore StoreFor(LineageNode node) => node switch
        {
            LineageNode.A => StoreA,
            LineageNode.B => StoreB,
            LineageNode.C => StoreC,
            LineageNode.D => StoreD,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        public DocumentDatabase DatabaseFor(LineageNode node) => node switch
        {
            LineageNode.A => DatabaseA,
            LineageNode.B => DatabaseB,
            LineageNode.C => DatabaseC,
            LineageNode.D => DatabaseD,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        public RavenServer ServerFor(LineageNode node) => node switch
        {
            LineageNode.A => ServerA,
            LineageNode.B => ServerB,
            LineageNode.C => ServerC,
            LineageNode.D => ServerD,
            _ => throw new ArgumentOutOfRangeException(nameof(node))
        };

        internal async Task PrimeAsync()
        {
            foreach (var writer in new[] { LineageNode.A, LineageNode.B, LineageNode.C, LineageNode.D })
            {
                var docId = $"internal//priming/{writer.ToString().ToLowerInvariant()}";
                await StoreUserAsync(this, writer, docId, $"prime-{writer}");

                foreach (var reader in new[] { LineageNode.A, LineageNode.B, LineageNode.C, LineageNode.D })
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
            foreach (var source in new[] { LineageNode.A, LineageNode.B, LineageNode.C, LineageNode.D })
            {
                var ready = WaitForValue(
                    () => DatabaseFor(source).ReplicationLoader.OutgoingHandlers
                              .Count(handler => handler.Destination is InternalReplication) >= 3,
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

        public bool WaitForDoc(LineageNode node, string docId, int timeout = 60_000)
        {
            return _owner.WaitForDocument(StoreFor(node), docId, timeout: timeout);
        }

        public bool WaitForDocumentName(LineageNode node, string docId, string expectedName, int timeout = 60_000)
        {
            return _owner.WaitForDocument<User>(StoreFor(node), docId, user => user.Name == expectedName, timeout: timeout);
        }

        public bool WaitForCounter(LineageNode node, string docId, string counterName, long expectedValue, int timeout = 60_000)
        {
            return WaitForValue(
                () =>
                {
                    var counter = GetCounterSnapshot(node, docId, counterName);
                    return counter.Exists && counter.Value == expectedValue;
                },
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForCounterGroup(LineageNode node, string docId, int expectedCount = 1, int timeout = 60_000)
        {
            return WaitForValue(
                () => GetCounterGroupSnapshots(node, docId).Count >= expectedCount,
                expectedVal: true,
                timeout: timeout);
        }

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

        public bool WaitForTimeSeries(LineageNode node, string docId, string timeSeriesName, int timeout = 60_000)
        {
            return WaitForValue(
                () => Read(
                    node,
                    context => DatabaseFor(node).DocumentsStorage.TimeSeriesStorage
                        .GetReader(context, docId, timeSeriesName, DateTime.MinValue, DateTime.MaxValue)
                        .AllValues()
                        .Any()),
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForTimeSeriesSegment(LineageNode node, string docId, string timeSeriesName, int expectedCount = 1, int timeout = 60_000)
        {
            return WaitForValue(
                () => GetTimeSeriesSegmentSnapshots(node, docId)
                    .Count(x => string.Equals(x.Name, timeSeriesName, StringComparison.OrdinalIgnoreCase)) >= expectedCount,
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForDeletedRange(LineageNode node, string docId, string timeSeriesName, int timeout = 60_000)
        {
            return WaitForValue(
                () => GetDeletedRangeSnapshots(node, docId)
                    .Any(x => string.Equals(x.Name, timeSeriesName, StringComparison.OrdinalIgnoreCase)),
                expectedVal: true,
                timeout: timeout);
        }

        public bool WaitForAttachmentTombstone(LineageNode node, string docId, string attachmentName, string hash, string contentType, int timeout = 60_000)
        {
            return WaitForValue(
                () => GetAttachmentTombstoneSnapshot(node, docId, attachmentName, hash, contentType).Exists,
                expectedVal: true,
                timeout: timeout);
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

        public async Task<string> WriteSyncMarkerAndWaitAsync(LineageNode sender, params LineageNode[] waitTargets)
        {
            var markerId = $"markers/sync-{Guid.NewGuid():N}";
            await StoreUserAsync(this, sender, markerId, "sync");

            foreach (var waitTarget in waitTargets)
            {
                Assert.True(WaitForDoc(waitTarget, markerId, timeout: 60_000),
                    userMessage: $"Sync marker '{markerId}' should have reached {waitTarget} from {sender}.");
            }

            return markerId;
        }

        public string GetDatabaseChangeVector(LineageNode node)
        {
            return Read(node, context => DocumentsStorage.GetDatabaseChangeVector(context).AsString());
        }

        public string GetFullDatabaseChangeVector(LineageNode node)
        {
            return Read(node, DocumentsStorage.GetFullDatabaseChangeVector);
        }

        public IDocumentStore CreateIsolatedStore(string suffix)
        {
            var store = _owner.GetDocumentStore(new Options
            {
                AdminCertificate = _certs.ServerCertificateForCommunication.Value,
                ClientCertificate = _certs.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => $"{_databaseName}-{suffix}-{Guid.NewGuid():N}"
            });

            TrackForDisposal(store);
            return store;
        }

        public async Task<ModifyOngoingTaskResult> ConnectSinkToHubAsync(
            IDocumentStore sinkStore,
            LineageNode targetNode,
            string[] allowedSinkToHubPaths = null)
        {
            var connectionName = $"sink-{targetNode}-{Guid.NewGuid():N}";

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                new RavenConnectionString
                {
                    Database = _databaseName,
                    Name = connectionName,
                    TopologyDiscoveryUrls = [ServerFor(targetNode).WebUrl]
                }));

            return await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(
                new PullReplicationAsSink
                {
                    ConnectionStringName = connectionName,
                    Mode = PullReplicationMode.SinkToHub,
                    CertificateWithPrivateKey = _pullCertPfxBase64,
                    HubName = GetHubName(targetNode),
                    AllowedSinkToHubPaths = allowedSinkToHubPaths ?? ["tickets/*"]
                }));
        }

        public string GetBackgroundWorkResponsibleNodeTag()
        {
            using (DatabaseA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return DatabaseA.ServerStore.Cluster.ReadDatabaseTopology(context, DatabaseA.Name).AllNodes.FirstOrDefault();
            }
        }

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

        public CounterSnapshot GetCounterSnapshot(LineageNode node, string docId, string counterName)
        {
            return Read(node, context =>
            {
                var counter = DatabaseFor(node).DocumentsStorage.CountersStorage.GetCounterValue(context, docId, counterName);
                if (counter == null)
                    return new CounterSnapshot(false, 0);

                return new CounterSnapshot(true, counter.Value.Value);
            });
        }

        public List<CounterGroupSnapshot> GetCounterGroupSnapshots(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var result = new List<CounterGroupSnapshot>();
                foreach (var counterGroup in DatabaseFor(node).DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0, skip: 0, take: long.MaxValue))
                {
                    using (counterGroup)
                    {
                        var counterDocId = counterGroup.DocumentId?.ToString(CultureInfo.InvariantCulture);
                        if (string.Equals(counterDocId, docId, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        result.Add(new CounterGroupSnapshot(
                            counterDocId,
                            counterGroup.ChangeVector,
                            counterGroup.Etag));
                    }
                }

                return result;
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
                            deletedRange.From,
                            deletedRange.To,
                            deletedRange.Etag));
                    }
                }

                return result;
            });
        }

        public List<TimeSeriesSegmentSnapshot> GetTimeSeriesSegmentSnapshots(LineageNode node, string docId)
        {
            return Read(node, context =>
            {
                var result = new List<TimeSeriesSegmentSnapshot>();
                foreach (var segment in DatabaseFor(node).DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, etag: 0))
                {
                    using (segment)
                    {
                        TimeSeriesValuesSegment.ParseTimeSeriesKey(segment.Key, context, out var segmentDocId, out var timeSeriesName);
                        var currentDocId = segmentDocId?.ToString(CultureInfo.InvariantCulture);
                        if (string.Equals(currentDocId, docId, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        result.Add(new TimeSeriesSegmentSnapshot(
                            currentDocId,
                            timeSeriesName?.ToString(CultureInfo.InvariantCulture),
                            segment.ChangeVector,
                            segment.Etag));
                    }
                }

                return result;
            });
        }

        public async Task ApplyHeartbeatChangeVectorAsync(LineageNode source, LineageNode target)
        {
            var sourceDatabase = DatabaseFor(source);
            var targetDatabase = DatabaseFor(target);
            string adjustedSourceChangeVector;
            long sourceLastDatabaseEtag;

            using (sourceDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                string sourceChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                sourceLastDatabaseEtag = sourceDatabase.DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);

                if (string.IsNullOrEmpty(sourceChangeVector))
                {
                    adjustedSourceChangeVector = sourceChangeVector;
                }
                else
                {
                    var update = ChangeVectorUtils.TryUpdateChangeVector(
                        sourceDatabase.ServerStore.NodeTag,
                        sourceDatabase.DbBase64Id,
                        sourceLastDatabaseEtag,
                        context.GetChangeVector(sourceChangeVector));
                    adjustedSourceChangeVector = update.IsValid ? update.ChangeVector : sourceChangeVector;
                }
            }


            var connectionInfo = new IncomingConnectionInfo
            {
                SourceDatabaseId = sourceDatabase.DbId.ToString(),
                SourceDatabaseBase64Id = sourceDatabase.DbBase64Id,
                SourceDatabaseName = sourceDatabase.Name,
                SourceUrl = ServerFor(source).WebUrl,
                SourceMachineName = Environment.MachineName,
                SourceTag = ServerFor(source).ServerStore.NodeTag,
                ReplicationsType = Raven.Client.Documents.Replication.Messages.ReplicationLatestEtagRequest.ReplicationType.Internal
            };

            var command = new IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommand(
                adjustedSourceChangeVector,
                sourceLastDatabaseEtag,
                connectionInfo,
                new AsyncManualResetEvent());

            await targetDatabase.TxMerger.Enqueue(command);
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
            {
                return read(context);
            }
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

            await bridgeStore.Maintenance.SendAsync(
                new DeleteOngoingTaskOperation(targetTask.TaskId, OngoingTaskType.PullReplicationAsSink));
            await bridgeStore.Maintenance.SendAsync(
                new DeleteOngoingTaskOperation(sourceTask.TaskId, OngoingTaskType.PullReplicationAsSink));
        }
    }

    protected sealed class InternalLinkBlocker : IDisposable
    {
        private readonly DatabaseOutgoingReplicationHandler _handler;
        private readonly ManualResetEventSlim _gate = new(initialState: false);
        private readonly Action _previousFetch;
        private readonly Action _previousHeartbeat;

        public InternalLinkBlocker(DatabaseOutgoingReplicationHandler handler)
        {
            _handler = handler;
            var testing = handler.ForTestingPurposesOnly();
            _previousFetch = testing.OnDocumentSenderFetchNewItem;
            _previousHeartbeat = testing.OnBeforeOutgoingHeartbeat;
            testing.OnDocumentSenderFetchNewItem = WaitForFetch;
            testing.OnBeforeOutgoingHeartbeat = WaitForHeartbeat;

        }

        public void Release()
        {
            _gate.Set();
        }

        public void Dispose()
        {
            var testing = _handler.ForTestingPurposesOnly();
            testing.OnDocumentSenderFetchNewItem = _previousFetch;
            testing.OnBeforeOutgoingHeartbeat = _previousHeartbeat;
            _gate.Set();
            _gate.Dispose();
        }

        private void WaitForFetch()
        {
            _previousFetch?.Invoke();
            _gate.Wait();
        }

        private void WaitForHeartbeat()
        {
            _previousHeartbeat?.Invoke();
            _gate.Wait();
        }
    }
}
