using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Voron;
using Xunit;
using static SlowTests.Issues.RavenDB_26295.FilteredPullDualClusterTestBase;

namespace SlowTests.Issues.RavenDB_26295.Tools;

public sealed class DualClusterLab : IAsyncDisposable
{
    private static readonly TimeSpan DefaultReplicationWaitTimeout = TimeSpan.FromSeconds(10);

    private readonly Dictionary<string, IDocumentStore> _stores;
    private readonly Dictionary<string, RavenServer> _servers;
    private readonly Dictionary<string, DocumentDatabase> _databases;
    private readonly List<LabNode> _hubNodes;
    private readonly List<LabNode> _sinkNodes;
    private readonly List<LogicalInternalReplicationBlocker> _internalReplicationBlockers;
    private readonly List<IDisposable> _testingHookScopes;
    private Func<ClusterSide, LabNode, Task> _waitForExpectedFilteredRoundTripItemAsync;
    private FilteredRoundTripReplicationGate _filteredRoundTripFirstHopGate;
    private FilteredRoundTripReplicationGate _filteredRoundTripReturnGate;
    private bool _filteredRoundTripTaskConfigured;
    private bool _filteredRoundTripReturnLegUnfiltered;
    private bool _allowedTicketThenFilteredOutUserStored;

    public DualClusterLab(
        TaggedCluster hubCluster,
        TaggedCluster sinkCluster,
        string hubDatabaseName,
        string sinkDatabaseName,
        string pullCertificate,
        ClusterSide? filteredPassReceiveSide,
        string itemName)
    {
        HubCluster = hubCluster;
        SinkCluster = sinkCluster;
        HubDatabaseName = hubDatabaseName;
        SinkDatabaseName = sinkDatabaseName;
        PullCertificate = pullCertificate;
        FilteredPassReceiveSide = filteredPassReceiveSide;
        FilteredRoundTripItemName = itemName;
        if (filteredPassReceiveSide.HasValue && string.IsNullOrWhiteSpace(itemName))
            throw new ArgumentException("Item name is required when the lab owns a filtered round-trip ticket id.", nameof(itemName));

        FilteredRoundTripPath = filteredPassReceiveSide.HasValue
            ? GetFilteredRoundTripPath(filteredPassReceiveSide.Value)
            : null;

        AllowedTicketBeforeFilteredOutUserId = filteredPassReceiveSide.HasValue
            ? $"{FilteredRoundTripPath}/before/{itemName}"
            : null;

        FilteredOutUserDocumentId = filteredPassReceiveSide.HasValue
            ? GetFilteredOutUserDocumentId(filteredPassReceiveSide.Value, itemName)
            : null;

        FilteredRoundTripTicketId = filteredPassReceiveSide.HasValue
            ? $"{FilteredRoundTripPath}/{itemName}"
            : null;

        _waitForExpectedFilteredRoundTripItemAsync = filteredPassReceiveSide.HasValue
            ? (side, node) => WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName)
            : null;

        _stores = new Dictionary<string, IDocumentStore>(StringComparer.OrdinalIgnoreCase);
        _servers = new Dictionary<string, RavenServer>(StringComparer.OrdinalIgnoreCase);
        _databases = new Dictionary<string, DocumentDatabase>(StringComparer.OrdinalIgnoreCase);
        _hubNodes = [];
        _sinkNodes = [];
        _internalReplicationBlockers = [];
        _testingHookScopes = [];
    }

    private TaggedCluster HubCluster { get; }

    private TaggedCluster SinkCluster { get; }

    private TestCertificatesHolder HubCertificates => HubCluster.Certificates;

    private TestCertificatesHolder SinkCertificates => SinkCluster.Certificates;

    public string HubDatabaseName { get; }

    public string SinkDatabaseName { get; }

    public string PullCertificate { get; }

    private ClusterSide? FilteredPassReceiveSide { get; }

    public string FilteredRoundTripTicketId { get; }

    private string FilteredRoundTripPath { get; }

    private string AllowedTicketBeforeFilteredOutUserId { get; }

    private string FilteredOutUserDocumentId { get; }

    private string FilteredRoundTripItemName { get; }

    private string RequiredFilteredRoundTripTicketId => FilteredRoundTripTicketId ?? throw new InvalidOperationException(
        $"{nameof(FilteredRoundTripTicketId)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    private string RequiredFilteredRoundTripPath => FilteredRoundTripPath ?? throw new InvalidOperationException(
        $"{nameof(FilteredRoundTripPath)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    private string RequiredAllowedTicketBeforeFilteredOutUserId => AllowedTicketBeforeFilteredOutUserId ?? throw new InvalidOperationException(
        $"{nameof(AllowedTicketBeforeFilteredOutUserId)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    private string RequiredFilteredOutUserDocumentId => FilteredOutUserDocumentId ?? throw new InvalidOperationException(
        $"{nameof(FilteredOutUserDocumentId)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    private string RequiredFilteredRoundTripItemName => FilteredRoundTripItemName ?? throw new InvalidOperationException(
        $"{nameof(FilteredRoundTripItemName)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    private ClusterSide RequiredFilteredPassReceiveSide => FilteredPassReceiveSide ?? throw new InvalidOperationException(
        $"{nameof(FilteredPassReceiveSide)} is available only when CreateDualClusterLabAsync is called with a filtered pass receive side and item name.");

    public void AddNode(ClusterSide side, LabNode node, RavenServer server, IDocumentStore store, DocumentDatabase database)
    {
        var key = Key(side, node);
        _servers[key] = server;
        _stores[key] = store;
        _databases[key] = database;
        (side == ClusterSide.Hub ? _hubNodes : _sinkNodes).Add(node);
    }

    private IDocumentStore Store(ClusterSide side, LabNode node) => _stores[Key(side, node)];

    public IDocumentStore GetStore(ClusterSide side, LabNode node) => Store(side, node);

    private IReadOnlyList<LabNode> NodesOf(ClusterSide side) => side == ClusterSide.Hub ? _hubNodes : _sinkNodes;

    public async Task SeedInternalReplicationAsync()
    {
        await SeedInternalReplicationAsync(ClusterSide.Hub);
        await SeedInternalReplicationAsync(ClusterSide.Sink);
    }

    private async Task SeedInternalReplicationAsync(ClusterSide side)
    {
        var nodes = NodesOf(side);
        foreach (var node in nodes)
        {
            var nodeTag = NodeTagLower(side, node);
            var documentId = $"tickets/internal-replication-seed/from-{nodeTag}";
            var name = "internal-replication-seed-from-" + nodeTag;

            await StoreTicketAsync(side, node, documentId, name);

            foreach (var other in nodes.Where(x => x != node))
                await WaitForDocumentNameByIdAsync(side, other, documentId, name, DefaultReplicationWaitTimeout);
        }
    }

    public async Task WaitForInternalHandlersAsync()
    {
        await WaitForInternalHandlersAsync(ClusterSide.Hub);
        await WaitForInternalHandlersAsync(ClusterSide.Sink);
    }

    private async Task WaitForInternalHandlersAsync(ClusterSide side)
    {
        var nodes = NodesOf(side);
        foreach (var node in nodes)
        {
            var ready = await WaitForValueAsync(
                () => Database(side, node).ReplicationLoader.OutgoingHandlers
                    .Count(x => x.Destination is InternalReplication) >= nodes.Count - 1,
                expectedVal: true,
                timeout: (int)DefaultReplicationWaitTimeout.TotalMilliseconds,
                interval: 100);

            Assert.True(ready, $"Expected internal outgoing replication handlers to be ready on {NodeTag(side, node)}.");
        }
    }

    private static string GetFilteredRoundTripPath(ClusterSide side)
    {
        var nodeB = NodeTagLower(side, LabNode.B);
        var viaNodeA = NodeTagLower(GetOppositeSide(side), LabNode.A);
        var nodeA = NodeTagLower(side, LabNode.A);
        return $"tickets/filtered-pass/{nodeB}-via-{viaNodeA}-to-{nodeA}";
    }

    private static string GetFilteredOutUserDocumentId(ClusterSide side, string itemName)
    {
        var nodeB = NodeTagLower(side, LabNode.B);
        var viaNodeA = NodeTagLower(GetOppositeSide(side), LabNode.A);
        var nodeA = NodeTagLower(side, LabNode.A);
        return $"users/filtered-out-gap/{nodeB}-via-{viaNodeA}-to-{nodeA}/{itemName}";
    }

    public async Task ConfigurePerNodeHubDefinitionsAsync()
    {
        foreach (var node in NodesOf(ClusterSide.Hub))
        {
            var hubDefinition = new PullReplicationDefinition
            {
                Name = HubName(node),
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                MentorNode = NodeTag(ClusterSide.Hub, node),
                PinToMentorNode = true
            };

            await Store(ClusterSide.Hub, node).Maintenance.SendAsync(new PutPullReplicationAsHubOperation(hubDefinition));
            await Store(ClusterSide.Hub, node).Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(
                HubName(node),
                new ReplicationHubAccess
                {
                    Name = HubAccessName,
                    CertificateBase64 = Convert.ToBase64String(
                        HubCluster.Certificates.ClientCertificate2.Value.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = ["tickets/*"],
                    AllowedHubToSinkPaths = ["tickets/*"]
                }));
        }
    }

    private RavenServer Server(ClusterSide side, LabNode node) => _servers[Key(side, node)];

    public RavenServer GetServer(ClusterSide side, LabNode node) => Server(side, node);

    private DocumentDatabase Database(ClusterSide side, LabNode node) => _databases[Key(side, node)];

    public string GetDatabaseIdFor(ClusterSide side, LabNode node) => Database(side, node).DbBase64Id;

    public string GetDatabaseIdFor(LabNode node) => GetDatabaseIdFor(RequiredFilteredPassReceiveSide, node);

    public string GetDatabaseChangeVector(ClusterSide side, LabNode node)
    {
        using (Database(side, node).DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return DocumentsStorage.GetDatabaseChangeVector(context).AsString();
        }
    }

    public string GetDatabaseChangeVector(LabNode node) => GetDatabaseChangeVector(RequiredFilteredPassReceiveSide, node);

    public DocumentSnapshot GetFilteredRoundTripDocument(LabNode node) =>
        GetDocumentById(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public DocumentSnapshot GetAllowedTicketBeforeFilteredOutUser(LabNode node) =>
        GetDocumentById(RequiredFilteredPassReceiveSide, node, RequiredAllowedTicketBeforeFilteredOutUserId);

    public DocumentSnapshot GetFilteredOutUser(LabNode node) =>
        GetDocumentById(RequiredFilteredPassReceiveSide, node, RequiredFilteredOutUserDocumentId);

    public List<ConflictSnapshot> GetFilteredRoundTripConflicts(LabNode node) =>
        GetConflicts(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public RevisionSnapshot GetFilteredRoundTripLatestRevision(LabNode node) =>
        GetLatestRevision(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public List<RevisionSnapshot> GetFilteredRoundTripRevisions(LabNode node) =>
        GetRevisions(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public List<RevisionTombstoneSnapshot> GetFilteredRoundTripRevisionTombstones(LabNode node) =>
        GetRevisionTombstones(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public TombstoneSnapshot GetFilteredRoundTripDocumentTombstone(LabNode node) =>
        GetDocumentTombstoneById(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId);

    public CounterSnapshot GetFilteredRoundTripCounter(LabNode node, string counterName) =>
        GetCounter(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, counterName);

    public AttachmentSnapshot GetFilteredRoundTripAttachment(LabNode node, string attachmentName) =>
        GetAttachment(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, attachmentName);

    public AttachmentTombstoneSnapshot GetFilteredRoundTripAttachmentTombstone(LabNode node, string attachmentName, string hash, string contentType) =>
        GetAttachmentTombstone(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, attachmentName, hash, contentType);

    public TimeSeriesSegmentSnapshot GetFilteredRoundTripTimeSeriesSegment(LabNode node, string timeSeriesName) =>
        GetTimeSeriesSegment(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, timeSeriesName);

    public int GetFilteredRoundTripTimeSeriesValueCount(LabNode node, string timeSeriesName, DateTime from, DateTime to) =>
        GetTimeSeriesValueCount(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, timeSeriesName, from, to);

    public TimeSeriesDeletedRangeSnapshot GetFilteredRoundTripTimeSeriesDeletedRange(LabNode node, string timeSeriesName) =>
        GetTimeSeriesDeletedRange(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, timeSeriesName);

    private DocumentSnapshot GetDocumentById(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (var document = database.DocumentsStorage.Get(context, documentId))
        {
            if (document == null)
            {
                return new DocumentSnapshot
                {
                    Exists = false
                };
            }

            document.Data.TryGet(nameof(Ticket.Name), out string name);

            return new DocumentSnapshot
            {
                Exists = true,
                Name = name,
                ChangeVector = document.ChangeVector
            };
        }
    }

    private List<ConflictSnapshot> GetConflicts(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var conflicts = database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, documentId);
            try
            {
                var result = new List<ConflictSnapshot>();
                foreach (var conflict in conflicts)
                {
                    string name = null;
                    conflict.Doc?.TryGet(nameof(Ticket.Name), out name);

                    result.Add(new ConflictSnapshot
                    {
                        Name = name,
                        ChangeVector = conflict.ChangeVector,
                        Etag = conflict.Etag
                    });
                }

                return result;
            }
            finally
            {
                foreach (var conflict in conflicts)
                    conflict.Dispose();
            }
        }
    }

    private RevisionSnapshot GetLatestRevision(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var revisionsResult = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, documentId, 0, 1);
            try
            {
                var revision = revisionsResult.Revisions.FirstOrDefault();
                if (revision == null)
                {
                    return new RevisionSnapshot
                    {
                        Exists = false,
                        Count = revisionsResult.Count
                    };
                }

                revision.Data.TryGet(nameof(Ticket.Name), out string name);

                return new RevisionSnapshot
                {
                    Exists = true,
                    Name = name,
                    ChangeVector = revision.ChangeVector,
                    Count = revisionsResult.Count,
                    Etag = revision.Etag
                };
            }
            finally
            {
                foreach (var revision in revisionsResult.Revisions)
                    revision?.Dispose();
            }
        }
    }

    private RevisionSnapshot GetRevision(ClusterSide side, LabNode node, string documentId, string changeVector)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var revisionsResult = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, documentId, 0, int.MaxValue);
            try
            {
                foreach (var revision in revisionsResult.Revisions)
                {
                    if (string.Equals(revision.ChangeVector, changeVector, StringComparison.Ordinal) == false)
                        continue;

                    revision.Data.TryGet(nameof(Ticket.Name), out string name);

                    return new RevisionSnapshot
                    {
                        Exists = true,
                        Name = name,
                        ChangeVector = revision.ChangeVector,
                        Count = revisionsResult.Count,
                        Etag = revision.Etag
                    };
                }

                return new RevisionSnapshot
                {
                    Exists = false,
                    Count = revisionsResult.Count
                };
            }
            finally
            {
                foreach (var revision in revisionsResult.Revisions)
                    revision?.Dispose();
            }
        }
    }

    private List<RevisionSnapshot> GetRevisions(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var revisionsResult = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, documentId, 0, int.MaxValue);
            try
            {
                var result = new List<RevisionSnapshot>();
                foreach (var revision in revisionsResult.Revisions)
                {
                    revision.Data.TryGet(nameof(Ticket.Name), out string name);

                    result.Add(new RevisionSnapshot
                    {
                        Exists = true,
                        Name = name,
                        ChangeVector = revision.ChangeVector,
                        Count = revisionsResult.Count,
                        Etag = revision.Etag
                    });
                }

                return result;
            }
            finally
            {
                foreach (var revision in revisionsResult.Revisions)
                    revision?.Dispose();
            }
        }
    }

    private List<RevisionTombstoneSnapshot> GetRevisionTombstones(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var result = new List<RevisionTombstoneSnapshot>();
            foreach (var tombstone in database.DocumentsStorage.GetTombstonesFrom(
                         context,
                         Raven.Server.Documents.Schemas.Revisions.RevisionsTombstones,
                         etag: 0,
                         start: 0,
                         take: long.MaxValue))
            {
                var tombstoneItem = TombstoneReplicationItem.From(context, tombstone);
                if (tombstoneItem is not RevisionTombstoneReplicationItem revisionTombstone)
                {
                    tombstoneItem.Dispose();
                    continue;
                }

                using (revisionTombstone)
                {
                    if (RevisionsStorage.TryExtractDocumentIdFromRevisionTombstoneKey(revisionTombstone.Id, out var tombstoneDocumentId) == false)
                        continue;

                    if (string.Equals(tombstoneDocumentId, documentId, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    result.Add(new RevisionTombstoneSnapshot
                    {
                        RawKey = revisionTombstone.Id.ToString(CultureInfo.InvariantCulture),
                        KeyChangeVector = null,
                        ChangeVector = revisionTombstone.ChangeVector,
                        Etag = revisionTombstone.Etag
                    });
                }
            }

            return result;
        }
    }

    public TombstoneSnapshot GetDocumentTombstoneById(ClusterSide side, LabNode node, string documentId)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var documentOrTombstone = database.DocumentsStorage.GetDocumentOrTombstone(context, documentId, throwOnConflict: false);
            try
            {
                if (documentOrTombstone.Tombstone == null)
                {
                    return new TombstoneSnapshot
                    {
                        Exists = false
                    };
                }

                return new TombstoneSnapshot
                {
                    Exists = true,
                    ChangeVector = documentOrTombstone.Tombstone.ChangeVector
                };
            }
            finally
            {
                documentOrTombstone.Document?.Dispose();
                documentOrTombstone.Tombstone?.Dispose();
            }
        }
    }

    private CounterSnapshot GetCounter(ClusterSide side, LabNode node, string documentId, string counterName)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var counter = database.DocumentsStorage.CountersStorage.GetCounterValue(context, documentId, counterName);
            if (counter == null)
            {
                return new CounterSnapshot
                {
                    Exists = false
                };
            }

            string changeVector = null;
            foreach (var group in database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(context, documentId))
            {
                var containsCounter = database.DocumentsStorage.CountersStorage
                    .GetCountersFromCounterGroup(group)
                    .Any(x => string.Equals(x.Name, counterName, StringComparison.OrdinalIgnoreCase));

                if (containsCounter == false)
                    continue;

                changeVector = group.ChangeVector;
                break;
            }

            return new CounterSnapshot
            {
                Exists = true,
                Value = counter.Value.Value,
                ChangeVector = changeVector
            };
        }
    }

    private AttachmentSnapshot GetAttachment(ClusterSide side, LabNode node, string documentId, string attachmentName)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (DocumentIdWorker.GetLoweredIdSliceFromId(context, documentId, out Slice lowerDocumentId))
        {
            var attachmentDetails = database.DocumentsStorage.AttachmentsStorage
                .GetAttachmentDetailsForDocument(context, lowerDocumentId)
                .FirstOrDefault(x => string.Equals(x.Name, attachmentName, StringComparison.Ordinal));

            if (attachmentDetails == null)
            {
                return new AttachmentSnapshot
                {
                    Exists = false
                };
            }

            var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, attachmentName, AttachmentType.Document, changeVector: null);
            if (attachment == null)
            {
                return new AttachmentSnapshot
                {
                    Exists = false
                };
            }

            return new AttachmentSnapshot
            {
                Exists = true,
                ChangeVector = attachmentDetails.ChangeVector,
                Hash = attachmentDetails.Hash,
                ContentType = attachmentDetails.ContentType,
                Size = attachment.Size
            };
        }
    }

    private unsafe AttachmentTombstoneSnapshot GetAttachmentTombstone(
        ClusterSide side,
        LabNode node,
        string documentId,
        string attachmentName,
        string hash,
        string contentType)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (DocumentIdWorker.GetLoweredIdSliceFromId(context, documentId, out Slice lowerDocumentId))
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
            var tombstone = database.DocumentsStorage.AttachmentsStorage.GetAttachmentTombstoneByKey(context, keySlice);
            if (tombstone == null)
            {
                return new AttachmentTombstoneSnapshot
                {
                    Exists = false
                };
            }

            using (tombstone)
            {
                return new AttachmentTombstoneSnapshot
                {
                    Exists = true,
                    ChangeVector = tombstone.ChangeVector
                };
            }
        }
    }

    private TimeSeriesSegmentSnapshot GetTimeSeriesSegment(ClusterSide side, LabNode node, string documentId, string timeSeriesName)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var valueCount = database.DocumentsStorage.TimeSeriesStorage
                .GetReader(context, documentId, timeSeriesName, DateTime.MinValue, DateTime.MaxValue)
                .AllValues()
                .Count();

            TimeSeriesSegmentSnapshot latest = null;
            foreach (var segment in database.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, etag: 0, includeDocumentChangeVector: false))
            {
                using (segment)
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(segment.Key, context, out var segmentDocumentId, out var segmentName);

                    if (string.Equals(segmentDocumentId?.ToString(CultureInfo.InvariantCulture), documentId, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (string.Equals(segmentName?.ToString(CultureInfo.InvariantCulture), timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (latest != null && latest.Etag >= segment.Etag)
                        continue;

                    latest = new TimeSeriesSegmentSnapshot
                    {
                        Exists = true,
                        ChangeVector = segment.ChangeVector,
                        Etag = segment.Etag,
                        ValueCount = valueCount
                    };
                }
            }

            return latest ?? new TimeSeriesSegmentSnapshot
            {
                Exists = false,
                ValueCount = valueCount
            };
        }
    }

    private int GetTimeSeriesValueCount(ClusterSide side, LabNode node, string documentId, string timeSeriesName, DateTime from, DateTime to)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return database.DocumentsStorage.TimeSeriesStorage
                .GetReader(context, documentId, timeSeriesName, from, to)
                .AllValues()
                .Count();
        }
    }

    private TimeSeriesDeletedRangeSnapshot GetTimeSeriesDeletedRange(ClusterSide side, LabNode node, string documentId, string timeSeriesName)
    {
        var ranges = GetTimeSeriesDeletedRanges(side, node, documentId, timeSeriesName);
        return ranges.Count == 0
            ? new TimeSeriesDeletedRangeSnapshot { Exists = false }
            : ranges.OrderByDescending(x => x.Etag).First();
    }

    private List<TimeSeriesDeletedRangeSnapshot> GetTimeSeriesDeletedRanges(ClusterSide side, LabNode node, string documentId, string timeSeriesName)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var ranges = new List<TimeSeriesDeletedRangeSnapshot>();
            foreach (var deletedRange in database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesForDoc(context, documentId))
            {
                using (deletedRange)
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(deletedRange.Key, context, out _, out var currentName);

                    if (string.Equals(currentName?.ToString(CultureInfo.InvariantCulture), timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    ranges.Add(new TimeSeriesDeletedRangeSnapshot
                    {
                        Exists = true,
                        ChangeVector = deletedRange.ChangeVector,
                        From = deletedRange.From,
                        To = deletedRange.To,
                        Etag = deletedRange.Etag
                    });
                }
            }

            return ranges;
        }
    }

    private static string FormatTimeSeriesDeletedRanges(List<TimeSeriesDeletedRangeSnapshot> ranges)
    {
        if (ranges.Count == 0)
            return "<none>";

        return string.Join("; ", ranges
            .OrderBy(x => x.From)
            .ThenBy(x => x.To)
            .ThenBy(x => x.Etag)
            .Select(x => $"etag={x.Etag}, from='{x.From:O}', to='{x.To:O}', CV='{x.ChangeVector ?? "<null>"}'"));
    }

    public Task StoreFilteredRoundTripTicketAsync(LabNode node) =>
        StoreTicketAsync(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);

    public Task StoreFilteredRoundTripTicketWithNameAsync(LabNode node, string name) =>
        StoreTicketAsync(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, name);

    public async Task StoreAllowedTicketThenFilteredOutUserAsync(LabNode node)
    {
        await StoreTicketAsync(RequiredFilteredPassReceiveSide, node, RequiredAllowedTicketBeforeFilteredOutUserId, "filtered-pass-before-" + RequiredFilteredRoundTripItemName);
        await StoreUserAsync(RequiredFilteredPassReceiveSide, node, RequiredFilteredOutUserDocumentId, "filtered-out-gap-" + RequiredFilteredRoundTripItemName);
        _allowedTicketThenFilteredOutUserStored = true;
    }

    public Task StoreFilteredOutInternalReplicationMarkerAsync(LabNode node, string markerName) =>
        StoreUserAsync(RequiredFilteredPassReceiveSide, node, FilteredOutInternalReplicationMarkerId(markerName), markerName);

    public Task WaitForFilteredOutInternalReplicationMarkerAsync(LabNode node, string markerName, TimeSpan? timeout = null) =>
        WaitForDocumentNameByIdAsync(RequiredFilteredPassReceiveSide, node, FilteredOutInternalReplicationMarkerId(markerName), markerName, timeout);

    private string FilteredOutInternalReplicationMarkerId(string markerName) =>
        $"{RequiredFilteredOutUserDocumentId}/internal-replication-marker/{markerName}";

    public async Task StoreTicketAsync(ClusterSide side, LabNode node, string documentId, string name)
    {
        using var session = Store(side, node).OpenAsyncSession();
        await session.StoreAsync(new Ticket { Name = name }, documentId);
        await session.SaveChangesAsync();
    }

    public async Task DeleteDocumentAsync(ClusterSide side, LabNode node, string documentId)
    {
        using var session = Store(side, node).OpenAsyncSession();
        session.Delete(documentId);
        await session.SaveChangesAsync();
    }

    public Task WaitForTicketAsync(ClusterSide side, LabNode node, string documentId, string expectedName, TimeSpan? timeout = null) =>
        WaitForDocumentNameByIdAsync(side, node, documentId, expectedName, timeout);

    public long GetNumberOfDocumentTombstones(ClusterSide side, LabNode node)
    {
        var database = Database(side, node);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
            return database.DocumentsStorage.GetNumberOfTombstones(context);
    }

    public Task RunTombstoneCleanupAsync(ClusterSide side, LabNode node) =>
        Database(side, node).TombstoneCleaner.ExecuteCleanup();

    private async Task StoreUserAsync(ClusterSide side, LabNode node, string documentId, string name)
    {
        using var session = Store(side, node).OpenAsyncSession();
        await session.StoreAsync(new User { Name = name }, documentId);
        await session.SaveChangesAsync();
    }

    public async Task EnableRevisionsAsync(ClusterSide side)
    {
        await Store(side, LabNode.A).Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 10
            }
        }));
    }

    public async Task ForceFilteredRoundTripRevisionAsync(LabNode node)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.Advanced.Revisions.ForceRevisionCreationFor(RequiredFilteredRoundTripTicketId);
        await session.SaveChangesAsync();
    }

    public Task RevertFilteredRoundTripDocumentToRevisionAsync(LabNode node, string revisionChangeVector) =>
        Store(RequiredFilteredPassReceiveSide, node).Operations.SendAsync(new RevertRevisionsByIdOperation(RequiredFilteredRoundTripTicketId, revisionChangeVector));

    public Task DeleteFilteredRoundTripRevisionAsync(LabNode node, string revisionChangeVector) =>
        Store(RequiredFilteredPassReceiveSide, node).Maintenance.SendAsync(new DeleteRevisionsOperation(RequiredFilteredRoundTripTicketId, [revisionChangeVector]));

    public async Task<long> GetFilteredRoundTripRevisionCountFromClientAsync(LabNode node)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        return await session.Advanced.Revisions.GetCountForAsync(RequiredFilteredRoundTripTicketId);
    }

    public async Task<List<string>> GetFilteredRoundTripRevisionNamesFromClientAsync(LabNode node)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        var revisions = await session.Advanced.Revisions.GetForAsync<Ticket>(RequiredFilteredRoundTripTicketId, start: 0, pageSize: 64);
        return revisions.Select(ticket => ticket?.Name).ToList();
    }

    public async Task DeleteFilteredRoundTripDocumentAsync(LabNode node)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.Delete(RequiredFilteredRoundTripTicketId);
        await session.SaveChangesAsync();
    }

    public async Task IncrementFilteredRoundTripCounterAsync(LabNode node, string counterName, long delta = 1)
    {
        using (var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession())
        {
            session.CountersFor(RequiredFilteredRoundTripTicketId).Increment(counterName, delta);
            await session.SaveChangesAsync();
        }
    }

    public async Task DeleteFilteredRoundTripCounterAsync(LabNode node, string counterName)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.CountersFor(RequiredFilteredRoundTripTicketId).Delete(counterName);
        await session.SaveChangesAsync();
    }

    public async Task PutFilteredRoundTripAttachmentAsync(LabNode node, string attachmentName, byte[] content, string contentType)
    {
        using var stream = new MemoryStream(content);
        await Store(RequiredFilteredPassReceiveSide, node).Operations.SendAsync(new PutAttachmentOperation(RequiredFilteredRoundTripTicketId, attachmentName, stream, contentType));
    }

    public async Task DeleteFilteredRoundTripAttachmentAsync(LabNode node, string attachmentName)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.Advanced.Attachments.Delete(RequiredFilteredRoundTripTicketId, attachmentName);
        await session.SaveChangesAsync();
    }

    public async Task AppendFilteredRoundTripTimeSeriesAsync(LabNode node, string timeSeriesName, DateTime timestamp, double value)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.TimeSeriesFor(RequiredFilteredRoundTripTicketId, timeSeriesName).Append(timestamp, value, "watches/fitbit");
        await session.SaveChangesAsync();
    }

    public async Task DeleteFilteredRoundTripTimeSeriesRangeAsync(LabNode node, string timeSeriesName, DateTime from, DateTime to)
    {
        using var session = Store(RequiredFilteredPassReceiveSide, node).OpenAsyncSession();
        session.TimeSeriesFor(RequiredFilteredRoundTripTicketId, timeSeriesName).Delete(from, to);
        await session.SaveChangesAsync();
    }

    public Task WaitForFilteredRoundTripDocumentNameAsync(LabNode node, string expectedName, TimeSpan? timeout = null) =>
        WaitForDocumentNameByIdAsync(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, expectedName, timeout);

    public Task WaitForFilteredRoundTripDocumentNameAsync(LabNode node, TimeSpan? timeout = null) =>
        WaitForDocumentNameByIdAsync(RequiredFilteredPassReceiveSide, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName, timeout);

    public async Task WaitForFilteredRoundTripDocumentNameOrConflictAsync(LabNode node, string expectedName, TimeSpan? timeout = null)
    {
        var side = RequiredFilteredPassReceiveSide;
        var reached = await WaitForValueAsync(
            () =>
            {
                if (GetConflicts(side, node, RequiredFilteredRoundTripTicketId).Count > 0)
                    return true;

                var current = GetDocumentById(side, node, RequiredFilteredRoundTripTicketId);
                return current.Exists && string.Equals(current.Name, expectedName, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var conflicts = GetConflicts(side, node, RequiredFilteredRoundTripTicketId);
        var document = conflicts.Count == 0
            ? GetDocumentById(side, node, RequiredFilteredRoundTripTicketId)
            : new DocumentSnapshot { Exists = false };

        Assert.True(
            reached,
            $"Expected document '{RequiredFilteredRoundTripTicketId}' with name '{expectedName}' or a conflict to reach {NodeTag(side, node)}. " +
            $"documentExists={document.Exists}, documentName='{document.Name ?? "<null>"}', documentCV='{document.ChangeVector ?? "<null>"}', " +
            $"conflicts='{FormatConflicts(conflicts)}'.");
    }

    private async Task WaitForDocumentNameByIdAsync(ClusterSide side, LabNode node, string documentId, string expectedName, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetDocumentById(side, node, documentId);
                return current.Exists && string.Equals(current.Name, expectedName, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected document '{documentId}' with name '{expectedName}' to reach {NodeTag(side, node)}.");
    }

    private async Task<List<ConflictSnapshot>> WaitForFilteredRoundTripConflictsAsync(
        ClusterSide side,
        LabNode node,
        int expectedCount,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () => GetConflicts(side, node, RequiredFilteredRoundTripTicketId).Count >= expectedCount,
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var conflicts = GetConflicts(side, node, RequiredFilteredRoundTripTicketId);

        Assert.True(
            reached,
            $"Expected {expectedCount} conflict(s) for '{RequiredFilteredRoundTripTicketId}' to reach {NodeTag(side, node)}. " +
            $"actualCount={conflicts.Count}, conflicts='{FormatConflicts(conflicts)}'.");

        return conflicts;
    }

    public Task<List<ConflictSnapshot>> WaitForFilteredRoundTripConflictsAsync(LabNode node, int expectedCount, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripConflictsAsync(RequiredFilteredPassReceiveSide, node, expectedCount, timeout);

    private async Task WaitForFilteredRoundTripRevisionAsync(
        ClusterSide side,
        LabNode node,
        string expectedChangeVector,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetRevision(side, node, RequiredFilteredRoundTripTicketId, expectedChangeVector);
                return current.Exists && string.Equals(current.ChangeVector, expectedChangeVector, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var revision = GetRevision(side, node, RequiredFilteredRoundTripTicketId, expectedChangeVector);
        var latestRevision = GetLatestRevision(side, node, RequiredFilteredRoundTripTicketId);

        Assert.True(
            reached,
            $"Expected revision '{RequiredFilteredRoundTripTicketId}' with CV '{expectedChangeVector}' to reach {NodeTag(side, node)}. " +
            $"found={revision.Exists}, revisionsCount={revision.Count}, latestExists={latestRevision.Exists}, latestCV='{latestRevision.ChangeVector ?? "<null>"}', " +
            $"latestName='{latestRevision.Name ?? "<null>"}', latestCount={latestRevision.Count}.");
    }

    public Task WaitForFilteredRoundTripRevisionAsync(LabNode node, string expectedChangeVector, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripRevisionAsync(RequiredFilteredPassReceiveSide, node, expectedChangeVector, timeout);

    private async Task WaitForFilteredRoundTripLatestRevisionNameAsync(
        ClusterSide side,
        LabNode node,
        string expectedName,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetLatestRevision(side, node, RequiredFilteredRoundTripTicketId);
                return current.Exists && string.Equals(current.Name, expectedName, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var revision = GetLatestRevision(side, node, RequiredFilteredRoundTripTicketId);

        Assert.True(
            reached,
            $"Expected latest revision '{RequiredFilteredRoundTripTicketId}' with name '{expectedName}' to reach {NodeTag(side, node)}. " +
            $"latestExists={revision.Exists}, latestName='{revision.Name ?? "<null>"}', latestCV='{revision.ChangeVector ?? "<null>"}', latestCount={revision.Count}.");
    }

    public Task WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode node, string expectedName, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripLatestRevisionNameAsync(RequiredFilteredPassReceiveSide, node, expectedName, timeout);

    private async Task WaitForFilteredRoundTripRevisionTombstonesAsync(
        ClusterSide side,
        LabNode node,
        int expectedCount = 1,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () => GetRevisionTombstones(side, node, RequiredFilteredRoundTripTicketId).Count >= expectedCount,
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var tombstones = GetRevisionTombstones(side, node, RequiredFilteredRoundTripTicketId);

        Assert.True(
            reached,
            $"Expected {expectedCount} revision tombstone(s) for '{RequiredFilteredRoundTripTicketId}' to reach {NodeTag(side, node)}. " +
            $"actualCount={tombstones.Count}, tombstones='{FormatRevisionTombstones(tombstones)}'.");
    }

    private async Task WaitForFilteredRoundTripDocumentTombstoneAsync(ClusterSide side, LabNode node, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () => GetDocumentTombstoneById(side, node, RequiredFilteredRoundTripTicketId).Exists,
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected document tombstone '{RequiredFilteredRoundTripTicketId}' to reach {NodeTag(side, node)}.");
    }

    private async Task WaitForFilteredRoundTripCounterAsync(ClusterSide side, LabNode node, string counterName, long expectedValue, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetCounter(side, node, RequiredFilteredRoundTripTicketId, counterName);
                return current.Exists && current.Value == expectedValue;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected counter '{counterName}' on '{RequiredFilteredRoundTripTicketId}' with value {expectedValue} to reach {NodeTag(side, node)}.");
    }

    public Task WaitForFilteredRoundTripCounterAsync(LabNode node, string counterName, long expectedValue, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripCounterAsync(RequiredFilteredPassReceiveSide, node, counterName, expectedValue, timeout);

    private async Task WaitForFilteredRoundTripCounterDeletionAsync(ClusterSide side, LabNode node, string counterName, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () => GetCounter(side, node, RequiredFilteredRoundTripTicketId, counterName).Exists == false,
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var counter = GetCounter(side, node, RequiredFilteredRoundTripTicketId, counterName);

        Assert.True(
            reached,
            $"Expected counter '{counterName}' on '{RequiredFilteredRoundTripTicketId}' to be deleted on {NodeTag(side, node)}. " +
            $"exists={counter.Exists}, value={counter.Value}, CV='{counter.ChangeVector ?? "<null>"}'.");
    }

    public Task WaitForFilteredRoundTripCounterDeletionAsync(LabNode node, string counterName, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripCounterDeletionAsync(RequiredFilteredPassReceiveSide, node, counterName, timeout);

    public async Task WaitForFilteredRoundTripCounterOrConflictAsync(LabNode node, string counterName, long expectedValue, TimeSpan? timeout = null)
    {
        var side = RequiredFilteredPassReceiveSide;
        var reached = await WaitForValueAsync(
            () =>
            {
                if (GetConflicts(side, node, RequiredFilteredRoundTripTicketId).Count > 0)
                    return true;

                var current = GetCounter(side, node, RequiredFilteredRoundTripTicketId, counterName);
                return current.Exists && current.Value == expectedValue;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var conflicts = GetConflicts(side, node, RequiredFilteredRoundTripTicketId);
        var counter = GetCounter(side, node, RequiredFilteredRoundTripTicketId, counterName);

        Assert.True(
            reached,
            $"Expected counter '{counterName}' on '{RequiredFilteredRoundTripTicketId}' with value {expectedValue} or a conflict to reach {NodeTag(side, node)}. " +
            $"counterExists={counter.Exists}, counterValue={counter.Value}, counterCV='{counter.ChangeVector ?? "<null>"}', conflicts='{FormatConflicts(conflicts)}'.");
    }

    private async Task WaitForFilteredRoundTripAttachmentAsync(
        ClusterSide side,
        LabNode node,
        string attachmentName,
        string expectedHash,
        long expectedSize,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetAttachment(side, node, RequiredFilteredRoundTripTicketId, attachmentName);
                return current.Exists &&
                       string.Equals(current.Hash, expectedHash, StringComparison.Ordinal) &&
                       current.Size == expectedSize;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected attachment '{attachmentName}' on '{RequiredFilteredRoundTripTicketId}' with hash '{expectedHash}' and size {expectedSize} to reach {NodeTag(side, node)}.");
    }

    public Task WaitForFilteredRoundTripAttachmentAsync(LabNode node, string attachmentName, string expectedHash, long expectedSize, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripAttachmentAsync(RequiredFilteredPassReceiveSide, node, attachmentName, expectedHash, expectedSize, timeout);

    public async Task WaitForFilteredRoundTripAttachmentOrConflictAsync(
        LabNode node,
        string attachmentName,
        string expectedHash,
        long expectedSize,
        TimeSpan? timeout = null)
    {
        var side = RequiredFilteredPassReceiveSide;
        var reached = await WaitForValueAsync(
            () =>
            {
                if (GetConflicts(side, node, RequiredFilteredRoundTripTicketId).Count > 0)
                    return true;

                var current = GetAttachment(side, node, RequiredFilteredRoundTripTicketId, attachmentName);
                return current.Exists &&
                       string.Equals(current.Hash, expectedHash, StringComparison.Ordinal) &&
                       current.Size == expectedSize;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var conflicts = GetConflicts(side, node, RequiredFilteredRoundTripTicketId);
        var attachment = GetAttachment(side, node, RequiredFilteredRoundTripTicketId, attachmentName);

        Assert.True(
            reached,
            $"Expected attachment '{attachmentName}' on '{RequiredFilteredRoundTripTicketId}' with hash '{expectedHash}' and size {expectedSize} or a conflict to reach {NodeTag(side, node)}. " +
            $"attachmentExists={attachment.Exists}, attachmentHash='{attachment.Hash ?? "<null>"}', attachmentSize={attachment.Size}, " +
            $"attachmentCV='{attachment.ChangeVector ?? "<null>"}', conflicts='{FormatConflicts(conflicts)}'.");
    }

    private async Task WaitForFilteredRoundTripAttachmentTombstoneAsync(
        ClusterSide side,
        LabNode node,
        string attachmentName,
        string hash,
        string contentType,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () => GetAttachmentTombstone(side, node, RequiredFilteredRoundTripTicketId, attachmentName, hash, contentType).Exists,
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected attachment tombstone '{attachmentName}' on '{RequiredFilteredRoundTripTicketId}' to reach {NodeTag(side, node)}.");
    }

    private async Task WaitForFilteredRoundTripTimeSeriesSegmentAsync(ClusterSide side, LabNode node, string timeSeriesName, int expectedValueCount, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetTimeSeriesSegment(side, node, RequiredFilteredRoundTripTicketId, timeSeriesName);
                return current.Exists && current.ValueCount >= expectedValueCount;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected time series '{timeSeriesName}' on '{RequiredFilteredRoundTripTicketId}' with at least {expectedValueCount} values to reach {NodeTag(side, node)}.");
    }

    public Task WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode node, string timeSeriesName, int expectedValueCount, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripTimeSeriesSegmentAsync(RequiredFilteredPassReceiveSide, node, timeSeriesName, expectedValueCount, timeout);

    private async Task WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(ClusterSide side, LabNode node, string timeSeriesName, TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var current = GetTimeSeriesDeletedRange(side, node, RequiredFilteredRoundTripTicketId, timeSeriesName);
                return current.Exists;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        Assert.True(reached, $"Expected deleted time series range '{timeSeriesName}' on '{RequiredFilteredRoundTripTicketId}' to reach {NodeTag(side, node)}.");
    }

    public Task WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(LabNode node, string timeSeriesName, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(RequiredFilteredPassReceiveSide, node, timeSeriesName, timeout);

    private async Task WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(
        ClusterSide side,
        LabNode node,
        string timeSeriesName,
        DateTime expectedFrom,
        DateTime expectedTo,
        TimeSpan? timeout = null)
    {
        var reached = await WaitForValueAsync(
            () =>
            {
                var ranges = GetTimeSeriesDeletedRanges(side, node, RequiredFilteredRoundTripTicketId, timeSeriesName);
                return ranges.Any(x => x.From <= expectedFrom && x.To >= expectedTo);
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var deletedRanges = GetTimeSeriesDeletedRanges(side, node, RequiredFilteredRoundTripTicketId, timeSeriesName);

        Assert.True(
            reached,
            $"Expected deleted time series range '{timeSeriesName}' on '{RequiredFilteredRoundTripTicketId}' covering '{expectedFrom:O}'..'{expectedTo:O}' to reach {NodeTag(side, node)}. " +
            $"actualRanges={FormatTimeSeriesDeletedRanges(deletedRanges)}.");
    }

    public Task WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(LabNode node, string timeSeriesName, DateTime expectedFrom, DateTime expectedTo, TimeSpan? timeout = null) =>
        WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(RequiredFilteredPassReceiveSide, node, timeSeriesName, expectedFrom, expectedTo, timeout);

    public void ExpectPassedConflicts(int expectedCount = 2)
    {
        _waitForExpectedFilteredRoundTripItemAsync = (side, node) => WaitForFilteredRoundTripConflictsAsync(side, node, expectedCount);
    }

    public void ExpectPassedDocumentTombstone()
    {
        _waitForExpectedFilteredRoundTripItemAsync = (side, node) => WaitForFilteredRoundTripDocumentTombstoneAsync(side, node);
    }

    public void ExpectPassedLatestRevisionName(string expectedName)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, expectedName);
            await WaitForFilteredRoundTripLatestRevisionNameAsync(side, node, expectedName);
        };
    }

    public void ExpectPassedRevisionTombstone(string expectedDocumentName, int expectedCount = 1)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, expectedDocumentName);
            await WaitForFilteredRoundTripRevisionTombstonesAsync(side, node, expectedCount);
        };
    }

    public void ExpectPassedCounter(string counterName, long expectedValue)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripCounterAsync(side, node, counterName, expectedValue);
        };
    }

    public void ExpectPassedCounterDeletion(string deletedCounterName, string retainedCounterName, long retainedCounterValue)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripCounterDeletionAsync(side, node, deletedCounterName);
            await WaitForFilteredRoundTripCounterAsync(side, node, retainedCounterName, retainedCounterValue);
        };
    }

    public void ExpectPassedAttachment(string attachmentName, string expectedHash, long expectedSize)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripAttachmentAsync(side, node, attachmentName, expectedHash, expectedSize);
        };
    }

    public void ExpectPassedAttachmentTombstone(string attachmentName, string hash, string contentType)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripAttachmentTombstoneAsync(side, node, attachmentName, hash, contentType);
        };
    }

    public void ExpectPassedTimeSeriesSegment(string timeSeriesName, int expectedValueCount)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripTimeSeriesSegmentAsync(side, node, timeSeriesName, expectedValueCount);
        };
    }

    public void ExpectPassedTimeSeriesDeletedRange(string timeSeriesName)
    {
        _waitForExpectedFilteredRoundTripItemAsync = async (side, node) =>
        {
            await WaitForDocumentNameByIdAsync(side, node, RequiredFilteredRoundTripTicketId, RequiredFilteredRoundTripItemName);
            await WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(side, node, timeSeriesName);
        };
    }

    public Task WaitForExpectedFilteredRoundTripItemAsync(ClusterSide side, LabNode node) =>
        _waitForExpectedFilteredRoundTripItemAsync != null
            ? _waitForExpectedFilteredRoundTripItemAsync(side, node)
            : throw new InvalidOperationException($"{nameof(WaitForExpectedFilteredRoundTripItemAsync)} is available only when the lab owns a filtered round-trip ticket id.");

    public Task WaitForExpectedFilteredRoundTripItemAsync(LabNode node) =>
        WaitForExpectedFilteredRoundTripItemAsync(RequiredFilteredPassReceiveSide, node);

    private async Task<LogicalInternalReplicationBlocker> BlockInternalReplicationAsync(ClusterSide side, LabNode from, LabNode to)
    {
        var blocker = new LogicalInternalReplicationBlocker(
            Database(side, from),
            NodeTag(side, from),
            NodeTag(side, to));

        await blocker.AttachAsync();
        _internalReplicationBlockers.Add(blocker);
        return blocker;
    }

    public Task<LogicalInternalReplicationBlocker> BlockInternalReplicationAsync(LabNode from, LabNode to) =>
        BlockInternalReplicationAsync(RequiredFilteredPassReceiveSide, from, to);

    public FilteredRoundTripReplicationGate BlockInternalReplicationBeforeScan(LabNode from, LabNode to)
    {
        var fromNodeTag = NodeTag(RequiredFilteredPassReceiveSide, from);
        var toNodeTag = NodeTag(RequiredFilteredPassReceiveSide, to);
        var gate = new FilteredRoundTripReplicationGate(
            Database(RequiredFilteredPassReceiveSide, from),
            matches: handler => handler.Destination is InternalReplication internalReplication &&
                                string.Equals(internalReplication.NodeTag, toNodeTag, StringComparison.OrdinalIgnoreCase),
            description: $"{fromNodeTag}->{toNodeTag} internal replication before scan");

        gate.Attach();
        _testingHookScopes.Add(gate);
        return gate;
    }

    private async Task BlockInternalReplicationUntilBlockedAsync(ClusterSide side, LabNode from, params LabNode[] to)
    {
        var blockers = new List<LogicalInternalReplicationBlocker>(to.Length);

        foreach (var destination in to)
            blockers.Add(await BlockInternalReplicationAsync(side, from, destination));

        var markerId = InternalReplicationBlockerTicketId(side, from, to);
        await StoreTicketAsync(side, from, markerId, "blocker");

        foreach (var blocker in blockers)
            await blocker.WaitForBlockedAsync();
    }

    public Task BlockInternalReplicationUntilBlockedAsync(LabNode from, params LabNode[] to) =>
        BlockInternalReplicationUntilBlockedAsync(RequiredFilteredPassReceiveSide, from, to);

    public async Task ConfigureFilteredRoundTripReplicationAsync(ClusterSide? receiveSide, bool returnLegUnfiltered = false)
    {
        if (receiveSide == null)
            return;

        _filteredRoundTripReturnLegUnfiltered = returnLegUnfiltered;

        InstallFilteredRoundTripOwnershipHooks(receiveSide.Value);
        InstallFilteredRoundTripRemoteUrlHooks(receiveSide.Value);

        _filteredRoundTripFirstHopGate = CreateFilteredRoundTripFirstHopGate(receiveSide.Value);
        _filteredRoundTripReturnGate = CreateFilteredRoundTripReturnGate(receiveSide.Value);
        _filteredRoundTripFirstHopGate.Attach();
        _filteredRoundTripReturnGate.Attach();
        _testingHookScopes.Add(_filteredRoundTripFirstHopGate);
        _testingHookScopes.Add(_filteredRoundTripReturnGate);

        await CreateFilteredRoundTripHubTaskAsync();
        await CreateFilteredRoundTripSinkTaskAsync();
        await _filteredRoundTripFirstHopGate.WaitForBlockedAsync();
        await _filteredRoundTripReturnGate.WaitForBlockedAsync();
        _filteredRoundTripTaskConfigured = true;
    }

    public async Task PassThroughFilteredReplicationAsync()
    {
        if (_filteredRoundTripTaskConfigured == false)
            throw new InvalidOperationException($"{nameof(PassThroughFilteredReplicationAsync)} requires a lab created with a filtered pass receive side and item name.");

        if (_allowedTicketThenFilteredOutUserStored == false)
            throw new InvalidOperationException($"{nameof(PassThroughFilteredReplicationAsync)} requires {nameof(StoreAllowedTicketThenFilteredOutUserAsync)} to run before the filtered pass.");

        var viaSide = GetOppositeSide(RequiredFilteredPassReceiveSide);

        _filteredRoundTripFirstHopGate.Release();
        await WaitForExpectedFilteredRoundTripItemAsync(viaSide, LabNode.A);
        AssertFilteredOutUserDidNotReach(viaSide, LabNode.A);

        _filteredRoundTripReturnGate.Release();
        await WaitForExpectedFilteredRoundTripItemAsync(RequiredFilteredPassReceiveSide, LabNode.A);

        Assert.True(_internalReplicationBlockers.Count > 0, "Expected filtered pass scenario to have internal replication blockers.");
        foreach (var blocker in _internalReplicationBlockers)
            blocker.AssertStillBlocking();

        var nodeBTag = NodeTag(RequiredFilteredPassReceiveSide, LabNode.B);
        var nodeATag = NodeTag(RequiredFilteredPassReceiveSide, LabNode.A);
        var nodeBToNodeABlockers = _internalReplicationBlockers
            .Where(x => x.Matches(nodeBTag, nodeATag))
            .ToList();

        Assert.True(
            nodeBToNodeABlockers.Count == 1,
            $"Expected exactly one {nodeBTag}->{nodeATag} internal replication blocker before asserting that filtered-out user '{RequiredFilteredOutUserDocumentId}' did not reach {nodeATag}. " +
            $"Actual blocker count: {nodeBToNodeABlockers.Count}.");

        AssertFilteredOutUserDidNotReach(RequiredFilteredPassReceiveSide, LabNode.A);
    }

    public async Task WaitForFilteredRoundTripFirstHopHeartbeatAsync(TimeSpan? timeout = null)
    {
        var initialHandler = GetFilteredRoundTripFirstHopHandler();

        Assert.NotNull(initialHandler);

        var initialHeartbeatTicks = initialHandler.LastHeartbeatTicks;
        var observed = await WaitForValueAsync(
            () =>
            {
                var currentHandler = GetFilteredRoundTripFirstHopHandler();
                return currentHandler != null &&
                       currentHandler.IsConnectionDisposed == false &&
                       currentHandler.LastHeartbeatTicks > initialHeartbeatTicks;
            },
            expectedVal: true,
            timeout: (int)(timeout ?? DefaultReplicationWaitTimeout).TotalMilliseconds,
            interval: 100);

        var latestHandler = GetFilteredRoundTripFirstHopHandler();
        Assert.True(
            observed,
            $"Expected filtered first-hop pull handler '{GetFilteredRoundTripFirstHopDescription(RequiredFilteredPassReceiveSide)}' to send an idle heartbeat after the filtered pass. " +
            $"initialHeartbeatTicks={initialHeartbeatTicks}, latestHeartbeatTicks={latestHandler?.LastHeartbeatTicks.ToString(CultureInfo.InvariantCulture) ?? "<missing>"}, " +
            $"handlerDisposed={latestHandler?.IsConnectionDisposed.ToString() ?? "<missing>"}.");
    }

    private async Task CreateFilteredRoundTripHubTaskAsync()
    {
        var hubStore = Store(ClusterSide.Hub, LabNode.A);

        await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
        {
            Name = FilteredRoundTripHubName,
            Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
            WithFiltering = true,
            MentorNode = NodeTag(ClusterSide.Hub, LabNode.A),
            PinToMentorNode = true
        }));

        await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(
            FilteredRoundTripHubName,
            new ReplicationHubAccess
                {
                    Name = HubAccessName,
                    CertificateBase64 = Convert.ToBase64String(
                        HubCluster.Certificates.ClientCertificate2.Value.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = GetFilteredRoundTripAllowedSinkToHubPaths(),
                    AllowedHubToSinkPaths = GetFilteredRoundTripAllowedHubToSinkPaths()
                }));
    }

    private async Task CreateFilteredRoundTripSinkTaskAsync()
    {
        var sinkStore = Store(ClusterSide.Sink, LabNode.A);
        var connectionStringName = $"filtered-round-trip-{Guid.NewGuid():N}";

        await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
            new RavenConnectionString
            {
                Name = connectionStringName,
                Database = HubDatabaseName,
                TopologyDiscoveryUrls =
                [
                    Server(ClusterSide.Hub, LabNode.A).WebUrl,
                    Server(ClusterSide.Hub, LabNode.B).WebUrl,
                    Server(ClusterSide.Hub, LabNode.C).WebUrl
                ]
            }));

        await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
            HubName = FilteredRoundTripHubName,
            AccessName = HubAccessName,
            CertificateWithPrivateKey = PullCertificate,
            PinToMentorNode = true,
            MentorNode = NodeTag(ClusterSide.Sink, LabNode.A),
            AllowedSinkToHubPaths = GetFilteredRoundTripAllowedSinkToHubPaths(),
            AllowedHubToSinkPaths = GetFilteredRoundTripAllowedHubToSinkPaths()
        }));
    }

    private string[] GetFilteredRoundTripAllowedHubToSinkPaths()
    {
        if (_filteredRoundTripReturnLegUnfiltered && RequiredFilteredPassReceiveSide == ClusterSide.Sink)
            return ["*"];

        return [RequiredFilteredRoundTripPath + "/*"];
    }

    private string[] GetFilteredRoundTripAllowedSinkToHubPaths()
    {
        if (_filteredRoundTripReturnLegUnfiltered && RequiredFilteredPassReceiveSide == ClusterSide.Hub)
            return ["*"];

        return [RequiredFilteredRoundTripPath + "/*"];
    }

    private void AssertFilteredOutUserDidNotReach(ClusterSide side, LabNode node)
    {
        var filteredOutUser = GetDocumentById(side, node, RequiredFilteredOutUserDocumentId);

        Assert.False(
            filteredOutUser.Exists,
            $"Expected filtered-out user document '{RequiredFilteredOutUserDocumentId}' not to reach {NodeTag(side, node)} through filtered pull replication. " +
            $"The filtered route allows '{RequiredFilteredRoundTripPath}/*' but this document is under 'users/*'. " +
            $"Actual CV='{filteredOutUser.ChangeVector ?? "<null>"}'.");
    }

    private void InstallFilteredRoundTripOwnershipHooks(ClusterSide receiveSide)
    {
        foreach (var node in NodesOnEachClusterSide)
        {
            var database = Database(ClusterSide.Sink, node);
            var testing = database.ReplicationLoader.ForTestingPurposesOnly();
            var previous = testing.ShouldOwnExternalReplicationTask;

            testing.ShouldOwnExternalReplicationTask = task =>
            {
                if (task is PullReplicationAsSink sink && IsFilteredRoundTripSinkTask(sink))
                    return ShouldOwnFilteredRoundTripTask(receiveSide, node, sink.Mode);

                return previous?.Invoke(task);
            };

            _testingHookScopes.Add(new TestingHookScope(() => testing.ShouldOwnExternalReplicationTask = previous));
        }
    }

    private void InstallFilteredRoundTripRemoteUrlHooks(ClusterSide receiveSide)
    {
        foreach (var node in NodesOnEachClusterSide)
        {
            var database = Database(ClusterSide.Sink, node);
            var testing = database.ReplicationLoader.ForTestingPurposesOnly();
            var previous = testing.SelectPullReplicationRemoteUrls;

            testing.SelectPullReplicationRemoteUrls = (sink, databaseName, remoteUrls) =>
            {
                if (IsFilteredRoundTripSinkTask(sink))
                {
                    var hubNode = GetFilteredRoundTripHubNode(receiveSide, sink.Mode);
                    return [Server(ClusterSide.Hub, hubNode).WebUrl];
                }

                return previous?.Invoke(sink, databaseName, remoteUrls);
            };

            _testingHookScopes.Add(new TestingHookScope(() => testing.SelectPullReplicationRemoteUrls = previous));
        }
    }

    private FilteredRoundTripReplicationGate CreateFilteredRoundTripReturnGate(ClusterSide receiveSide)
    {
        return receiveSide switch
        {
            ClusterSide.Hub => new FilteredRoundTripReplicationGate(
                Database(ClusterSide.Sink, LabNode.A),
                matches: handler => handler is OutgoingPullReplicationHandlerAsSink &&
                                    handler.Destination is PullReplicationAsSink sink &&
                                    IsFilteredRoundTripSinkTask(sink) &&
                                    sink.Mode == PullReplicationMode.SinkToHub,
                description: "SA->HA filtered round trip"),

            ClusterSide.Sink => new FilteredRoundTripReplicationGate(
                Database(ClusterSide.Hub, LabNode.A),
                matches: handler => handler is OutgoingPullReplicationHandlerAsHub asHub &&
                                    string.Equals(asHub.PullReplicationDefinitionName, FilteredRoundTripHubName, StringComparison.OrdinalIgnoreCase),
                description: "HA->SA filtered round trip"),

            _ => throw new ArgumentOutOfRangeException(nameof(receiveSide), receiveSide, null)
        };
    }

    private FilteredRoundTripReplicationGate CreateFilteredRoundTripFirstHopGate(ClusterSide receiveSide)
    {
        return receiveSide switch
        {
            ClusterSide.Hub => new FilteredRoundTripReplicationGate(
                Database(ClusterSide.Hub, LabNode.B),
                matches: handler => handler is OutgoingPullReplicationHandlerAsHub asHub &&
                                    string.Equals(asHub.PullReplicationDefinitionName, FilteredRoundTripHubName, StringComparison.OrdinalIgnoreCase),
                description: "HB->SA filtered round trip"),

            ClusterSide.Sink => new FilteredRoundTripReplicationGate(
                Database(ClusterSide.Sink, LabNode.B),
                matches: handler => handler is OutgoingPullReplicationHandlerAsSink &&
                                    handler.Destination is PullReplicationAsSink sink &&
                                    IsFilteredRoundTripSinkTask(sink) &&
                                    sink.Mode == PullReplicationMode.SinkToHub,
                description: "SB->HA filtered round trip"),

            _ => throw new ArgumentOutOfRangeException(nameof(receiveSide), receiveSide, null)
        };
    }

    private DatabaseOutgoingReplicationHandler GetFilteredRoundTripFirstHopHandler()
    {
        return RequiredFilteredPassReceiveSide switch
        {
            ClusterSide.Hub => Database(ClusterSide.Hub, LabNode.B).ReplicationLoader.OutgoingHandlers
                .FirstOrDefault(handler => handler is OutgoingPullReplicationHandlerAsHub asHub &&
                                           string.Equals(asHub.PullReplicationDefinitionName, FilteredRoundTripHubName, StringComparison.OrdinalIgnoreCase)),

            ClusterSide.Sink => Database(ClusterSide.Sink, LabNode.B).ReplicationLoader.OutgoingHandlers
                .FirstOrDefault(handler => handler is OutgoingPullReplicationHandlerAsSink &&
                                           handler.Destination is PullReplicationAsSink sink &&
                                           IsFilteredRoundTripSinkTask(sink) &&
                                           sink.Mode == PullReplicationMode.SinkToHub),

            _ => throw new ArgumentOutOfRangeException(nameof(FilteredPassReceiveSide), FilteredPassReceiveSide, null)
        };
    }

    private static string GetFilteredRoundTripFirstHopDescription(ClusterSide receiveSide)
    {
        return receiveSide switch
        {
            ClusterSide.Hub => "HB->SA filtered round trip",
            ClusterSide.Sink => "SB->HA filtered round trip",
            _ => throw new ArgumentOutOfRangeException(nameof(receiveSide), receiveSide, null)
        };
    }

    private bool IsFilteredRoundTripSinkTask(PullReplicationAsSink sink)
    {
        return string.Equals(sink.HubName, FilteredRoundTripHubName, StringComparison.OrdinalIgnoreCase) &&
               (ContainsFilteredRoundTripPath(sink.AllowedSinkToHubPaths) ||
                ContainsFilteredRoundTripPath(sink.AllowedHubToSinkPaths));
    }

    private bool ContainsFilteredRoundTripPath(string[] paths) =>
        paths?.Any(x => string.Equals(x, RequiredFilteredRoundTripPath + "/*", StringComparison.OrdinalIgnoreCase)) == true;

    private static bool ShouldOwnFilteredRoundTripTask(ClusterSide receiveSide, LabNode sinkNode, PullReplicationMode mode)
    {
        return receiveSide switch
        {
            ClusterSide.Hub => sinkNode == LabNode.A,
            ClusterSide.Sink when mode == PullReplicationMode.SinkToHub => sinkNode == LabNode.B,
            ClusterSide.Sink when mode == PullReplicationMode.HubToSink => sinkNode == LabNode.A,
            ClusterSide.Sink => false,
            _ => throw new ArgumentOutOfRangeException(nameof(receiveSide), receiveSide, null)
        };
    }

    private static LabNode GetFilteredRoundTripHubNode(ClusterSide receiveSide, PullReplicationMode mode)
    {
        return receiveSide switch
        {
            ClusterSide.Hub when mode == PullReplicationMode.HubToSink => LabNode.B,
            ClusterSide.Hub when mode == PullReplicationMode.SinkToHub => LabNode.A,
            ClusterSide.Sink => LabNode.A,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };
    }

    private sealed class TestingHookScope : IDisposable
    {
        private readonly Action _restore;
        private bool _disposed;

        public TestingHookScope(Action restore)
        {
            _restore = restore;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _restore();
        }
    }

    public async ValueTask DisposeAsync()
    {
        for (var i = _internalReplicationBlockers.Count - 1; i >= 0; i--)
            await _internalReplicationBlockers[i].DisposeAsync();

        for (var i = _testingHookScopes.Count - 1; i >= 0; i--)
            _testingHookScopes[i].Dispose();

        foreach (var store in _stores.Values)
            store.Dispose();

        await Task.CompletedTask;
    }

    private static string Key(ClusterSide side, LabNode node) => side + ":" + node;

    private static ClusterSide GetOppositeSide(ClusterSide side) => side == ClusterSide.Hub ? ClusterSide.Sink : ClusterSide.Hub;

    private static string InternalReplicationBlockerTicketId(ClusterSide side, LabNode from, params LabNode[] to)
    {
        var fromTag = NodeTagLower(side, from);
        var destinations = string.Join("-and-", to.Select(x => NodeTagLower(side, x)));
        return $"tickets/internal-replication-blocker/{fromTag}-to-{destinations}";
    }
}
