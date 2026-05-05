using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class NonDocumentDbCvProtectionTests : NonDocumentDbCvProtectionTestBase
{
    private sealed record SatelliteCase(
        string Label,
        string DocId,
        Func<NonDocumentLab, string, Task<SatelliteMarker>> SeedAsync,
        Func<NonDocumentLab, LineageNode, string, SatelliteMarker, bool> WaitForItem);

    private sealed record FilteredPullItemCase(
        string Label,
        string DocId,
        Func<IDocumentStore, string, Task<SatelliteMarker>> SeedAsync,
        Func<NonDocumentLab, LineageNode, string, SatelliteMarker, bool> WaitForItem);

    private const LineageNode HubEntry = LineageNode.A;
    private const LineageNode SiblingNode = LineageNode.B;
    private const LineageNode HubIntermediate = LineageNode.C;
    private const LineageNode DirectObserver = LineageNode.C;
    private const LineageNode IndirectObserver = LineageNode.D;
    private const int FilteredPullHandshakeTimeoutMs = 25_000;

    private static readonly SatelliteCase CounterCase = new(
        Label: "counter",
        DocId: "tickets/non-doc-counter",
        SeedAsync: (lab, docId) => AddCounterAsync(lab, docId, "views", SiblingNode),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForCounter(node, docId, marker.Name, expectedValue: 1));

    private static readonly SatelliteCase AttachmentCase = new(
        Label: "attachment",
        DocId: "tickets/non-doc-attachment",
        SeedAsync: (lab, docId) => AddAttachmentAsync(lab, docId, "data.bin", SiblingNode),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForAttachment(node, docId, marker.Name, marker.Hash));

    private static readonly SatelliteCase TimeSeriesCase = new(
        Label: "time-series",
        DocId: "tickets/non-doc-ts",
        SeedAsync: (lab, docId) => AddTimeSeriesAsync(lab, docId, "HeartRate", new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc), SiblingNode),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForTimeSeries(node, docId, marker.Name));

    private static readonly SatelliteCase TimeSeriesDeletedRangeCase = new(
        Label: "time-series-deleted-range",
        DocId: "tickets/non-doc-ts-deleted-range",
        SeedAsync: (lab, docId) => AddTimeSeriesDeletedRangeAsync(lab, docId, "Steps", new DateTime(2024, 02, 01, 00, 00, 00, DateTimeKind.Utc), SiblingNode),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForDeletedRange(node, docId, marker.Name));

    private static readonly SatelliteCase AttachmentTombstoneCase = new(
        Label: "attachment-tombstone",
        DocId: "tickets/non-doc-attachment-tombstone",
        SeedAsync: (lab, docId) => AddAttachmentTombstoneAsync(lab, docId, "deleted.bin", SiblingNode),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForAttachmentTombstone(node, docId, marker.Name, marker.Hash, marker.ContentType));

    private static readonly FilteredPullItemCase FilteredPullCounterCase = new(
        Label: "counter",
        DocId: "tickets/filtered-pull-counter",
        SeedAsync: (store, docId) => SeedSourceCounterAsync(store, docId, "views"),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForCounter(node, docId, marker.Name, expectedValue: 1, timeout: FilteredPullHandshakeTimeoutMs));

    private static readonly FilteredPullItemCase FilteredPullAttachmentCase = new(
        Label: "attachment",
        DocId: "tickets/filtered-pull-attachment",
        SeedAsync: (store, docId) => SeedSourceAttachmentAsync(store, docId, "data.bin"),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForAttachment(node, docId, marker.Name, marker.Hash, timeout: FilteredPullHandshakeTimeoutMs));

    private static readonly FilteredPullItemCase FilteredPullTimeSeriesCase = new(
        Label: "time-series",
        DocId: "tickets/filtered-pull-ts",
        SeedAsync: (store, docId) => SeedSourceTimeSeriesAsync(store, docId, "HeartRate", new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc)),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForTimeSeries(node, docId, marker.Name, timeout: FilteredPullHandshakeTimeoutMs));

    private static readonly FilteredPullItemCase FilteredPullTimeSeriesDeletedRangeCase = new(
        Label: "time-series-deleted-range",
        DocId: "tickets/filtered-pull-ts-deleted-range",
        SeedAsync: (store, docId) => SeedSourceTimeSeriesDeletedRangeAsync(store, docId, "Steps", new DateTime(2024, 02, 01, 00, 00, 00, DateTimeKind.Utc)),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForDeletedRange(node, docId, marker.Name, timeout: FilteredPullHandshakeTimeoutMs));

    private static readonly FilteredPullItemCase FilteredPullAttachmentTombstoneCase = new(
        Label: "attachment-tombstone",
        DocId: "tickets/filtered-pull-attachment-tombstone",
        SeedAsync: (store, docId) => SeedSourceAttachmentTombstoneAsync(store, docId, "deleted.bin"),
        WaitForItem: (lab, node, docId, marker) => lab.WaitForAttachmentTombstone(node, docId, marker.Name, marker.Hash, marker.ContentType, timeout: FilteredPullHandshakeTimeoutMs));

    public NonDocumentDbCvProtectionTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Counter_Direct_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunDirectScenarioAsync(options, CounterCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Counter_Indirect_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunIndirectScenarioAsync(options, CounterCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_Direct_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunDirectScenarioAsync(options, AttachmentCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_Indirect_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunIndirectScenarioAsync(options, AttachmentCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeries_Direct_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunDirectScenarioAsync(options, TimeSeriesCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeries_Indirect_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunIndirectScenarioAsync(options, TimeSeriesCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_Direct_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunDirectScenarioAsync(options, TimeSeriesDeletedRangeCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_Indirect_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunIndirectScenarioAsync(options, TimeSeriesDeletedRangeCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task AttachmentTombstone_Direct_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunDirectScenarioAsync(options, AttachmentTombstoneCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task AttachmentTombstone_Indirect_ShouldStillDeliverBacklogToObserver(Options options) =>
        RunIndirectScenarioAsync(options, AttachmentTombstoneCase);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Counter_HeartbeatCascade_ShouldStillDeliverBacklogToReceiver(Options options) =>
        RunHeartbeatCascadeScenarioAsync(options);

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Counter_NormalInternalReplication_ShouldStillReplicate(Options options) =>
        RunNormalInternalReplicationRegressionAsync(options);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Counter_FilteredPullHandshake_ShouldStillDeliverItem(Options options) =>
        RunFilteredPullHandshakeScenarioAsync(options, FilteredPullCounterCase);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_FilteredPullHandshake_ShouldStillDeliverItem(Options options) =>
        RunFilteredPullHandshakeScenarioAsync(options, FilteredPullAttachmentCase);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeries_FilteredPullHandshake_ShouldStillDeliverItem(Options options) =>
        RunFilteredPullHandshakeScenarioAsync(options, FilteredPullTimeSeriesCase);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_FilteredPullHandshake_ShouldStillDeliverItem(Options options) =>
        RunFilteredPullHandshakeScenarioAsync(options, FilteredPullTimeSeriesDeletedRangeCase);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task AttachmentTombstone_FilteredPullHandshake_ShouldStillDeliverItem(Options options) =>
        RunFilteredPullHandshakeScenarioAsync(options, FilteredPullAttachmentTombstoneCase);

    private async Task RunDirectScenarioAsync(Options options, SatelliteCase satelliteCase)
    {
        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(SiblingNode, HubEntry);
        using var siblingToObserver = lab.BlockLink(SiblingNode, DirectObserver);
        using var siblingToIdle = lab.BlockLink(SiblingNode, IndirectObserver);
        using var hubEntryToIdle = lab.BlockLink(HubEntry, IndirectObserver);
        using var observerToIdle = lab.BlockLink(DirectObserver, IndirectObserver);

        await RunProtectedScenarioAsync(
            lab,
            satelliteCase,
            baselineSource: HubEntry,
            observer: DirectObserver,
            releaseSiblingToObserver: siblingToObserver,
            deliverToObserverAsync: marker =>
            {
                Assert.True(
                    satelliteCase.WaitForItem(lab, DirectObserver, satelliteCase.DocId, marker),
                    $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach direct observer {DirectObserver} from hub-entry {HubEntry}.");
                return Task.CompletedTask;
            });
    }

    private async Task RunIndirectScenarioAsync(Options options, SatelliteCase satelliteCase)
    {
        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(SiblingNode, HubEntry);
        using var siblingToIntermediate = lab.BlockLink(SiblingNode, HubIntermediate);
        using var siblingToObserver = lab.BlockLink(SiblingNode, IndirectObserver);
        using var hubEntryToObserver = lab.BlockLink(HubEntry, IndirectObserver);

        await RunProtectedScenarioAsync(
            lab,
            satelliteCase,
            baselineSource: HubIntermediate,
            observer: IndirectObserver,
            releaseSiblingToObserver: siblingToObserver,
            deliverToObserverAsync: async marker =>
            {
                Assert.True(
                    satelliteCase.WaitForItem(lab, HubIntermediate, satelliteCase.DocId, marker),
                    $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach hub-intermediate {HubIntermediate} from hub-entry {HubEntry}.");

                await lab.WriteSyncMarkerAndWaitAsync(HubIntermediate, IndirectObserver);
                if (satelliteCase.WaitForItem(lab, IndirectObserver, satelliteCase.DocId, marker) == false)
                    await lab.WriteSyncMarkerAndWaitAsync(HubIntermediate, IndirectObserver);

                Assert.True(
                    satelliteCase.WaitForItem(lab, IndirectObserver, satelliteCase.DocId, marker),
                    $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach indirect observer {IndirectObserver} via hub-intermediate {HubIntermediate}.");
            });
    }

    private async Task RunProtectedScenarioAsync(
        NonDocumentLab lab,
        SatelliteCase satelliteCase,
        LineageNode baselineSource,
        LineageNode observer,
        InternalLinkBlocker releaseSiblingToObserver,
        Func<SatelliteMarker, Task> deliverToObserverAsync)
    {
        await ApplyBaselineHeartbeatsAsync(lab, baselineSource, observer);

        var backlogId = $"backlog/{satelliteCase.Label}/{Guid.NewGuid():N}";
        var backlogName = $"backlog-{satelliteCase.Label}-{Guid.NewGuid():N}";
        await StoreUserAsync(lab, SiblingNode, backlogId, backlogName);

        await StoreUserAsync(lab, SiblingNode, satelliteCase.DocId, $"{satelliteCase.Label}-owner");
        var marker = await satelliteCase.SeedAsync(lab, satelliteCase.DocId);

        await lab.InjectExistingTicketAsync(satelliteCase.DocId, SiblingNode, HubEntry);

        Assert.True(
            lab.WaitForDoc(HubEntry, satelliteCase.DocId, timeout: 60_000),
            $"Expected bridged document '{satelliteCase.DocId}' on hub-entry {HubEntry}.");

        Assert.True(
            satelliteCase.WaitForItem(lab, HubEntry, satelliteCase.DocId, marker),
            $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to arrive on hub-entry {HubEntry} before propagating it further.");

        if (baselineSource == HubEntry)
            await lab.ApplyHeartbeatChangeVectorAsync(baselineSource, observer);

        await deliverToObserverAsync(marker);

        releaseSiblingToObserver.Release();

        Assert.True(
            lab.WaitForDocumentName(observer, backlogId, backlogName, timeout: 60_000),
            $"[{satelliteCase.Label}] Expected backlog document '{backlogId}' from sibling node {SiblingNode} to reach observer {observer} after releasing the direct sibling link.");
    }

    private async Task RunHeartbeatCascadeScenarioAsync(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(SiblingNode, HubEntry);
        using var siblingToInflatedObserver = lab.BlockLink(SiblingNode, HubIntermediate);
        using var siblingToHeartbeatReceiver = lab.BlockLink(SiblingNode, IndirectObserver);
        using var hubEntryToHeartbeatReceiver = lab.BlockLink(HubEntry, IndirectObserver);
        using var inflatedObserverToHeartbeatReceiver = lab.BlockLink(HubIntermediate, IndirectObserver);

        const string docId = "tickets/non-doc-heartbeat-counter";
        await ApplyBaselineHeartbeatsAsync(lab, baselineSource: HubIntermediate, observer: IndirectObserver);
        await lab.ApplyHeartbeatChangeVectorAsync(HubIntermediate, IndirectObserver);
        var backlogId = $"backlog/heartbeat/{Guid.NewGuid():N}";
        var backlogName = $"backlog-heartbeat-{Guid.NewGuid():N}";
        await StoreUserAsync(lab, SiblingNode, backlogId, backlogName);

        await StoreUserAsync(lab, SiblingNode, docId, "heartbeat-owner");
        var marker = await AddCounterAsync(lab, docId, "heartbeat-counter", SiblingNode);

        await lab.InjectExistingTicketAsync(docId, SiblingNode, HubEntry);

        Assert.True(lab.WaitForDoc(HubEntry, docId, timeout: 60_000), $"Expected bridged counter document '{docId}' on hub-entry {HubEntry}.");
        Assert.True(CounterCase.WaitForItem(lab, HubEntry, docId, marker), $"Expected bridged counter '{marker.Name}' on hub-entry {HubEntry}.");
        Assert.True(CounterCase.WaitForItem(lab, HubIntermediate, docId, marker), $"Expected bridged counter '{marker.Name}' on inflated observer {HubIntermediate} before applying the heartbeat update.");

        await lab.ApplyHeartbeatChangeVectorAsync(HubIntermediate, IndirectObserver);

        siblingToHeartbeatReceiver.Release();
        Assert.True(
            lab.WaitForDocumentName(IndirectObserver, backlogId, backlogName, timeout: 60_000),
            $"[heartbeat] Expected backlog document '{backlogId}' from sibling node {SiblingNode} to reach heartbeat receiver {IndirectObserver} after releasing the sibling link.");
    }

    private async Task RunNormalInternalReplicationRegressionAsync(Options options)
    {
        await using var lab = await CreateLabAsync(options);

        const string docId = "tickets/non-doc-regression-normal";
        const string counterName = "clicks";

        using (var session = lab.StoreFor(HubEntry).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "regression" }, docId);
            session.CountersFor(docId).Increment(counterName, 10);
            await session.SaveChangesAsync();
        }

        Assert.True(
            lab.WaitForCounter(DirectObserver, docId, counterName, expectedValue: 10),
            $"[regression] Expected counter '{counterName}' on '{docId}' to reach observer {DirectObserver} via normal internal replication.");

    }

    private async Task RunFilteredPullHandshakeScenarioAsync(Options options, FilteredPullItemCase itemCase)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore($"filtered-pull-{itemCase.Label}");

        var marker = await itemCase.SeedAsync(source, itemCase.DocId);
        await PreconditionHubAsIfSourceProgressWasAlreadyObservedAsync(lab, source, HubEntry);

        await lab.ConnectSinkToHubAsync(source, HubEntry);

        Assert.True(
            itemCase.WaitForItem(lab, HubEntry, itemCase.DocId, marker),
            $"Expected filtered sink-to-hub replication to deliver {itemCase.Label} on '{itemCase.DocId}' after the hub had been preconditioned with the same source identity. " +
            $"If this fails, a legitimate {itemCase.Label} item was skipped before it reached the hub.");
    }

    private async Task ApplyBaselineHeartbeatsAsync(NonDocumentLab lab, LineageNode baselineSource, LineageNode observer)
    {
        if (baselineSource != HubEntry)
            await lab.ApplyHeartbeatChangeVectorAsync(HubEntry, baselineSource);
        await lab.ApplyHeartbeatChangeVectorAsync(baselineSource, observer);
    }

    private async Task PreconditionHubAsIfSourceProgressWasAlreadyObservedAsync(NonDocumentLab lab, IDocumentStore source, LineageNode hub)
    {
        var sourceChangeVector = await ReadDatabaseChangeVectorAsync(source);
        var target = lab.DatabaseFor(hub);

        using (target.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenWriteTransaction())
        {
            var regularChangeVector = DocumentsStorage.GetDatabaseChangeVector(context).AsString();
            var fullChangeVector = string.IsNullOrWhiteSpace(regularChangeVector)
                ? sourceChangeVector
                : $"{regularChangeVector},{sourceChangeVector}";

            target.DocumentsStorage.SetDatabaseChangeVector(context, context.GetChangeVector(regularChangeVector));
            target.DocumentsStorage.SetFullDatabaseChangeVector(context, fullChangeVector);
            context.Transaction.Commit();
        }
    }

    private async Task<string> ReadDatabaseChangeVectorAsync(IDocumentStore store)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return DocumentsStorage.GetDatabaseChangeVector(context).AsString();
        }
    }

    private static async Task StoreSourceUserAsync(IDocumentStore store, string docId, string name)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new User { Name = name }, docId);
        await session.SaveChangesAsync();
    }

    private static async Task<SatelliteMarker> SeedSourceCounterAsync(IDocumentStore store, string docId, string counterName)
    {
        await StoreSourceUserAsync(store, docId, "counter-owner");
        using var session = store.OpenAsyncSession();
        session.CountersFor(docId).Increment(counterName, delta: 1);
        await session.SaveChangesAsync();
        return new SatelliteMarker(counterName);
    }

    private static async Task<SatelliteMarker> SeedSourceAttachmentAsync(IDocumentStore store, string docId, string attachmentName)
    {
        await StoreSourceUserAsync(store, docId, "attachment-owner");
        var attachment = await store.Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([1, 2, 3, 4, 5]),
                "application/octet-stream"));

        return new SatelliteMarker(attachmentName, attachment.Hash, attachment.ContentType);
    }

    private static async Task<SatelliteMarker> SeedSourceTimeSeriesAsync(IDocumentStore store, string docId, string timeSeriesName, DateTime baseline)
    {
        await StoreSourceUserAsync(store, docId, "time-series-owner");
        using var session = store.OpenAsyncSession();
        session.TimeSeriesFor(docId, timeSeriesName).Append(baseline, 72.0, "bpm");
        await session.SaveChangesAsync();
        return new SatelliteMarker(timeSeriesName);
    }

    private static async Task<SatelliteMarker> SeedSourceTimeSeriesDeletedRangeAsync(IDocumentStore store, string docId, string timeSeriesName, DateTime baseline)
    {
        await StoreSourceUserAsync(store, docId, "time-series-deleted-range-owner");

        using (var session = store.OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Append(baseline, 99.0, "bpm");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
            await session.SaveChangesAsync();
        }

        return new SatelliteMarker(timeSeriesName);
    }

    private static async Task<SatelliteMarker> SeedSourceAttachmentTombstoneAsync(IDocumentStore store, string docId, string attachmentName)
    {
        await StoreSourceUserAsync(store, docId, "attachment-tombstone-owner");
        var attachment = await store.Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7]),
                "application/octet-stream"));

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete(docId, attachmentName);
            await session.SaveChangesAsync();
        }

        return new SatelliteMarker(attachmentName, attachment.Hash, attachment.ContentType);
    }
}
