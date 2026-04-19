using System;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public class NonDocumentDbCvProtectionTests : NonDocumentDbCvProtectionTestBase
{
    public enum SatelliteCaseKind
    {
        Counter,
        Attachment,
        TimeSeries,
        TimeSeriesDeletedRange,
        AttachmentTombstone
    }

    private sealed record SatelliteCase(
        string Label,
        string DocId,
        Func<NonDocumentLab, string, Task<SatelliteMarker>> SeedAsync,
        Func<NonDocumentLab, LineageNode, string, SatelliteMarker, bool> WaitForItem);

    private const LineageNode HubEntry = LineageNode.A;
    private const LineageNode SiblingNode = LineageNode.B;
    private const LineageNode HubIntermediate = LineageNode.C;
    private const LineageNode DirectObserver = LineageNode.C;
    private const LineageNode IndirectObserver = LineageNode.D;

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

    public NonDocumentDbCvProtectionTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [SatelliteCaseKind.Counter])]
    [RavenData(Data = [SatelliteCaseKind.Attachment])]
    [RavenData(Data = [SatelliteCaseKind.TimeSeries])]
    [RavenData(Data = [SatelliteCaseKind.TimeSeriesDeletedRange])]
    [RavenData(Data = [SatelliteCaseKind.AttachmentTombstone])]
    public async Task Direct_ShouldNotInflateHubObserverDbCv(SatelliteCaseKind satelliteCaseKind)
    {
        var satelliteCase = GetSatelliteCase(satelliteCaseKind);

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: DirectObserver);
        using var siblingToIdle = lab.BlockLink(source: SiblingNode, target: IndirectObserver);
        using var hubEntryToIdle = lab.BlockLink(source: HubEntry, target: IndirectObserver);
        using var observerToIdle = lab.BlockLink(source: DirectObserver, target: IndirectObserver);

        await RunProtectedScenarioAsync(
            lab,
            satelliteCase,
            baselineSource: HubEntry,
            observer: DirectObserver,
            releaseSiblingToObserver: siblingToObserver,
            deliverToObserverAsync: marker =>
            {
                Assert.True(satelliteCase.WaitForItem(lab, DirectObserver, satelliteCase.DocId, marker),
                    userMessage: $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach direct observer {DirectObserver} from hub-entry {HubEntry}.");
                return Task.CompletedTask;
            });
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [SatelliteCaseKind.Counter])]
    [RavenData(Data = [SatelliteCaseKind.Attachment])]
    [RavenData(Data = [SatelliteCaseKind.TimeSeries])]
    [RavenData(Data = [SatelliteCaseKind.TimeSeriesDeletedRange])]
    [RavenData(Data = [SatelliteCaseKind.AttachmentTombstone])]
    public async Task Indirect_ShouldNotInflateHubObserverDbCv(SatelliteCaseKind satelliteCaseKind)
    {
        var satelliteCase = GetSatelliteCase(satelliteCaseKind);

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToIntermediate = lab.BlockLink(source: SiblingNode, target: HubIntermediate);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: IndirectObserver);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: IndirectObserver);

        await RunProtectedScenarioAsync(
            lab,
            satelliteCase,
            baselineSource: HubIntermediate,
            observer: IndirectObserver,
            releaseSiblingToObserver: siblingToObserver,
            deliverToObserverAsync: async marker =>
            {
                Assert.True(satelliteCase.WaitForItem(lab, HubIntermediate, satelliteCase.DocId, marker),
                    userMessage: $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach hub-intermediate {HubIntermediate} from hub-entry {HubEntry}.");

                await lab.WriteSyncMarkerAndWaitAsync(sender: HubIntermediate, waitTargets: IndirectObserver);
                if (satelliteCase.WaitForItem(lab, IndirectObserver, satelliteCase.DocId, marker) == false)
                    await lab.WriteSyncMarkerAndWaitAsync(sender: HubIntermediate, waitTargets: IndirectObserver);

                Assert.True(satelliteCase.WaitForItem(lab, IndirectObserver, satelliteCase.DocId, marker),
                    userMessage: $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to reach indirect observer {IndirectObserver} via hub-intermediate {HubIntermediate}.");
            });
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task Counter_HeartbeatCascade_ShouldNotPropagateInflatedDbCv()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToInflatedObserver = lab.BlockLink(source: SiblingNode, target: HubIntermediate);
        using var siblingToHeartbeatReceiver = lab.BlockLink(source: SiblingNode, target: IndirectObserver);
        using var hubEntryToHeartbeatReceiver = lab.BlockLink(source: HubEntry, target: IndirectObserver);
        using var inflatedObserverToHeartbeatReceiver = lab.BlockLink(source: HubIntermediate, target: IndirectObserver);

        const string docId = "tickets/non-doc-heartbeat-counter";
        await EnsureBaselineReadyAsync(
            lab,
            baselineSource: HubIntermediate,
            scenario: "heartbeat");
        await lab.ApplyHeartbeatChangeVectorAsync(source: HubIntermediate, target: IndirectObserver);
        WaitForObserverSiblingBaseline(
            lab,
            sourceNode: HubIntermediate,
            observerNode: IndirectObserver,
            siblingNode: SiblingNode,
            scenario: "heartbeat-precondition");
        var heartbeatReceiverDbCvBefore = lab.GetDatabaseChangeVector(IndirectObserver);
        var backlogId = $"backlog/heartbeat/{Guid.NewGuid():N}";
        var backlogName = $"backlog-heartbeat-{Guid.NewGuid():N}";

        await StoreUserAsync(lab, SiblingNode, docId, "heartbeat-owner");
        var marker = await AddCounterAsync(lab, docId, "heartbeat-counter", SiblingNode);

        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        Assert.True(lab.WaitForDoc(HubEntry, docId, timeout: 60_000), userMessage: $"Expected bridged counter document '{docId}' on hub-entry {HubEntry}.");
        Assert.True(CounterCase.WaitForItem(lab, HubEntry, docId, marker), userMessage: $"Expected bridged counter '{marker.Name}' on hub-entry {HubEntry}.");
        Assert.True(CounterCase.WaitForItem(lab, HubIntermediate, docId, marker), userMessage: $"Expected bridged counter '{marker.Name}' on inflated observer {HubIntermediate} before applying the heartbeat update.");

        await lab.ApplyHeartbeatChangeVectorAsync(source: HubIntermediate, target: IndirectObserver);

        AssertNodeEtagUnchanged(
            heartbeatReceiverDbCvBefore,
            lab.GetDatabaseChangeVector(IndirectObserver),
            SiblingNode,
            $"[heartbeat] receiver {IndirectObserver} DB CV after heartbeat-style merge",
            $"receiver {IndirectObserver} DB CV before heartbeat-style merge");

        await StoreUserAsync(lab, SiblingNode, backlogId, backlogName);
        siblingToHeartbeatReceiver.Release();
        Assert.True(lab.WaitForDocumentName(IndirectObserver, backlogId, backlogName, timeout: 60_000),
            userMessage: $"[heartbeat] Expected backlog document '{backlogId}' from sibling node {SiblingNode} to reach heartbeat receiver {IndirectObserver} after releasing the sibling link.");
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task Counter_NormalInternalReplication_StillAdvancesDbCv()
    {
        await using var lab = await CreateLabAsync(new Options());

        const string docId = "tickets/non-doc-regression-normal";
        const string counterName = "clicks";

        using (var session = lab.StoreFor(HubEntry).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "regression" }, docId);
            session.CountersFor(docId).Increment(counterName, 10);
            await session.SaveChangesAsync();
        }

        Assert.True(lab.WaitForCounter(DirectObserver, docId, counterName, expectedValue: 10),
            userMessage: $"[regression] Expected counter '{counterName}' on '{docId}' to reach observer {DirectObserver} via normal internal replication.");

        var hubEntryDbId = lab.DatabaseFor(HubEntry).DbBase64Id;
        var observerDbCv = lab.GetDatabaseChangeVector(DirectObserver);
        Assert.True(observerDbCv.Contains(hubEntryDbId),
            userMessage: $"[regression] Observer {DirectObserver} DB CV should contain hub-entry {HubEntry}'s DB ID after normal internal replication. DB CV: '{observerDbCv}'. Hub-entry DB ID: '{hubEntryDbId}'.");
    }

    private static SatelliteCase GetSatelliteCase(SatelliteCaseKind satelliteCaseKind) => satelliteCaseKind switch
    {
        SatelliteCaseKind.Counter => CounterCase,
        SatelliteCaseKind.Attachment => AttachmentCase,
        SatelliteCaseKind.TimeSeries => TimeSeriesCase,
        SatelliteCaseKind.TimeSeriesDeletedRange => TimeSeriesDeletedRangeCase,
        SatelliteCaseKind.AttachmentTombstone => AttachmentTombstoneCase,
        _ => throw new ArgumentOutOfRangeException(nameof(satelliteCaseKind), satelliteCaseKind, null)
    };

    private async Task RunProtectedScenarioAsync(
        NonDocumentLab lab,
        SatelliteCase satelliteCase,
        LineageNode baselineSource,
        LineageNode observer,
        InternalLinkBlocker releaseSiblingToObserver,
        Func<SatelliteMarker, Task> deliverToObserverAsync)
    {
        await EnsureBaselineReadyAsync(
            lab,
            baselineSource,
            scenario: $"protected-{satelliteCase.Label}");

        await lab.ApplyHeartbeatChangeVectorAsync(source: baselineSource, target: observer);
        WaitForObserverSiblingBaseline(
            lab,
            sourceNode: baselineSource,
            observerNode: observer,
            siblingNode: SiblingNode,
            scenario: $"protected-{satelliteCase.Label}-{baselineSource}-to-{observer}");

        var backlogId = $"backlog/{satelliteCase.Label}/{Guid.NewGuid():N}";
        var backlogName = $"backlog-{satelliteCase.Label}-{Guid.NewGuid():N}";

        await StoreUserAsync(lab, SiblingNode, satelliteCase.DocId, $"{satelliteCase.Label}-owner");
        var marker = await satelliteCase.SeedAsync(lab, satelliteCase.DocId);

        await lab.InjectExistingTicketAsync(satelliteCase.DocId, sourceNode: SiblingNode, targetNode: HubEntry);

        Assert.True(lab.WaitForDoc(HubEntry, satelliteCase.DocId, timeout: 60_000),
            userMessage: $"Expected bridged document '{satelliteCase.DocId}' on hub-entry {HubEntry}.");

        var hubEntryDoc = lab.GetDocumentSnapshot(HubEntry, satelliteCase.DocId);
        Assert.True(hubEntryDoc.Exists, userMessage: $"Expected bridged document '{satelliteCase.DocId}' on hub-entry {HubEntry}.");
        AssertFlagged(hubEntryDoc.Flags, $"bridged document '{satelliteCase.DocId}' on hub-entry {HubEntry}");

        Assert.True(satelliteCase.WaitForItem(lab, HubEntry, satelliteCase.DocId, marker),
            userMessage: $"Expected {satelliteCase.Label} on '{satelliteCase.DocId}' to arrive on hub-entry {HubEntry} before propagating it further.");

        if (baselineSource == HubEntry)
        {
            await lab.ApplyHeartbeatChangeVectorAsync(source: baselineSource, target: observer);
            WaitForObserverSiblingBaseline(
                lab,
                sourceNode: baselineSource,
                observerNode: observer,
                siblingNode: SiblingNode,
                scenario: $"protected-{satelliteCase.Label}-post-source-ready-{baselineSource}-to-{observer}");
        }

        var observerDbCvBefore = lab.GetDatabaseChangeVector(observer);
        await deliverToObserverAsync(marker);

        var observerDoc = lab.GetDocumentSnapshot(observer, satelliteCase.DocId);
        Assert.True(observerDoc.Exists, userMessage: $"Expected bridged document '{satelliteCase.DocId}' on observer {observer}.");
        AssertFlagged(observerDoc.Flags, $"bridged document '{satelliteCase.DocId}' on observer {observer}");

        var observerDbCvAfter = lab.GetDatabaseChangeVector(observer);
        AssertNodeEtagUnchanged(
            observerDbCvBefore,
            observerDbCvAfter,
            SiblingNode,
            $"[{satelliteCase.Label}] observer {observer} DB CV after {satelliteCase.Label} propagation",
            $"observer {observer} DB CV before {satelliteCase.Label} propagation");

        await StoreUserAsync(lab, SiblingNode, backlogId, backlogName);
        releaseSiblingToObserver.Release();

        Assert.True(lab.WaitForDocumentName(observer, backlogId, backlogName, timeout: 60_000),
            userMessage: $"[{satelliteCase.Label}] Expected backlog document '{backlogId}' from sibling node {SiblingNode} to reach observer {observer} after releasing the direct sibling link.");
    }

    private static void AssertFlagged(DocumentFlags flags, string subject)
    {
        Assert.True((flags & DocumentFlags.FromFilteredPullReplicationHub) == DocumentFlags.FromFilteredPullReplicationHub,
            userMessage: $"Expected {subject} to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
    }

    private async Task EnsureBaselineReadyAsync(NonDocumentLab lab, LineageNode baselineSource, string scenario)
    {
        if (baselineSource != HubEntry)
        {
            await lab.ApplyHeartbeatChangeVectorAsync(source: HubEntry, target: baselineSource);
            WaitForObserverSiblingBaseline(
                lab,
                sourceNode: HubEntry,
                observerNode: baselineSource,
                siblingNode: SiblingNode,
                scenario: $"{scenario}-upstream-{HubEntry}-to-{baselineSource}");
        }
    }

    private static void WaitForObserverSiblingBaseline(NonDocumentLab lab, LineageNode sourceNode, LineageNode observerNode, LineageNode siblingNode, string scenario)
    {
        var sourceDbCv = lab.GetDatabaseChangeVector(sourceNode);
        var expectedSiblingEtag = GetNodeEtag(sourceDbCv, siblingNode);
        Assert.True(expectedSiblingEtag.HasValue,
            userMessage: $"Expected source {sourceNode} DB CV to include sibling node '{siblingNode}' before {scenario}, but source CV was '{sourceDbCv ?? "<null>"}'.");


        Assert.True(WaitForValue(
                () => (GetNodeEtag(lab.GetDatabaseChangeVector(observerNode), siblingNode) ?? -1) >= expectedSiblingEtag.Value,
                expectedVal: true,
                timeout: 60_000),
            userMessage: $"Expected observer {observerNode} sibling baseline for '{siblingNode}' to reach at least {expectedSiblingEtag.Value} before {scenario}.");

        var actual = GetNodeEtag(lab.GetDatabaseChangeVector(observerNode), siblingNode) ?? -1;


        Assert.True(actual >= expectedSiblingEtag.Value,
            userMessage: $"Expected observer {observerNode} sibling baseline for '{siblingNode}' to be at least {expectedSiblingEtag.Value} before {scenario}, but got {actual}.");
    }

    private static void AssertNodeEtagUnchanged(string expectedChangeVector, string actualChangeVector, LineageNode node, string actualSubject, string expectedSubject)
    {
        var expectedEtag = GetNodeEtag(expectedChangeVector, node);
        var actualEtag = GetNodeEtag(actualChangeVector, node);

        Assert.True(expectedEtag.HasValue && actualEtag.HasValue && expectedEtag.Value == actualEtag.Value,
            userMessage: $"Expected {actualSubject} to keep the '{node}' etag from {expectedSubject}, but expected {expectedEtag?.ToString() ?? "<missing>"} and got {actualEtag?.ToString() ?? "<missing>"}. Expected CV: '{expectedChangeVector ?? "<null>"}'. Actual CV: '{actualChangeVector ?? "<null>"}'.");
    }

    private static long? GetNodeEtag(string changeVector, LineageNode node)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return null;

        foreach (var entry in changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colonIndex = entry.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            if (string.Equals(entry.Substring(0, colonIndex), node.ToString(), StringComparison.OrdinalIgnoreCase) == false)
                continue;

            var dashIndex = entry.IndexOf('-', colonIndex + 1);
            var etagText = dashIndex > colonIndex
                ? entry.Substring(colonIndex + 1, dashIndex - colonIndex - 1)
                : entry[(colonIndex + 1)..];

            return long.TryParse(etagText, out var etag) ? etag : null;
        }

        return null;
    }
}
