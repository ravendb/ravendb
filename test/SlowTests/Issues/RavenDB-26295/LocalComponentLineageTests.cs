using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public class LocalComponentLineageTests : NonDocumentDbCvProtectionTestBase
{
    private const LineageNode HubEntry = LineageNode.A;
    private const LineageNode SiblingNode = LineageNode.B;
    private const LineageNode HubInternal = LineageNode.C;
    private const LineageNode Observer = LineageNode.D;

    private enum MetadataUpdateMode
    {
        Refresh,
        Archival
    }

    public LocalComponentLineageTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task TimeSeriesAppend_ShouldNotReuseSiblingLineage(bool onHubInternal)
    {
        const string timeSeriesName = "HeartRate";
        var baseline = new DateTime(2024, 03, 01, 00, 00, 00, DateTimeKind.Utc);
        var docId = GetDocId(prefix: "ts-append");

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);

        await StoreUserAsync(lab, SiblingNode, docId, userName: "ts-owner");
        await AddTimeSeriesAsync(lab, docId, timeSeriesName, baseline, SiblingNode);
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        Assert.True(lab.WaitForTimeSeries(node: HubEntry, docId, timeSeriesName, timeout: 60_000),
            userMessage: $"Expected '{timeSeriesName}' on '{docId}' at hub-entry {HubEntry}.");

        if (onHubInternal)
        {
            WaitForFlaggedDocument(lab, node: HubInternal, docId, subject: "hub-internal");
            Assert.True(lab.WaitForTimeSeries(node: HubInternal, docId, timeSeriesName, timeout: 60_000),
                userMessage: $"Expected '{timeSeriesName}' on '{docId}' at hub-internal {HubInternal}.");
        }

        await lab.ApplyHeartbeatChangeVectorAsync(source: baselineSource, target: Observer);
        await WaitForObserverSiblingBaseline(
            lab,
            sourceNode: baselineSource,
            observerNode: Observer,
            siblingNode: SiblingNode,
            scenario: $"time series append on {localNode}");

        var observerDbCvBefore = lab.GetDatabaseChangeVector(node: Observer);

        using (var session = lab.StoreFor(localNode).OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Append(baseline.AddMinutes(1), 73.0, "bpm");
            await session.SaveChangesAsync();
        }

        var localSegment = lab.GetTimeSeriesSegmentSnapshots(localNode, docId)
            .Where(x => string.Equals(x.Name, timeSeriesName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(x => x.Etag)
            .First();

        AssertNotContainsNodeEntry(localSegment.ChangeVector, SiblingNode, subject: $"local TS segment '{timeSeriesName}' on '{docId}' at {localNode}");

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/ts-append/{Guid.NewGuid():N}");

        Assert.True(lab.WaitForTimeSeries(node: Observer, docId, timeSeriesName, timeout: 60_000),
            userMessage: $"Expected local TS append '{timeSeriesName}' on '{docId}' from {localNode} to reach observer {Observer}.");

        WaitForFlaggedDocument(lab, node: Observer, docId, "observer");
        AssertNodeEtagUnchanged(
            observerDbCvBefore,
            lab.GetDatabaseChangeVector(node: Observer),
            SiblingNode,
            actualSubject: $"observer {Observer} DB CV after TS append on {localNode}",
            expectedSubject: $"observer {Observer} DB CV before TS append on {localNode}");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task CounterIncrement_ShouldUseLocalOnlyCounterGroupCv(bool onHubInternal)
    {
        const string counterName = "views";
        var docId = GetDocId(prefix: "counter-increment");

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);

        await StoreUserAsync(lab, SiblingNode, docId, userName: "counter-owner");
        await AddCounterAsync(lab, docId, counterName, SiblingNode);
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        Assert.True(lab.WaitForCounter(node: HubEntry, docId, counterName, expectedValue: 1, timeout: 60_000),
            userMessage: $"Expected counter '{counterName}' on '{docId}' at hub-entry {HubEntry}.");

        if (onHubInternal)
        {
            WaitForFlaggedDocument(lab, node: HubInternal, docId, subject: "hub-internal");
            Assert.True(lab.WaitForCounter(node: HubInternal, docId, counterName, expectedValue: 1, timeout: 60_000),
                userMessage: $"Expected counter '{counterName}' on '{docId}' at hub-internal {HubInternal}.");
        }

        await lab.ApplyHeartbeatChangeVectorAsync(source: baselineSource, target: Observer);
        await WaitForObserverSiblingBaseline(
            lab,
            sourceNode: baselineSource,
            observerNode: Observer,
            siblingNode: SiblingNode,
            scenario: $"counter increment on {localNode}");

        var observerDbCvBefore = lab.GetDatabaseChangeVector(node: Observer);

        using (var session = lab.StoreFor(localNode).OpenAsyncSession())
        {
            session.CountersFor(docId).Increment(counterName);
            await session.SaveChangesAsync();
        }

        var localCounterGroup = lab.GetCounterGroupSnapshots(localNode, docId)
            .OrderByDescending(counterGroupSnapshot => counterGroupSnapshot.Etag)
            .First();

        AssertNotContainsNodeEntry(
            localCounterGroup.ChangeVector,
            SiblingNode,
            subject: $"local counter group for '{docId}' at {localNode}");

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/counter-increment/{Guid.NewGuid():N}");

        Assert.True(lab.WaitForCounter(node: Observer, docId, counterName, expectedValue: 2, timeout: 60_000),
            userMessage: $"Expected local counter increment '{counterName}' on '{docId}' from {localNode} to reach observer {Observer}.");

        WaitForFlaggedDocument(lab, node: Observer, docId, subject: "observer");
        AssertNodeEtagUnchanged(
            observerDbCvBefore,
            lab.GetDatabaseChangeVector(node: Observer),
            SiblingNode,
            actualSubject: $"observer {Observer} DB CV after counter increment on {localNode}",
            expectedSubject: $"observer {Observer} DB CV before counter increment on {localNode}");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task NewAttachment_ShouldUseLocalOnlyAttachmentCv(bool onHubInternal)
    {
        const string attachmentName = "local.bin";
        var docId = GetDocId(prefix: "attachment-put");

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);

        await StoreUserAsync(lab, SiblingNode, docId, userName: "attachment-owner");
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        if (onHubInternal)
            WaitForFlaggedDocument(lab, node: HubInternal, docId, subject: "hub-internal");

        await lab.ApplyHeartbeatChangeVectorAsync(source: baselineSource, target: Observer);
        await WaitForObserverSiblingBaseline(
            lab,
            sourceNode: baselineSource,
            observerNode: Observer,
            siblingNode: SiblingNode,
            scenario: $"attachment put on {localNode}");

        await lab.StoreFor(localNode).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7, 6]),
                contentType: "application/octet-stream"));

        var localAttachment = lab.GetAttachmentSnapshot(localNode, docId, attachmentName);
        Assert.True(localAttachment.Exists, userMessage: $"Expected local attachment '{attachmentName}' on '{docId}' at {localNode}.");
        AssertNotContainsNodeEntry(
            localAttachment.ChangeVector,
            SiblingNode,
            subject: $"local attachment '{attachmentName}' on '{docId}' at {localNode}");

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/attachment-put/{Guid.NewGuid():N}");

        Assert.True(lab.WaitForAttachment(node: Observer, docId, attachmentName, localAttachment.Hash, timeout: 60_000),
            userMessage: $"Expected local attachment '{attachmentName}' on '{docId}' from {localNode} to reach observer {Observer}.");

        WaitForFlaggedDocument(lab, node: Observer, docId, subject: "observer");
        var observerAttachment = lab.GetAttachmentSnapshot(node: Observer, docId, attachmentName);
        Assert.True(observerAttachment.Exists, userMessage: $"Expected observer {Observer} to expose attachment '{attachmentName}' on '{docId}'.");
        Assert.Equal(localAttachment.ChangeVector, observerAttachment.ChangeVector);
        AssertNotContainsNodeEntry(
            observerAttachment.ChangeVector,
            SiblingNode,
            subject: $"observer attachment '{attachmentName}' on '{docId}' at {Observer}");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task RefreshMetadataOnFlaggedDocument_ShouldRemainPendingInThisHarness(bool onHubInternal)
    {
        const MetadataUpdateMode mode = MetadataUpdateMode.Refresh;
        var docId = GetDocId(prefix: "metadata-refresh");
        var dueTime = DateTime.UtcNow.AddMinutes(-5);

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);

        await StoreUserAsync(lab, SiblingNode, docId, userName: "refresh-owner");
        await SetDueMetadataAsync(lab, SiblingNode, docId, mode, dueTime);
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        if (onHubInternal)
            WaitForFlaggedDocument(lab, node: HubInternal, docId, subject: "hub-internal");

        WaitForMetadataKey(lab, localNode, docId, Raven.Client.Constants.Documents.Metadata.Refresh);
        var localBefore = lab.GetDocumentSnapshot(localNode, docId);
        var localDbCvBefore = lab.GetDatabaseChangeVector(localNode);
        var isResponsibleNode = await ProcessMetadataUpdateAsync(lab, localNode, mode, dueTime.AddMinutes(1));

        var localAfter = lab.GetDocumentSnapshot(localNode, docId);
        Assert.True(localAfter.Exists, userMessage: $"Expected '{docId}' to exist on {localNode} after refresh cleaner.");
        AssertFlagged(localAfter.Flags, subject: $"refresh pending document '{docId}' on {localNode}");
        if (isResponsibleNode)
        {
            Assert.True(ContainsNodeEntry(localAfter.ChangeVector, SiblingNode),
                userMessage: $"Expected refreshed document '{docId}' on {localNode} to carry sibling lineage in its CV, but CV was '{localAfter.ChangeVector ?? "<null>"}'.");
            Assert.NotEqual(localBefore.ChangeVector, localAfter.ChangeVector);
            AssertNodeEtagUnchanged(
                localDbCvBefore,
                lab.GetDatabaseChangeVector(localNode),
                SiblingNode,
                actualSubject: $"{localNode} DB CV after refresh processing",
                expectedSubject: $"{localNode} DB CV before refresh processing");
            AssertRefreshMetadataCleared(lab, localNode, docId);
            return;
        }

        Assert.True(ContainsNodeEntry(localAfter.ChangeVector, SiblingNode),
            userMessage: $"Expected refresh pending document '{docId}' on {localNode} to carry sibling lineage in its CV, but CV was '{localAfter.ChangeVector ?? "<null>"}'.");
        Assert.Equal(localBefore.ChangeVector, localAfter.ChangeVector);
        AssertRefreshMetadataStillPending(lab, localNode, docId);
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task ArchivalMetadataUpdate_ShouldKeepFilteredGuardOnFlaggedDocument(bool onHubInternal)
    {
        const MetadataUpdateMode mode = MetadataUpdateMode.Archival;
        var docId = GetDocId(prefix: $"metadata-{mode.ToString().ToLowerInvariant()}");
        var dueTime = DateTime.UtcNow.AddMinutes(-5);

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);

        await StoreUserAsync(lab, SiblingNode, docId, userName: $"{mode}-owner");
        await SetDueMetadataAsync(lab, SiblingNode, docId, mode, dueTime);
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        if (onHubInternal)
            WaitForFlaggedDocument(lab, node: HubInternal, docId, subject: "hub-internal");

        WaitForMetadataKey(lab, localNode, docId, Raven.Client.Constants.Documents.Metadata.ArchiveAt);
        var localBefore = lab.GetDocumentSnapshot(localNode, docId);
        var localDbCvBefore = lab.GetDatabaseChangeVector(localNode);

        var isResponsibleNode = await ProcessMetadataUpdateAsync(lab, localNode, mode, dueTime.AddMinutes(1));

        var localSnapshot = lab.GetDocumentSnapshot(localNode, docId);
        Assert.True(localSnapshot.Exists, userMessage: $"Expected '{docId}' to exist on {localNode} after {mode} processing.");
        AssertFlagged(localSnapshot.Flags, subject: $"{mode} updated document '{docId}' on {localNode}");

        if (isResponsibleNode)
        {
            Assert.True(localSnapshot.Flags.Contain(DocumentFlags.Archived),
                userMessage: $"Expected archival-updated document '{docId}' on {localNode} to keep {nameof(DocumentFlags.Archived)}, but flags were '{localSnapshot.Flags}'.");
            Assert.True(ContainsNodeEntry(localSnapshot.ChangeVector, SiblingNode),
                userMessage: $"Expected archival-updated document '{docId}' on {localNode} to retain sibling lineage in its CV, but CV was '{localSnapshot.ChangeVector ?? "<null>"}'.");
            Assert.NotEqual(localBefore.ChangeVector, localSnapshot.ChangeVector);
            AssertNodeEtagUnchanged(
                localDbCvBefore,
                lab.GetDatabaseChangeVector(localNode),
                SiblingNode,
                actualSubject: $"{localNode} DB CV after archival processing",
                expectedSubject: $"{localNode} DB CV before archival processing");

            await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/metadata-{mode.ToString().ToLowerInvariant()}/{Guid.NewGuid():N}");

            WaitForFlaggedDocument(lab, node: Observer, docId, subject: "observer");
            var observerSnapshot = lab.GetDocumentSnapshot(node: Observer, docId);
            AssertFlagged(observerSnapshot.Flags, subject: $"archival-updated document '{docId}' on observer {Observer}");
            Assert.True(observerSnapshot.Flags.Contain(DocumentFlags.Archived),
                userMessage: $"Expected observer {Observer} to receive archived '{docId}' from {localNode}.");
            Assert.True(ContainsNodeEntry(observerSnapshot.ChangeVector, SiblingNode),
                userMessage: $"Expected observer {Observer} archived document '{docId}' from {localNode} to retain sibling lineage in its CV, but CV was '{observerSnapshot.ChangeVector ?? "<null>"}'.");
            return;
        }

        Assert.False(localSnapshot.Flags.Contain(DocumentFlags.Archived),
            userMessage: $"Expected archival-updated document '{docId}' on {localNode} to remain unarchived on non-responsible node '{localNode}', but flags were '{localSnapshot.Flags}'.");
        Assert.Equal(localBefore.ChangeVector, localSnapshot.ChangeVector);
        AssertNodeEtagUnchanged(
            localDbCvBefore,
            lab.GetDatabaseChangeVector(localNode),
            SiblingNode,
            actualSubject: $"{localNode} DB CV after non-responsible archival processing",
            expectedSubject: $"{localNode} DB CV before non-responsible archival processing");
        AssertArchivalMetadataStillPending(lab, localNode, docId);
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task ConflictResolution_ShouldPreserveFlaggedWinnerAndGuardDbCv()
    {
        var docId = GetDocId(prefix: "conflict-resolution");

        await using var lab = await CreateLabAsync(new Options());
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var conflictToHubEntry = lab.BlockLink(source: Observer, target: HubEntry);
        using var conflictToSibling = lab.BlockLink(source: Observer, target: SiblingNode);
        using var conflictToHubInternal = lab.BlockLink(source: Observer, target: HubInternal);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubEntryToHubInternal = lab.BlockLink(source: HubEntry, target: HubInternal);

        await StoreUserAsync(lab, node: Observer, docId, userName: "conflict-local");
        await StoreUserAsync(lab, SiblingNode, docId, userName: "flagged-winner");
        await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingNode, targetNode: HubEntry);

        WaitForFlaggedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        await lab.ApplyHeartbeatChangeVectorAsync(source: SiblingNode, target: HubInternal);
        await WaitForObserverSiblingBaseline(
            lab,
            sourceNode: SiblingNode,
            observerNode: HubInternal,
            siblingNode: SiblingNode,
            scenario: "conflict resolution observer baseline");

        await SetReplicationConflictResolutionAsync((DocumentStore)lab.StoreFor(node: HubEntry), StraightforwardConflictResolution.ResolveToLatest);

        var conflictResolutionMarkerId = $"markers/conflict-resolution/{Guid.NewGuid():N}";
        await StoreUserAsync(lab, node: Observer, conflictResolutionMarkerId, userName: "sync");
        conflictToHubEntry.Release();
        Assert.True(lab.WaitForDoc(HubEntry, conflictResolutionMarkerId, timeout: 60_000),
            userMessage: $"Expected sync marker '{conflictResolutionMarkerId}' from {Observer} to reach {HubEntry}.");

        Assert.Equal(
            0,
            WaitForValue(
                () => ((DocumentStore)lab.StoreFor(node: HubEntry)).Commands().GetConflictsForAsync(docId).GetAwaiter().GetResult().Length,
                expectedVal: 0,
                timeout: 60_000));

        Assert.True(lab.WaitForDocumentName(node: HubEntry, docId, expectedName: "flagged-winner", timeout: 60_000),
            userMessage: $"Expected conflict winner on hub-entry {HubEntry} for '{docId}' to remain the flagged document.");

        var resolvedSnapshot = lab.GetDocumentSnapshot(node: HubEntry, docId);
        AssertFlagged(resolvedSnapshot.Flags, subject: $"resolved winner '{docId}' on hub-entry {HubEntry}");
        Assert.True(resolvedSnapshot.Flags.Contain(DocumentFlags.Resolved),
            userMessage: $"Expected resolved winner '{docId}' on hub-entry {HubEntry} to keep {nameof(DocumentFlags.Resolved)}, but flags were '{resolvedSnapshot.Flags}'.");
        Assert.True(ContainsNodeEntry(resolvedSnapshot.ChangeVector, SiblingNode),
            userMessage: $"Expected resolved winner '{docId}' on hub-entry {HubEntry} to retain sibling lineage in its CV, but CV was '{resolvedSnapshot.ChangeVector ?? "<null>"}'.");

        var conflictPropagationMarkerId = $"markers/conflict-resolution-propagation/{Guid.NewGuid():N}";
        await StoreUserAsync(lab, node: HubEntry, conflictPropagationMarkerId, userName: "sync");
        hubEntryToHubInternal.Release();
        Assert.True(lab.WaitForDoc(HubInternal, conflictPropagationMarkerId, timeout: 60_000),
            userMessage: $"Expected sync marker '{conflictPropagationMarkerId}' from {HubEntry} to reach {HubInternal}.");

        Assert.True(lab.WaitForDocumentName(HubInternal, docId, expectedName: "flagged-winner", timeout: 60_000),
            userMessage: $"Expected resolved winner '{docId}' to reach observer node {HubInternal}.");

        var observerSnapshot = lab.GetDocumentSnapshot(HubInternal, docId);
        AssertFlagged(observerSnapshot.Flags, subject: $"resolved winner '{docId}' on observer {HubInternal}");
        Assert.True(observerSnapshot.Flags.Contain(DocumentFlags.Resolved),
            userMessage: $"Expected resolved winner '{docId}' on observer {HubInternal} to keep {nameof(DocumentFlags.Resolved)}, but flags were '{observerSnapshot.Flags}'.");
        Assert.True(ContainsNodeEntry(observerSnapshot.ChangeVector, SiblingNode),
            userMessage: $"Expected resolved winner '{docId}' on observer {HubInternal} to retain sibling lineage in its CV, but CV was '{observerSnapshot.ChangeVector ?? "<null>"}'.");
    }

    private static string GetDocId(string prefix) =>
        $"tickets/local-component-{prefix}";

    private static LineageNode GetObserverBaselineSource(LineageNode localNode) =>
        localNode == HubEntry ? SiblingNode : localNode;

    private static async Task SetDueMetadataAsync(NonDocumentLab lab, LineageNode node, string docId, MetadataUpdateMode mode, DateTime dueTime)
    {
        using var session = lab.StoreFor(node).OpenAsyncSession();
        var user = await session.LoadAsync<User>(docId);
        Assert.NotNull(user);

        var metadata = session.Advanced.GetMetadataFor(user);
        var value = dueTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
        switch (mode)
        {
            case MetadataUpdateMode.Refresh:
                metadata[Raven.Client.Constants.Documents.Metadata.Refresh] = value;
                break;
            case MetadataUpdateMode.Archival:
                metadata[Raven.Client.Constants.Documents.Metadata.ArchiveAt] = value;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }

        await session.SaveChangesAsync();
    }

    private static async Task<bool> ProcessMetadataUpdateAsync(NonDocumentLab lab, LineageNode node, MetadataUpdateMode mode, DateTime currentTime)
    {
        var database = lab.DatabaseFor(node);
        DatabaseTopology topology;
        string nodeTag;

        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
        using (serverContext.OpenReadTransaction())
        {
            topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
            nodeTag = database.ServerStore.NodeTag;
        }

        var isResponsibleNode = AbstractBackgroundWorkStorage.ShouldHandleWorkOnCurrentNode(topology, nodeTag);
        switch (mode)
        {
            case MetadataUpdateMode.Refresh:
                await RefreshHelper.SetupExpiration(
                    lab.StoreFor(node),
                    lab.ServerFor(node).ServerStore,
                    new Raven.Client.Documents.Operations.Refresh.RefreshConfiguration
                    {
                        Disabled = false,
                        RefreshFrequencyInSec = 3600
                    },
                    database.Name);

                database.Time.UtcDateTime = () => currentTime;
                await database.ExpiredDocumentsCleaner.RefreshDocs(throwOnError: true);
                return isResponsibleNode;
            case MetadataUpdateMode.Archival:
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var options = new BackgroundWorkParameters(context, currentTime, topology, nodeTag, AmountToTake: 16, MaxItemsToProcess: long.MaxValue);
                    var totalCount = 0;
                    var toArchive = database.DocumentsStorage.DataArchivalStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                    database.DocumentsStorage.DataArchivalStorage.ProcessDocuments(context, toArchive, currentTime);
                    context.Transaction.Commit();
                }

                return isResponsibleNode;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    }

    private static void AssertRefreshMetadataStillPending(NonDocumentLab lab, LineageNode node, string docId)
    {
        using var session = lab.StoreFor(node).OpenSession();
        var user = session.Load<User>(docId);
        Assert.NotNull(user);

        var metadata = session.Advanced.GetMetadataFor(user);
        Assert.True(metadata.ContainsKey(Raven.Client.Constants.Documents.Metadata.Refresh), userMessage: $"Expected '@refresh' to remain pending on '{docId}' at {node} in this harness.");
    }

    private static void AssertRefreshMetadataCleared(NonDocumentLab lab, LineageNode node, string docId)
    {
        using var session = lab.StoreFor(node).OpenSession();
        var user = session.Load<User>(docId);
        Assert.NotNull(user);

        var metadata = session.Advanced.GetMetadataFor(user);
        Assert.False(metadata.ContainsKey(Raven.Client.Constants.Documents.Metadata.Refresh), $"Expected '@refresh' to be cleared on '{docId}' at {node} after processing.");
    }

    private static void AssertArchivalMetadataStillPending(NonDocumentLab lab, LineageNode node, string docId)
    {
        using var session = lab.StoreFor(node).OpenSession();
        var user = session.Load<User>(docId);
        Assert.NotNull(user);

        var metadata = session.Advanced.GetMetadataFor(user);
        Assert.True(metadata.ContainsKey(Raven.Client.Constants.Documents.Metadata.ArchiveAt), userMessage: $"Expected '@archive-at' to remain pending on '{docId}' at {node} on non-responsible node.");
    }

    private static void WaitForFlaggedDocument(NonDocumentLab lab, LineageNode node, string docId, string subject)
    {
        Assert.True(lab.WaitForDoc(node, docId, timeout: 60_000), userMessage: $"Expected '{docId}' on {subject} node {node}.");
        var snapshot = lab.GetDocumentSnapshot(node, docId);
        Assert.True(snapshot.Exists, userMessage: $"Expected '{docId}' to exist on {subject} node {node}.");
        AssertFlagged(snapshot.Flags, $"{subject} document '{docId}' on {node}");
    }

    private void WaitForMetadataKey(NonDocumentLab lab, LineageNode node, string docId, string metadataKey)
    {
        Assert.True(WaitForValue(
                () => HasMetadataKey(lab, node, docId, metadataKey),
                expectedVal: true,
                timeout: 60_000),
            userMessage: $"Expected '{docId}' on {node} to contain metadata key '{metadataKey}' before processing the local metadata path.");
    }

    private async Task WaitForObserverSiblingBaseline(NonDocumentLab lab, LineageNode sourceNode, LineageNode observerNode, LineageNode siblingNode, string scenario)
    {
        var sourceDbCv = lab.GetDatabaseChangeVector(sourceNode);
        var expectedSiblingEtag = GetNodeEtag(sourceDbCv, siblingNode);

        Assert.True(expectedSiblingEtag.HasValue,
            userMessage: $"Expected source {sourceNode} DB CV to already include sibling node '{siblingNode}' before {scenario}, but source CV was '{sourceDbCv ?? "<null>"}'.");


        var baselineReached = WaitForValue(
            () =>
            {
                var current = GetNodeEtag(lab.GetDatabaseChangeVector(observerNode), siblingNode);
                return current.HasValue && current.Value >= expectedSiblingEtag.Value
                    ? expectedSiblingEtag.Value
                    : -1;
            },
            expectedVal: expectedSiblingEtag.Value,
            timeout: 60_000) == expectedSiblingEtag.Value;

        if (baselineReached == false && sourceNode != siblingNode)
        {

            await lab.ApplyHeartbeatChangeVectorAsync(source: siblingNode, target: observerNode);
            _ = WaitForValue(
                    () =>
                    {
                        var current = GetNodeEtag(lab.GetDatabaseChangeVector(observerNode), siblingNode);
                        return current.HasValue && current.Value >= expectedSiblingEtag.Value
                            ? expectedSiblingEtag.Value
                            : -1;
                    },
                    expectedVal: expectedSiblingEtag.Value,
                    timeout: 60_000) == expectedSiblingEtag.Value;
        }

        var observerDbCvAfter = lab.GetDatabaseChangeVector(observerNode);
        var actual = GetNodeEtag(observerDbCvAfter, siblingNode);


        Assert.True(actual.HasValue && actual.Value >= expectedSiblingEtag.Value,
            userMessage: $"Expected observer {observerNode} DB CV to reach at least sibling node '{siblingNode}' etag {expectedSiblingEtag.Value} before {scenario}, but actual was {actual?.ToString() ?? "<missing>"}. Source CV: '{sourceDbCv ?? "<null>"}'. Observer CV: '{observerDbCvAfter ?? "<null>"}'.");
    }

    private static async Task ReleaseToObserverAsync(NonDocumentLab lab, LineageNode source, InternalLinkBlocker blocker, string markerId)
    {
        await ReleaseLinkAndWaitAsync(lab, source: source, target: Observer, blocker: blocker, markerId: markerId);
    }

    private static async Task ReleaseLinkAndWaitAsync(NonDocumentLab lab, LineageNode source, LineageNode target, InternalLinkBlocker blocker, string markerId)
    {
        await StoreUserAsync(lab, source, markerId, "sync");
        blocker.Release();
        Assert.True(lab.WaitForDoc(target, markerId, timeout: 60_000), userMessage: $"Expected sync marker '{markerId}' from {source} to reach {target}.");
    }

    private static void AssertFlagged(DocumentFlags flags, string subject)
    {
        Assert.True((flags & DocumentFlags.FromFilteredPullReplicationHub) == DocumentFlags.FromFilteredPullReplicationHub,
            userMessage: $"Expected {subject} to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
    }

    private static void AssertNotContainsNodeEntry(string changeVector, LineageNode node, string subject)
    {
        Assert.True(ContainsNodeEntry(changeVector, node) == false,
            userMessage: $"Expected {subject} NOT to contain sibling node '{node}', but CV was '{changeVector ?? "<null>"}'.");
    }

    private static void AssertNodeEtagUnchanged(string expectedChangeVector, string actualChangeVector, LineageNode node, string actualSubject, string expectedSubject)
    {
        var expectedEtag = GetNodeEtag(expectedChangeVector, node);
        var actualEtag = GetNodeEtag(actualChangeVector, node);

        Assert.True(expectedEtag.HasValue && actualEtag.HasValue && expectedEtag.Value == actualEtag.Value,
            userMessage: $"Expected {actualSubject} to keep the '{node}' etag from {expectedSubject}, but expected {expectedEtag?.ToString() ?? "<missing>"} and got {actualEtag?.ToString() ?? "<missing>"}. Expected CV: '{expectedChangeVector ?? "<null>"}'. Actual CV: '{actualChangeVector ?? "<null>"}'.");
    }

    private static bool ContainsNodeEntry(string changeVector, LineageNode node)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return false;

        foreach (var entry in changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colonIndex = entry.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            if (string.Equals(entry[..colonIndex], node.ToString(), StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static bool HasMetadataKey(NonDocumentLab lab, LineageNode node, string docId, string metadataKey)
    {
        using var session = lab.StoreFor(node).OpenSession();
        var user = session.Load<User>(docId);
        if (user == null)
            return false;

        var metadata = session.Advanced.GetMetadataFor(user);
        return metadata.ContainsKey(metadataKey);
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

            if (string.Equals(entry[..colonIndex], node.ToString(), StringComparison.OrdinalIgnoreCase) == false)
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
