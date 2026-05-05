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

namespace SlowTests.Issues;

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
    public async Task TimeSeriesAppend_ShouldReachObserver(Options options, bool onHubInternal)
    {
        const string timeSeriesName = "HeartRate";
        var baseline = new DateTime(2024, 03, 01, 00, 00, 00, DateTimeKind.Utc);
        var docId = GetDocId(prefix: "ts-append");

        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);
        await StoreUserAsync(lab, SiblingNode, docId, userName: "ts-owner");
        await AddTimeSeriesAsync(lab, docId, timeSeriesName, baseline, SiblingNode);
        await lab.InjectExistingTicketAsync(docId, SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForReplicatedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        Assert.True(lab.WaitForTimeSeries(node: HubEntry, docId, timeSeriesName, timeout: 60_000),
            userMessage: $"Expected '{timeSeriesName}' on '{docId}' at hub-entry {HubEntry}.");

        if (onHubInternal)
        {
            WaitForReplicatedDocument(lab, node: HubInternal, docId, subject: "hub-internal");
            Assert.True(lab.WaitForTimeSeries(node: HubInternal, docId, timeSeriesName, timeout: 60_000),
                userMessage: $"Expected '{timeSeriesName}' on '{docId}' at hub-internal {HubInternal}.");
        }

        await lab.ApplyHeartbeatChangeVectorAsync(baselineSource, Observer);

        using (var session = lab.StoreFor(localNode).OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Append(baseline.AddMinutes(1), 73.0, "bpm");
            await session.SaveChangesAsync();
        }

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/ts-append/{Guid.NewGuid():N}");

        Assert.True(lab.WaitForTimeSeries(node: Observer, docId, timeSeriesName, timeout: 60_000),
            userMessage: $"Expected local TS append '{timeSeriesName}' on '{docId}' from {localNode} to reach observer {Observer}.");
        WaitForReplicatedDocument(lab, node: Observer, docId, subject: "observer");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task CounterIncrement_ShouldReachObserver(Options options, bool onHubInternal)
    {
        const string counterName = "views";
        var docId = GetDocId(prefix: "counter-increment");

        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);
        await StoreUserAsync(lab, SiblingNode, docId, userName: "counter-owner");
        await AddCounterAsync(lab, docId, counterName, SiblingNode);
        await lab.InjectExistingTicketAsync(docId, SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForReplicatedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        Assert.True(lab.WaitForCounter(node: HubEntry, docId, counterName, expectedValue: 1, timeout: 60_000),
            userMessage: $"Expected counter '{counterName}' on '{docId}' at hub-entry {HubEntry}.");

        if (onHubInternal)
        {
            WaitForReplicatedDocument(lab, node: HubInternal, docId, subject: "hub-internal");
            Assert.True(lab.WaitForCounter(node: HubInternal, docId, counterName, expectedValue: 1, timeout: 60_000),
                userMessage: $"Expected counter '{counterName}' on '{docId}' at hub-internal {HubInternal}.");
        }

        await lab.ApplyHeartbeatChangeVectorAsync(baselineSource, Observer);

        using (var session = lab.StoreFor(localNode).OpenAsyncSession())
        {
            session.CountersFor(docId).Increment(counterName);
            await session.SaveChangesAsync();
        }

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/counter-increment/{Guid.NewGuid():N}");

        Assert.True(lab.WaitForCounter(node: Observer, docId, counterName, expectedValue: 2, timeout: 60_000),
            userMessage: $"Expected local counter increment '{counterName}' on '{docId}' from {localNode} to reach observer {Observer}.");

        WaitForReplicatedDocument(lab, node: Observer, docId, subject: "observer");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task NewAttachment_ShouldReachObserver(Options options, bool onHubInternal)
    {
        const string attachmentName = "local.bin";
        var docId = GetDocId(prefix: "attachment-put");

        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);
        await StoreUserAsync(lab, SiblingNode, docId, userName: "attachment-owner");
        await lab.InjectExistingTicketAsync(docId, SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;
        var baselineSource = GetObserverBaselineSource(localNode);

        WaitForReplicatedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        if (onHubInternal)
            WaitForReplicatedDocument(lab, node: HubInternal, docId, subject: "hub-internal");

        await lab.ApplyHeartbeatChangeVectorAsync(baselineSource, Observer);

        await lab.StoreFor(localNode).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7, 6]),
                contentType: "application/octet-stream"));

        var localAttachment = lab.GetAttachmentSnapshot(localNode, docId, attachmentName);
        Assert.True(localAttachment.Exists, userMessage: $"Expected local attachment '{attachmentName}' on '{docId}' at {localNode}.");

        await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/attachment-put/{Guid.NewGuid():N}");

        Assert.True(
            lab.WaitForAttachment(node: Observer, docId, attachmentName, localAttachment.Hash, timeout: 60_000),
            userMessage: $"Expected local attachment '{attachmentName}' on '{docId}' from {localNode} to reach observer {Observer}.");

        var observerAttachment = lab.GetAttachmentSnapshot(node: Observer, docId, attachmentName);
        Assert.True(observerAttachment.Exists, userMessage: $"Expected observer {Observer} to expose attachment '{attachmentName}' on '{docId}'.");
        WaitForReplicatedDocument(lab, node: Observer, docId, subject: "observer");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ArchivalMetadataUpdate_ShouldReplicateArchivedDocument(Options options, bool onHubInternal)
    {
        const MetadataUpdateMode mode = MetadataUpdateMode.Archival;
        var docId = GetDocId(prefix: $"metadata-{mode.ToString().ToLowerInvariant()}");
        var dueTime = DateTime.UtcNow.AddMinutes(-5);

        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubInternalToObserver = lab.BlockLink(source: HubInternal, target: Observer);
        await StoreUserAsync(lab, SiblingNode, docId, userName: $"{mode}-owner");
        await SetDueMetadataAsync(lab, SiblingNode, docId, mode, dueTime);
        await lab.InjectExistingTicketAsync(docId, SiblingNode, targetNode: HubEntry);

        var localNode = onHubInternal ? HubInternal : HubEntry;
        var observerBlocker = onHubInternal ? hubInternalToObserver : hubEntryToObserver;

        WaitForReplicatedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        if (onHubInternal)
            WaitForReplicatedDocument(lab, node: HubInternal, docId, subject: "hub-internal");

        WaitForMetadataKey(lab, localNode, docId, Raven.Client.Constants.Documents.Metadata.ArchiveAt);
        var isResponsibleNode = await ProcessMetadataUpdateAsync(lab, localNode, mode, dueTime.AddMinutes(1));

        var localSnapshot = lab.GetDocumentSnapshot(localNode, docId);
        Assert.True(localSnapshot.Exists, userMessage: $"Expected '{docId}' to exist on {localNode} after {mode} processing.");

        if (isResponsibleNode)
        {
            await ReleaseToObserverAsync(lab, localNode, observerBlocker, markerId: $"markers/metadata-{mode.ToString().ToLowerInvariant()}/{Guid.NewGuid():N}");
            WaitForReplicatedDocument(lab, node: Observer, docId, subject: "observer");
            return;
        }

        Assert.True(localSnapshot.Exists,
            userMessage: $"Expected non-responsible node '{localNode}' to keep '{docId}' intact while waiting for the responsible archival worker.");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ConflictResolution_ShouldReplicateWinningDocument(Options options)
    {
        var docId = GetDocId(prefix: "conflict-resolution");

        await using var lab = await CreateLabAsync(options);
        using var siblingToHubEntry = lab.BlockLink(source: SiblingNode, target: HubEntry);
        using var siblingToHubInternal = lab.BlockLink(source: SiblingNode, target: HubInternal);
        using var siblingToObserver = lab.BlockLink(source: SiblingNode, target: Observer);
        using var conflictToHubEntry = lab.BlockLink(source: Observer, target: HubEntry);
        using var conflictToSibling = lab.BlockLink(source: Observer, target: SiblingNode);
        using var conflictToHubInternal = lab.BlockLink(source: Observer, target: HubInternal);
        using var hubEntryToObserver = lab.BlockLink(source: HubEntry, target: Observer);
        using var hubEntryToHubInternal = lab.BlockLink(source: HubEntry, target: HubInternal);
        await StoreUserAsync(lab, node: Observer, docId, userName: "conflict-local");
        await StoreUserAsync(lab, SiblingNode, docId, userName: "protected-winner");
        await lab.InjectExistingTicketAsync(docId, SiblingNode, targetNode: HubEntry);

        WaitForReplicatedDocument(lab, node: HubEntry, docId, subject: "hub-entry");
        await lab.ApplyHeartbeatChangeVectorAsync(SiblingNode, HubInternal);
        await SetReplicationConflictResolutionAsync((DocumentStore)lab.StoreFor(node: HubEntry), StraightforwardConflictResolution.ResolveToLatest);

        await ReleaseLinkAndWaitAsync(
            lab,
            source: Observer,
            target: HubEntry,
            blocker: conflictToHubEntry,
            markerId: $"markers/conflict-resolution/{Guid.NewGuid():N}");

        Assert.Equal(
            0,
            WaitForValue(
                () => ((DocumentStore)lab.StoreFor(node: HubEntry)).Commands().GetConflictsForAsync(docId).GetAwaiter().GetResult().Length,
                expectedVal: 0,
                timeout: 60_000));

        Assert.True(lab.WaitForDocumentName(node: HubEntry, docId, expectedName: "protected-winner", timeout: 60_000),
            userMessage: $"Expected conflict winner on hub-entry {HubEntry} for '{docId}' to remain the bridged document.");

        await ReleaseLinkAndWaitAsync(
            lab,
            source: HubEntry,
            target: HubInternal,
            blocker: hubEntryToHubInternal,
            markerId: $"markers/conflict-resolution-propagation/{Guid.NewGuid():N}");

        Assert.True(
            lab.WaitForDocumentName(HubInternal, docId, expectedName: "protected-winner", timeout: 60_000),
            userMessage: $"Expected resolved winner '{docId}' to reach observer node {HubInternal}.");

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

    private static void WaitForReplicatedDocument(NonDocumentLab lab, LineageNode node, string docId, string subject)
    {
        Assert.True(lab.WaitForDoc(node, docId, timeout: 60_000), $"Expected '{docId}' on {subject} node {node}.");
        var snapshot = lab.GetDocumentSnapshot(node, docId);
        Assert.True(snapshot.Exists, $"Expected '{docId}' to exist on {subject} node {node}.");
    }

    private void WaitForMetadataKey(NonDocumentLab lab, LineageNode node, string docId, string metadataKey)
    {
        Assert.True(
            WaitForValue(
                () => HasMetadataKey(lab, node, docId, metadataKey),
                expectedVal: true,
                timeout: 60_000),
            $"Expected '{docId}' on {node} to contain metadata key '{metadataKey}' before processing the local metadata path.");
    }

    private static async Task ReleaseToObserverAsync(NonDocumentLab lab, LineageNode source, InternalLinkBlocker blocker, string markerId)
    {
        await ReleaseLinkAndWaitAsync(lab, source, Observer, blocker, markerId);
    }

    private static async Task ReleaseLinkAndWaitAsync(NonDocumentLab lab, LineageNode source, LineageNode target, InternalLinkBlocker blocker, string markerId)
    {
        await StoreUserAsync(lab, source, markerId, "sync");
        blocker.Release();
        Assert.True(lab.WaitForDoc(target, markerId, timeout: 60_000), $"Expected sync marker '{markerId}' from {source} to reach {target}.");
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

}
