using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class TombstoneLineagePreservationTests : TombstoneLineagePreservationTestBase
{
    private const int FilteredPullHandshakeTimeoutMs = 25_000;

    private sealed record ScenarioTopology(
        LineageNode Writer,
        LineageNode HubEntry,
        LineageNode Receiver,
        (LineageNode Source, LineageNode Target) FirstBlockedLink,
        (LineageNode Source, LineageNode Target) SecondBlockedLink,
        LineageNode[] Peers,
        string Label);

    private static readonly ScenarioTopology HubEntryTopology = new(
        Writer: LineageNode.A,
        HubEntry: LineageNode.B,
        Receiver: LineageNode.B,
        FirstBlockedLink: (LineageNode.A, LineageNode.B),
        SecondBlockedLink: (LineageNode.C, LineageNode.B),
        Peers: [LineageNode.A, LineageNode.C],
        Label: "hub-entry");

    private static readonly ScenarioTopology HubInternalTopology = new(
        Writer: LineageNode.A,
        HubEntry: LineageNode.B,
        Receiver: LineageNode.C,
        FirstBlockedLink: (LineageNode.A, LineageNode.B),
        SecondBlockedLink: (LineageNode.A, LineageNode.C),
        Peers: [LineageNode.A, LineageNode.B],
        Label: "hub-internal");

    public TombstoneLineagePreservationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_DeleteRecreate_OnHubEntry_ShouldReplicateRecreatedDocumentWithoutConflict(Options options) =>
        RunDocumentDeleteRecreateScenarioAsync(options, HubEntryTopology, "tickets/doc-hub-entry");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_DeleteRecreate_OnHubInternal_ShouldReplicateRecreatedDocumentWithoutConflict(Options options) =>
        RunDocumentDeleteRecreateScenarioAsync(options, HubInternalTopology, "tickets/doc-hub-internal");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_DeleteRecreate_OnHubEntry_ShouldReplicateRecreatedAttachmentWithoutConflict(Options options) =>
        RunAttachmentDeleteRecreateScenarioAsync(options, HubEntryTopology, "tickets/attachment-hub-entry", "lineage.bin");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_DeleteRecreate_OnHubInternal_ShouldReplicateRecreatedAttachmentWithoutConflict(Options options) =>
        RunAttachmentDeleteRecreateScenarioAsync(options, HubInternalTopology, "tickets/attachment-hub-internal", "lineage.bin");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_OnHubEntry_ShouldReplicateDeletedRangeWithoutConflict(Options options) =>
        RunTimeSeriesDeletedRangeScenarioAsync(options, HubEntryTopology, "tickets/ts-range-hub-entry", "HeartRate");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_OnHubInternal_ShouldReplicateDeletedRangeWithoutConflict(Options options) =>
        RunTimeSeriesDeletedRangeScenarioAsync(options, HubInternalTopology, "tickets/ts-range-hub-internal", "HeartRate");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_CreateWithNoPriorTombstone_OnHubEntry_ShouldReplicateFreshDocumentWithoutConflict(Options options) =>
        RunFreshDocumentRegressionScenarioAsync(options);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_RecreateArrivesViaReplication_ShouldReachReceiverWithoutConflict(Options options) =>
        RunReplicatedRecreateRegressionScenarioAsync(options);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task DocumentTombstone_FilteredPullHandshake_ShouldStillDeliverTombstone(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore("filtered-pull-document-tombstone");

        const string docId = "tickets/filtered-pull-document-tombstone";
        await CreateDocumentTombstoneOnStoreAsync(source, docId);
        await lab.PreconditionFullDatabaseChangeVectorFromStoreAsync(source, LineageNode.A);

        await lab.ConnectSinkToHubAsync(source, LineageNode.A);

        var delivered = WaitForValue(
            () => lab.GetDocumentTombstoneSnapshot(LineageNode.A, docId).Exists,
            expectedVal: true,
            timeout: FilteredPullHandshakeTimeoutMs);

        Assert.True(
            delivered,
            $"Expected filtered sink-to-hub replication to deliver document tombstone '{docId}' after the hub had been preconditioned with the same source identity. " +
            "If this fails, a legitimate document tombstone was skipped before it reached the hub.");
    }

    private async Task RunDocumentDeleteRecreateScenarioAsync(Options options, ScenarioTopology topology, string docId)
    {
        await using var lab = await CreateLabAsync(options);
        using var firstBlocker = lab.BlockLink(topology.FirstBlockedLink.Source, topology.FirstBlockedLink.Target);
        using var secondBlocker = lab.BlockLink(topology.SecondBlockedLink.Source, topology.SecondBlockedLink.Target);
        await lab.WriteAndInjectTicketAsync(docId, topology.Writer, topology.HubEntry);

        Assert.True(
            lab.WaitForDoc(topology.Receiver, docId, timeout: 60_000),
            $"Expected '{docId}' to exist on {topology.Label} receiver {topology.Receiver}.");

        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            session.Delete(docId);
            await session.SaveChangesAsync();
        }

        var tombstone = lab.GetDocumentTombstoneSnapshot(topology.Receiver, docId);
        Assert.True(tombstone.Exists, $"Expected document tombstone for '{docId}' on {topology.Receiver}.");

        var recreatedName = $"recreated-on-{topology.Receiver}-{Guid.NewGuid():N}";
        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = recreatedName }, docId);
            await session.SaveChangesAsync();
        }

        var recreated = lab.GetDocumentSnapshot(topology.Receiver, docId);
        Assert.True(recreated.Exists, $"Expected recreated document '{docId}' on {topology.Receiver}.");
        Assert.Equal(recreatedName, recreated.Name);

        foreach (var peer in topology.Peers)
        {
            Assert.True(
                lab.WaitForDocumentName(peer, docId, recreatedName, timeout: 60_000),
                $"Expected recreated document '{docId}' from {topology.Receiver} to reach peer {peer} without a conflict.");

            var conflictCount = lab.GetConflictCount(peer, docId);
            Assert.True(
                conflictCount == 0,
                $"Spurious conflict on peer {peer} for recreated document '{docId}' after delete+recreate on {topology.Receiver}. Conflict count: {conflictCount}.");
        }
    }

    private async Task RunAttachmentDeleteRecreateScenarioAsync(Options options, ScenarioTopology topology, string docId, string attachmentName)
    {
        await using var lab = await CreateLabAsync(options);
        using var firstBlocker = lab.BlockLink(topology.FirstBlockedLink.Source, topology.FirstBlockedLink.Target);
        using var secondBlocker = lab.BlockLink(topology.SecondBlockedLink.Source, topology.SecondBlockedLink.Target);
        using (var session = lab.StoreFor(topology.Writer).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"attachment-source-{topology.Writer}" }, docId);
            await session.SaveChangesAsync();
        }

        await lab.StoreFor(topology.Writer).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([1, 2, 3, 4]),
                "application/octet-stream"));

        await lab.InjectExistingTicketAsync(docId, topology.Writer, topology.HubEntry);

        Assert.True(
            lab.WaitForDoc(topology.Receiver, docId, timeout: 60_000),
            $"Expected attachment owner document '{docId}' on {topology.Receiver}.");
        Assert.True(
            lab.WaitForAttachment(topology.Receiver, docId, attachmentName, timeout: 60_000),
            $"Expected attachment '{attachmentName}' on '{docId}' at {topology.Receiver}.");

        var existingAttachment = lab.GetAttachmentSnapshot(topology.Receiver, docId, attachmentName);
        Assert.True(existingAttachment.Exists, $"Expected attachment '{attachmentName}' on '{docId}' at {topology.Receiver}.");

        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete(docId, attachmentName);
            await session.SaveChangesAsync();
        }

        var tombstone = lab.GetAttachmentTombstoneSnapshot(
            topology.Receiver,
            docId,
            attachmentName,
            existingAttachment.Hash,
            existingAttachment.ContentType);
        Assert.True(tombstone.Exists, $"Expected attachment tombstone for '{attachmentName}' on '{docId}' at {topology.Receiver}.");

        await lab.StoreFor(topology.Receiver).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7, 6]),
                existingAttachment.ContentType));

        var recreatedAttachment = lab.GetAttachmentSnapshot(topology.Receiver, docId, attachmentName);
        Assert.True(recreatedAttachment.Exists, $"Expected recreated attachment '{attachmentName}' on '{docId}' at {topology.Receiver}.");
        Assert.NotEqual(existingAttachment.Hash, recreatedAttachment.Hash);

        await lab.WriteSyncMarkerAndWaitAsync(topology.Receiver, topology.Peers);

        foreach (var peer in topology.Peers)
        {
            Assert.True(
                lab.WaitForAttachment(peer, docId, attachmentName, recreatedAttachment.Hash, timeout: 60_000),
                $"Expected recreated attachment '{attachmentName}' from {topology.Receiver} to reach peer {peer}.");

            var conflictCount = lab.GetConflictCount(peer, docId);
            Assert.True(
                conflictCount == 0,
                $"Spurious conflict on peer {peer} for attachment '{attachmentName}' on '{docId}' after delete+recreate on {topology.Receiver}. Conflict count: {conflictCount}.");
        }
    }

    private async Task RunTimeSeriesDeletedRangeScenarioAsync(Options options, ScenarioTopology topology, string docId, string timeSeriesName)
    {
        await using var lab = await CreateLabAsync(options);
        using var firstBlocker = lab.BlockLink(topology.FirstBlockedLink.Source, topology.FirstBlockedLink.Target);
        using var secondBlocker = lab.BlockLink(topology.SecondBlockedLink.Source, topology.SecondBlockedLink.Target);
        await lab.WriteAndInjectTicketAsync(docId, topology.Writer, topology.HubEntry);

        Assert.True(
            lab.WaitForDoc(topology.Receiver, docId, timeout: 60_000),
            $"Expected '{docId}' to exist on {topology.Receiver} before creating a deleted range.");

        var from = new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var to = from.AddHours(1);
        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            session.TimeSeriesFor(docId, timeSeriesName).Delete(from, to);
            await session.SaveChangesAsync();
        }

        Assert.True(
            lab.WaitForDeletedRange(topology.Receiver, docId, timeSeriesName, timeout: 60_000),
            $"Expected a time-series deleted range for '{timeSeriesName}' on '{docId}' at {topology.Receiver}.");

        var deletedRange = lab.GetDeletedRangeSnapshots(topology.Receiver, docId)
            .Where(x => string.Equals(x.Name, timeSeriesName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(x => x.Etag)
            .First();

        await lab.WriteSyncMarkerAndWaitAsync(topology.Receiver, topology.Peers);

        foreach (var peer in topology.Peers)
        {
            Assert.True(
                lab.WaitForDeletedRange(peer, docId, timeSeriesName, timeout: 60_000),
                $"Expected deleted range '{timeSeriesName}' from {topology.Receiver} to reach peer {peer}.");

            var conflictCount = lab.GetConflictCount(peer, docId);
            Assert.True(
                conflictCount == 0,
                $"Spurious conflict on peer {peer} for time-series deleted range '{timeSeriesName}' on '{docId}' after local delete on {topology.Receiver}. Conflict count: {conflictCount}.");
        }
    }

    private async Task RunFreshDocumentRegressionScenarioAsync(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var writerToHubEntry = lab.BlockLink(LineageNode.A, LineageNode.B);
        using var observerToHubEntry = lab.BlockLink(LineageNode.C, LineageNode.B);
        const string seededDocId = "tickets/seeded-protected-doc";
        await lab.WriteAndInjectTicketAsync(seededDocId, LineageNode.A, LineageNode.B);

        var injected = lab.GetDocumentSnapshot(LineageNode.B, seededDocId);
        Assert.True(injected.Exists, $"Expected seeded bridged document '{seededDocId}' on B.");

        const string freshDocId = "tickets/fresh-local-no-tombstone";
        var freshName = $"fresh-on-B-{Guid.NewGuid():N}";
        using (var session = lab.StoreFor(LineageNode.B).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = freshName }, freshDocId);
            await session.SaveChangesAsync();
        }

        var fresh = lab.GetDocumentSnapshot(LineageNode.B, freshDocId);
        Assert.True(fresh.Exists, $"Expected fresh document '{freshDocId}' on B.");
        Assert.Equal(freshName, fresh.Name);

        foreach (var peer in new[] { LineageNode.A, LineageNode.C })
        {
            Assert.True(
                lab.WaitForDocumentName(peer, freshDocId, freshName, timeout: 60_000),
                $"Expected fresh document '{freshDocId}' from B to reach peer {peer}.");

            var conflictCount = lab.GetConflictCount(peer, freshDocId);
            Assert.True(
                conflictCount == 0,
                $"Unexpected conflict on peer {peer} for fresh document '{freshDocId}'. Conflict count: {conflictCount}.");
        }
    }

    private async Task RunReplicatedRecreateRegressionScenarioAsync(Options options)
    {
        await using var lab = await CreateLabAsync(options);

        const string docId = "tickets/recreate-via-replication";
        await lab.WriteAndInjectTicketAsync(docId, LineageNode.C, LineageNode.A);

        Assert.True(
            lab.WaitForDoc(LineageNode.B, docId, timeout: 60_000),
            $"Expected bridged document '{docId}' from A to reach receiver B.");

        var bridgedOnB = lab.GetDocumentSnapshot(LineageNode.B, docId);
        Assert.True(bridgedOnB.Exists, $"Expected bridged document '{docId}' on B.");

        using (var session = lab.StoreFor(LineageNode.B).OpenAsyncSession())
        {
            session.Delete(docId);
            await session.SaveChangesAsync();
        }

        Assert.True(
            WaitForValue(
                () => lab.GetDocumentTombstoneSnapshot(LineageNode.A, docId).Exists,
                expectedVal: true,
                timeout: 60_000),
            $"Expected delete of '{docId}' from B to reach A before recreating it there.");

        var tombstoneOnA = lab.GetDocumentTombstoneSnapshot(LineageNode.A, docId);
        Assert.True(tombstoneOnA.Exists, $"Expected replicated tombstone for '{docId}' on A.");

        var recreatedName = $"recreated-on-A-{Guid.NewGuid():N}";
        using (var session = lab.StoreFor(LineageNode.A).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = recreatedName }, docId);
            await session.SaveChangesAsync();
        }

        var recreatedOnA = lab.GetDocumentSnapshot(LineageNode.A, docId);
        Assert.True(recreatedOnA.Exists, $"Expected recreated document '{docId}' on A.");
        Assert.Equal(recreatedName, recreatedOnA.Name);

        Assert.True(
            lab.WaitForDocumentName(LineageNode.B, docId, recreatedName, timeout: 60_000),
            $"Expected replicated recreate of '{docId}' from A to reach receiver B.");

        var replicated = lab.GetDocumentSnapshot(LineageNode.B, docId);
        Assert.True(replicated.Exists, $"Expected replicated document '{docId}' on receiver B.");
        Assert.Equal(recreatedName, replicated.Name);
        var conflictCount = lab.GetConflictCount(LineageNode.B, docId);
        Assert.True(
            conflictCount == 0,
            $"Unexpected conflict on receiver B for replicated recreate '{docId}'. Conflict count: {conflictCount}.");
    }

    private static async Task CreateDocumentTombstoneOnStoreAsync(IDocumentStore store, string docId)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"tombstone-source-{Guid.NewGuid():N}" }, docId);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Delete(docId);
            await session.SaveChangesAsync();
        }
    }
}
