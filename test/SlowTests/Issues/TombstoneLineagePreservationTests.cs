using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class TombstoneLineagePreservationTests : TombstoneLineagePreservationTestBase
{
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
    public Task Document_DeleteRecreate_OnHubEntry_ShouldPreserveSiblingLineage(Options options) =>
        RunDocumentDeleteRecreateScenarioAsync(options, HubEntryTopology, "tickets/doc-hub-entry");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_DeleteRecreate_OnHubInternal_ShouldPreserveSiblingLineage(Options options) =>
        RunDocumentDeleteRecreateScenarioAsync(options, HubInternalTopology, "tickets/doc-hub-internal");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_DeleteRecreate_OnHubEntry_ShouldPreserveSiblingLineage(Options options) =>
        RunAttachmentDeleteRecreateScenarioAsync(options, HubEntryTopology, "tickets/attachment-hub-entry", "lineage.bin");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Attachment_DeleteRecreate_OnHubInternal_ShouldPreserveSiblingLineage(Options options) =>
        RunAttachmentDeleteRecreateScenarioAsync(options, HubInternalTopology, "tickets/attachment-hub-internal", "lineage.bin");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_OnHubEntry_ShouldPreserveSiblingLineage(Options options) =>
        RunTimeSeriesDeletedRangeScenarioAsync(options, HubEntryTopology, "tickets/ts-range-hub-entry", "HeartRate");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task TimeSeriesDeletedRange_OnHubInternal_ShouldPreserveSiblingLineage(Options options) =>
        RunTimeSeriesDeletedRangeScenarioAsync(options, HubInternalTopology, "tickets/ts-range-hub-internal", "HeartRate");

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_CreateWithNoPriorTombstone_OnHubEntry_ShouldNotBorrowLineage(Options options) =>
        RunFreshDocumentRegressionScenarioAsync(options);

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public Task Document_RecreateArrivesViaReplication_ShouldPreferReplicatedChangeVector(Options options) =>
        RunReplicatedRecreateRegressionScenarioAsync(options);

    private async Task RunDocumentDeleteRecreateScenarioAsync(Options options, ScenarioTopology topology, string docId)
    {
        await using var lab = await CreateLabAsync(options);
        using var firstBlocker = lab.BlockLink(topology.FirstBlockedLink.Source, topology.FirstBlockedLink.Target);
        using var secondBlocker = lab.BlockLink(topology.SecondBlockedLink.Source, topology.SecondBlockedLink.Target);

        await lab.WriteAndInjectTicketAsync(docId, topology.Writer, topology.HubEntry);

        Assert.True(
            lab.WaitForDoc(topology.Receiver, docId, timeout: 60_000),
            $"Expected '{docId}' to exist on {topology.Label} receiver {topology.Receiver}.");

        var injected = lab.GetDocumentSnapshot(topology.Receiver, docId);
        Assert.True(injected.Exists, $"Expected injected document '{docId}' on {topology.Receiver}.");
        AssertFlagged(injected.Flags, $"injected document '{docId}' on {topology.Receiver}");
        AssertContainsNodeEntry(injected.ChangeVector, topology.Writer, $"injected document '{docId}' on {topology.Receiver}");
        var receiverDbCvBeforeDelete = lab.GetDatabaseChangeVector(topology.Receiver);
        AssertDbCvBehindItemChangeVector(
            injected.ChangeVector,
            receiverDbCvBeforeDelete,
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} before document delete+recreate");

        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            session.Delete(docId);
            await session.SaveChangesAsync();
        }

        var tombstone = lab.GetDocumentTombstoneSnapshot(topology.Receiver, docId);
        Assert.True(tombstone.Exists, $"Expected document tombstone for '{docId}' on {topology.Receiver}.");
        AssertFlagged(tombstone.Flags, $"document tombstone '{docId}' on {topology.Receiver}");
        AssertContainsNodeEntry(tombstone.ChangeVector, topology.Writer, $"document tombstone '{docId}' on {topology.Receiver}");
        AssertNodeEtagUnchanged(
            receiverDbCvBeforeDelete,
            lab.GetDatabaseChangeVector(topology.Receiver),
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} after document delete",
            $"receiver DB CV on {topology.Receiver} before document delete");

        var recreatedName = $"recreated-on-{topology.Receiver}-{Guid.NewGuid():N}";
        using (var session = lab.StoreFor(topology.Receiver).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = recreatedName }, docId);
            await session.SaveChangesAsync();
        }

        var recreated = lab.GetDocumentSnapshot(topology.Receiver, docId);
        Assert.True(recreated.Exists, $"Expected recreated document '{docId}' on {topology.Receiver}.");
        Assert.Equal(recreatedName, recreated.Name);
        AssertFlagged(recreated.Flags, $"recreated document '{docId}' on {topology.Receiver}");
        AssertContainsNodeEntry(recreated.ChangeVector, topology.Writer, $"recreated document '{docId}' on {topology.Receiver}");
        AssertSameNodeEtag(
            tombstone.ChangeVector,
            recreated.ChangeVector,
            topology.Writer,
            $"recreated document '{docId}' on {topology.Receiver}",
            $"document tombstone '{docId}' on {topology.Receiver}");
        AssertNodeEtagUnchanged(
            receiverDbCvBeforeDelete,
            lab.GetDatabaseChangeVector(topology.Receiver),
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} after document recreate",
            $"receiver DB CV on {topology.Receiver} before document delete");

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

        var receiverDoc = lab.GetDocumentSnapshot(topology.Receiver, docId);
        Assert.True(receiverDoc.Exists, $"Expected attachment owner document '{docId}' on {topology.Receiver}.");
        AssertFlagged(receiverDoc.Flags, $"attachment owner document '{docId}' on {topology.Receiver}");
        AssertContainsNodeEntry(receiverDoc.ChangeVector, topology.Writer, $"attachment owner document '{docId}' on {topology.Receiver}");

        var existingAttachment = lab.GetAttachmentSnapshot(topology.Receiver, docId, attachmentName);
        Assert.True(existingAttachment.Exists, $"Expected attachment '{attachmentName}' on '{docId}' at {topology.Receiver}.");
        AssertContainsNodeEntry(existingAttachment.ChangeVector, topology.Writer, $"attachment '{attachmentName}' on '{docId}' at {topology.Receiver}");
        var receiverDbCvBeforeDelete = lab.GetDatabaseChangeVector(topology.Receiver);

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
        AssertContainsNodeEntry(tombstone.ChangeVector, topology.Writer, $"attachment tombstone '{attachmentName}' on '{docId}' at {topology.Receiver}");
        AssertNodeEtagUnchanged(
            receiverDbCvBeforeDelete,
            lab.GetDatabaseChangeVector(topology.Receiver),
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} after attachment delete",
            $"receiver DB CV on {topology.Receiver} before attachment delete");

        await lab.StoreFor(topology.Receiver).Operations.SendAsync(
            new PutAttachmentOperation(
                docId,
                attachmentName,
                new MemoryStream([9, 8, 7, 6]),
                existingAttachment.ContentType));

        var recreatedAttachment = lab.GetAttachmentSnapshot(topology.Receiver, docId, attachmentName);
        Assert.True(recreatedAttachment.Exists, $"Expected recreated attachment '{attachmentName}' on '{docId}' at {topology.Receiver}.");
        Assert.NotEqual(existingAttachment.Hash, recreatedAttachment.Hash);
        AssertContainsNodeEntry(recreatedAttachment.ChangeVector, topology.Writer, $"recreated attachment '{attachmentName}' on '{docId}' at {topology.Receiver}");
        AssertSameNodeEtag(
            tombstone.ChangeVector,
            recreatedAttachment.ChangeVector,
            topology.Writer,
            $"recreated attachment '{attachmentName}' on '{docId}' at {topology.Receiver}",
            $"attachment tombstone '{attachmentName}' on '{docId}' at {topology.Receiver}");
        AssertNodeEtagUnchanged(
            receiverDbCvBeforeDelete,
            lab.GetDatabaseChangeVector(topology.Receiver),
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} after attachment recreate",
            $"receiver DB CV on {topology.Receiver} before attachment delete");

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

        var receiverDoc = lab.GetDocumentSnapshot(topology.Receiver, docId);
        Assert.True(receiverDoc.Exists, $"Expected deleted-range owner document '{docId}' on {topology.Receiver}.");
        AssertFlagged(receiverDoc.Flags, $"deleted-range owner document '{docId}' on {topology.Receiver}");
        AssertContainsNodeEntry(receiverDoc.ChangeVector, topology.Writer, $"deleted-range owner document '{docId}' on {topology.Receiver}");
        var dbCvBeforeDelete = lab.GetDatabaseChangeVector(topology.Receiver);

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

        AssertContainsNodeEntry(deletedRange.ChangeVector, topology.Writer, $"deleted range '{timeSeriesName}' on '{docId}' at {topology.Receiver}");
        AssertSameNodeEtag(
            receiverDoc.ChangeVector,
            deletedRange.ChangeVector,
            topology.Writer,
            $"deleted range '{timeSeriesName}' on '{docId}' at {topology.Receiver}",
            $"flagged document '{docId}' on {topology.Receiver}");
        AssertNodeEtagUnchanged(
            dbCvBeforeDelete,
            lab.GetDatabaseChangeVector(topology.Receiver),
            topology.Writer,
            $"receiver DB CV on {topology.Receiver} after creating a deleted range",
            $"receiver DB CV on {topology.Receiver} before creating a deleted range");

        await lab.WriteSyncMarkerAndWaitAsync(topology.Receiver, topology.Peers);

        foreach (var peer in topology.Peers)
        {
            Assert.True(
                lab.WaitForDeletedRange(peer, docId, timeSeriesName, deletedRange.ChangeVector, timeout: 60_000),
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

        const string seededDocId = "tickets/seeded-flagged-doc";
        await lab.WriteAndInjectTicketAsync(seededDocId, LineageNode.A, LineageNode.B);

        var injected = lab.GetDocumentSnapshot(LineageNode.B, seededDocId);
        Assert.True(injected.Exists, $"Expected seeded flagged document '{seededDocId}' on B.");
        AssertFlagged(injected.Flags, $"seeded flagged document '{seededDocId}' on B");
        var dbCvBeforeFresh = lab.GetDatabaseChangeVector(LineageNode.B);
        AssertDbCvBehindItemChangeVector(
            injected.ChangeVector,
            dbCvBeforeFresh,
            LineageNode.A,
            "hub-entry DB CV on B before creating a fresh document");

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
        AssertSameNodeEtag(
            dbCvBeforeFresh,
            fresh.ChangeVector,
            LineageNode.A,
            $"fresh document '{freshDocId}' on B",
            "hub-entry DB CV on B before the fresh create");
        AssertNodeEtagGreater(
            injected.ChangeVector,
            fresh.ChangeVector,
            LineageNode.A,
            $"seeded flagged document '{seededDocId}' on B",
            $"fresh document '{freshDocId}' on B");
        AssertNotFlagged(fresh.Flags, $"fresh document '{freshDocId}' on B");

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
        Assert.Equal(recreatedOnA.ChangeVector, replicated.ChangeVector);
        var conflictCount = lab.GetConflictCount(LineageNode.B, docId);
        Assert.True(
            conflictCount == 0,
            $"Unexpected conflict on receiver B for replicated recreate '{docId}'. Conflict count: {conflictCount}.");
    }

    private static void AssertContainsNodeEntry(string changeVector, LineageNode node, string subject)
    {
        var needle = $"{node}:";
        Assert.True(
            changeVector?.Contains(needle, StringComparison.Ordinal) == true,
            $"Expected {subject} change vector to contain sibling entry '{needle}', but was '{changeVector ?? "<null>"}'.");
    }

    private static void AssertNotContainsNodeEntry(string changeVector, LineageNode node, string subject)
    {
        var needle = $"{node}:";
        Assert.True(
            changeVector?.Contains(needle, StringComparison.Ordinal) != true,
            $"Expected {subject} change vector NOT to contain entry '{needle}', but was '{changeVector ?? "<null>"}'.");
    }

    private static void AssertDbCvBehindItemChangeVector(string itemChangeVector, string databaseChangeVector, LineageNode sibling, string subject)
    {
        var itemEtag = GetNodeEtag(itemChangeVector, sibling);
        var dbCvEtag = GetNodeEtag(databaseChangeVector, sibling);

        Assert.True(
            itemEtag.HasValue && dbCvEtag.HasValue && dbCvEtag.Value < itemEtag.Value,
            $"Expected {subject} to stay behind sibling entry '{sibling}' (item etag: {itemEtag?.ToString() ?? "<missing>"}, DB CV etag: {dbCvEtag?.ToString() ?? "<missing>"}). Item CV: '{itemChangeVector ?? "<null>"}'. DB CV: '{databaseChangeVector ?? "<null>"}'.");
    }

    private static void AssertSameNodeEtag(string expectedChangeVector, string actualChangeVector, LineageNode node, string actualSubject, string expectedSubject)
    {
        var expectedEtag = GetNodeEtag(expectedChangeVector, node);
        var actualEtag = GetNodeEtag(actualChangeVector, node);

        Assert.True(
            expectedEtag.HasValue && actualEtag.HasValue && expectedEtag.Value == actualEtag.Value,
            $"Expected {actualSubject} to preserve the '{node}' etag from {expectedSubject}, but expected {expectedEtag?.ToString() ?? "<missing>"} and got {actualEtag?.ToString() ?? "<missing>"}. Expected CV: '{expectedChangeVector ?? "<null>"}'. Actual CV: '{actualChangeVector ?? "<null>"}'.");
    }

    private static void AssertNodeEtagUnchanged(string expectedChangeVector, string actualChangeVector, LineageNode node, string actualSubject, string expectedSubject)
    {
        var expectedEtag = GetNodeEtag(expectedChangeVector, node);
        var actualEtag = GetNodeEtag(actualChangeVector, node);

        Assert.True(
            expectedEtag.HasValue && actualEtag.HasValue && expectedEtag.Value == actualEtag.Value,
            $"Expected {actualSubject} to keep the '{node}' etag from {expectedSubject}, but expected {expectedEtag?.ToString() ?? "<missing>"} and got {actualEtag?.ToString() ?? "<missing>"}. Expected CV: '{expectedChangeVector ?? "<null>"}'. Actual CV: '{actualChangeVector ?? "<null>"}'.");
    }

    private static void AssertNodeEtagGreater(string greaterChangeVector, string smallerChangeVector, LineageNode node, string greaterSubject, string smallerSubject)
    {
        var greaterEtag = GetNodeEtag(greaterChangeVector, node);
        var smallerEtag = GetNodeEtag(smallerChangeVector, node);

        Assert.True(
            greaterEtag.HasValue && smallerEtag.HasValue && greaterEtag.Value > smallerEtag.Value,
            $"Expected '{node}' etag in {greaterSubject} to be greater than in {smallerSubject}, but got {greaterEtag?.ToString() ?? "<missing>"} and {smallerEtag?.ToString() ?? "<missing>"}. Greater CV: '{greaterChangeVector ?? "<null>"}'. Smaller CV: '{smallerChangeVector ?? "<null>"}'.");
    }

    private static void AssertFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(
            (flags & DocumentFlags.FromFilteredPullReplicationHub) == DocumentFlags.FromFilteredPullReplicationHub,
            $"Expected {subject} to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
    }

    private static void AssertNotFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(
            (flags & DocumentFlags.FromFilteredPullReplicationHub) == 0,
            $"Expected {subject} NOT to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
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
