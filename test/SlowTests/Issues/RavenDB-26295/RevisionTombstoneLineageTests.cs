using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public class RevisionTombstoneLineageTests : TombstoneLineagePreservationTestBase
{
    private const string SubjectDocId = "tickets/revision-tombstone";

    public RevisionTombstoneLineageTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(Data = [true])]
    [RavenData(Data = [false])]
    public async Task InternalReplication_ShouldStoreEquivalentKeys(bool seedFilteredLineage)
    {
        await using var lab = await CreateLabAsync(new Options());
        using var writerToSource = lab.BlockLink(source: LineageNode.C, target: LineageNode.A);
        using var writerToReceiver = lab.BlockLink(source: LineageNode.C, target: LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var sourceSnapshots = await CreateSourceRevisionTombstonesAsync(lab, seedFilteredLineage);

        if (seedFilteredLineage)
            Assert.Contains(sourceSnapshots, snapshot => ContainsNodeEntry(snapshot.KeyChangeVector, LineageNode.C));

        await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, blocker: sourceToReceiver);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

        var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
        AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(Data = [true])]
    [RavenData(Data = [false])]
    public async Task FilteredPullReplication_ShouldStoreEquivalentKeys(bool seedFilteredLineage)
    {
        await using var lab = await CreateLabAsync(new Options());
        using var writerToSource = lab.BlockLink(source: LineageNode.C, target: LineageNode.A);
        using var writerToReceiver = lab.BlockLink(source: LineageNode.C, target: LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var sourceSnapshots = await CreateSourceRevisionTombstonesAsync(lab, seedFilteredLineage);

        if (seedFilteredLineage)
            Assert.Contains(sourceSnapshots, snapshot => ContainsNodeEntry(snapshot.KeyChangeVector, LineageNode.C));

        await lab.InjectExistingTicketAsync(SubjectDocId, sourceNode: LineageNode.A, targetNode: LineageNode.B);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

        var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
        AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(Data = [false, false])]
    [RavenData(Data = [true, false])]
    [RavenData(Data = [false, true])]
    public async Task DirectFilteredRevisionTombstone_ShouldKeepSinkDbIdOutOfDbCvAndStoreEquivalentKeys(bool assertInternalReceiverDbCv, bool assertFilteredReceiverDbCv)
    {
        await using var lab = await CreateLabAsync(new Options());
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.A, PullReplicationMode.SinkToHub);
        await externalSink.Maintenance.SendAsync(new ConfigureRevisionsOperation(CreateRevisionsConfiguration()));

        await CreateDirectRevisionTombstonesOnExternalStoreAsync(externalSink);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.A, SubjectDocId, expectedCount: 1, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach hub A from the external sink.");

        var externalDatabase = await GetDocumentDatabaseInstanceForAsync(externalSink, RavenDatabaseMode.Single, SubjectDocId);
        var externalDbId = externalDatabase.DbBase64Id;

        var sourceSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.A, SubjectDocId);
        Assert.NotEmpty(sourceSnapshots);
        Assert.Contains(sourceSnapshots, snapshot => ContainsDatabaseId(snapshot.KeyChangeVector, externalDbId));

        var hubDbCv = lab.GetDatabaseChangeVector(LineageNode.A);
        Assert.False(ContainsDatabaseId(changeVector: hubDbCv, externalDbId),
            userMessage: $"Expected hub A DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{hubDbCv}'.");

        if (assertInternalReceiverDbCv)
        {
            var receiverDbCvBefore = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: receiverDbCvBefore, externalDbId),
                userMessage: $"Expected receiver B DB CV before internal replication to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvBefore}'.");

            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected direct filtered-pull revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var receiverDbCvAfter = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: receiverDbCvAfter, externalDbId),
                userMessage: $"Expected internal receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvAfter}'.");
        }

        if (assertFilteredReceiverDbCv)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, sourceNode: LineageNode.A, targetNode: LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected direct filtered-pull revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var filteredReceiverDbCv = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(
                ContainsDatabaseId(changeVector: filteredReceiverDbCv, externalDbId),
                userMessage: $"Expected filtered-pull receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{filteredReceiverDbCv}'.");
        }
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(Data = [true, false])]
    [RavenData(Data = [false, true])]
    public async Task LocalRevisionTombstone_OnUnknownSinkLineageWithoutSiblingEntries_ShouldStoreEquivalentKeys(bool assertInternalReceiver, bool assertFilteredReceiver)
    {
        await using var lab = await CreateLabAsync(new Options());
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.A, PullReplicationMode.SinkToHub);
        var externalDatabase = await GetDocumentDatabaseInstanceForAsync(externalSink, RavenDatabaseMode.Single, SubjectDocId);
        var externalDbId = externalDatabase.DbBase64Id;

        await SeedUnknownSinkDocumentOnHubAsync(lab, externalSink);

        var sourceDoc = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
        Assert.True(sourceDoc.Exists, userMessage: $"Expected unknown-sink document '{SubjectDocId}' on A.");
        AssertNotFlagged(sourceDoc.Flags);
        Assert.True(ContainsDatabaseId(sourceDoc.ChangeVector, externalDbId),
            userMessage: $"Expected unknown-sink document '{SubjectDocId}' on A to preserve external sink dbId '{externalDbId}', but CV was '{sourceDoc.ChangeVector}'.");

        var hubDbCv = lab.GetDatabaseChangeVector(LineageNode.A);
        Assert.False(
            ContainsDatabaseId(changeVector: hubDbCv, externalDbId),
            userMessage: $"Expected hub A DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{hubDbCv}'.");

        var sourceSnapshots = await CreateLocalHubRevisionTombstonesAsync(lab);
        Assert.Contains(
            sourceSnapshots,
            snapshot => ContainsDatabaseId(snapshot.KeyChangeVector, externalDbId));

        if (assertInternalReceiver)
        {
            var receiverDbCvBefore = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(
                ContainsDatabaseId(changeVector: receiverDbCvBefore, externalDbId),
                userMessage: $"Expected receiver B DB CV before internal replication to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvBefore}'.");

            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected local unknown-sink revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var receiverDbCvAfter = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: receiverDbCvAfter, externalDbId),
                userMessage: $"Expected internal receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvAfter}'.");
        }

        if (assertFilteredReceiver)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, sourceNode: LineageNode.A, targetNode: LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected local unknown-sink revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var filteredReceiverDbCv = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: filteredReceiverDbCv, externalDbId),
                userMessage: $"Expected filtered-pull receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{filteredReceiverDbCv}'.");
        }
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(Data = [true, false])]
    [RavenData(Data = [false, true])]
    public async Task LocalRevisionTombstone_OnUnknownSinkLineageWithSiblingEntries_ShouldStoreEquivalentKeys(bool assertInternalReceiver, bool assertFilteredReceiver)
    {
        await using var lab = await CreateLabAsync(new Options());
        using var writerToSource = lab.BlockLink(source: LineageNode.C, target: LineageNode.A);
        using var writerToReceiver = lab.BlockLink(source: LineageNode.C, target: LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.C, PullReplicationMode.HubToSink);
        await lab.ConfigureExternalSinkConnectionAsync(externalSink, LineageNode.A, PullReplicationMode.SinkToHub);

        var externalDatabase = await GetDocumentDatabaseInstanceForAsync(externalSink, RavenDatabaseMode.Single, SubjectDocId);
        var externalDbId = externalDatabase.DbBase64Id;

        var sourceDoc = await SeedFlaggedUnknownSinkDocumentOnHubAsync(lab, externalSink);
        Assert.True(sourceDoc.Exists, userMessage: $"Expected flagged unknown-sink document '{SubjectDocId}' on A.");
        AssertFlagged(sourceDoc.Flags);
        Assert.True(ContainsDatabaseId(sourceDoc.ChangeVector, externalDbId),
            userMessage: $"Expected flagged unknown-sink document '{SubjectDocId}' on A to preserve external sink dbId '{externalDbId}', but CV was '{sourceDoc.ChangeVector}'.");
        Assert.True(ContainsNodeEntry(sourceDoc.ChangeVector, LineageNode.C),
            userMessage: $"Expected flagged unknown-sink document '{SubjectDocId}' on A to preserve sibling node '{LineageNode.C}', but CV was '{sourceDoc.ChangeVector}'.");

        var hubDbCv = lab.GetDatabaseChangeVector(LineageNode.A);
        Assert.False(ContainsDatabaseId(changeVector: hubDbCv, externalDbId),
            userMessage: $"Expected hub A DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{hubDbCv}'.");

        var sourceSnapshots = await CreateLocalHubRevisionTombstonesAsync(lab);
        Assert.Contains(sourceSnapshots, snapshot => ContainsDatabaseId(snapshot.KeyChangeVector, externalDbId));
        Assert.Contains(sourceSnapshots, snapshot => ContainsNodeEntry(snapshot.KeyChangeVector, LineageNode.C));

        if (assertInternalReceiver)
        {
            var receiverDbCvBefore = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: receiverDbCvBefore, externalDbId),
                userMessage: $"Expected receiver B DB CV before internal replication to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvBefore}'.");

            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected maximal-lineage revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var receiverDbCvAfter = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: receiverDbCvAfter, externalDbId),
                userMessage: $"Expected internal receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{receiverDbCvAfter}'.");
        }

        if (assertFilteredReceiver)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, sourceNode: LineageNode.A, targetNode: LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected maximal-lineage revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);

            var filteredReceiverDbCv = lab.GetDatabaseChangeVector(LineageNode.B);
            Assert.False(ContainsDatabaseId(changeVector: filteredReceiverDbCv, externalDbId),
                userMessage: $"Expected filtered-pull receiver B DB CV to stay free of external sink dbId '{externalDbId}', but DB CV was '{filteredReceiverDbCv}'.");
        }
    }

    private static RevisionsConfiguration CreateRevisionsConfiguration() =>
        new() { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };

    private static bool ContainsNodeEntry(string changeVector, LineageNode node) =>
        changeVector?.Contains($"{node}:", StringComparison.Ordinal) == true;

    private static void AssertFlagged(DocumentFlags flags) =>
        Assert.True((flags & DocumentFlags.FromFilteredPullReplicationHub) == DocumentFlags.FromFilteredPullReplicationHub,
            userMessage: $"Expected flags to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");

    private static void AssertNotFlagged(DocumentFlags flags) =>
        Assert.True((flags & DocumentFlags.FromFilteredPullReplicationHub) == 0,
            userMessage: $"Expected flags NOT to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");

    private static async Task<List<RevisionTombstoneSnapshot>> CreateSourceRevisionTombstonesAsync(LineageLab lab, bool seedFilteredLineage)
    {
        if (seedFilteredLineage)
        {
            await lab.WriteAndInjectTicketAsync(SubjectDocId, sourceNode: LineageNode.C, targetNode: LineageNode.A);

            Assert.True(lab.WaitForDoc(LineageNode.A, SubjectDocId, timeout: 60_000),
                userMessage: $"Expected seeded document '{SubjectDocId}' to arrive on A via filtered pull.");

            var seeded = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
            Assert.True(seeded.Exists, userMessage: $"Expected seeded document '{SubjectDocId}' on A.");
            AssertFlagged(seeded.Flags);
        }
        else
        {
            using (var session = lab.StoreFor(LineageNode.A).OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"local-seed-{Guid.NewGuid():N}" }, SubjectDocId);
                await session.SaveChangesAsync();
            }

            var localSeed = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
            Assert.True(localSeed.Exists, userMessage: $"Expected local seed document '{SubjectDocId}' on A.");
            AssertNotFlagged(localSeed.Flags);
        }

        for (var i = 0; i < 2; i++)
        {
            using var session = lab.StoreFor(LineageNode.A).OpenAsyncSession();
            var doc = await session.LoadAsync<User>(SubjectDocId);
            Assert.NotNull(doc);
            doc.Name = $"revision-{i}-{Guid.NewGuid():N}";
            await session.SaveChangesAsync();
        }

        await lab.StoreFor(LineageNode.A).Maintenance.SendAsync(new DeleteRevisionsOperation(documentIds: [SubjectDocId]));

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.A, SubjectDocId, expectedCount: 1, timeout: 60_000),
            userMessage: $"Expected local revision tombstones for '{SubjectDocId}' on A.");

        var snapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.A, SubjectDocId)
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(snapshots);
        return snapshots;
    }

    private static async Task CreateDirectRevisionTombstonesOnExternalStoreAsync(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"external-seed-{Guid.NewGuid():N}" }, SubjectDocId);
            await session.SaveChangesAsync();
        }

        for (var i = 0; i < 2; i++)
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<User>(SubjectDocId);
            Assert.NotNull(doc);
            doc.Name = $"external-revision-{i}-{Guid.NewGuid():N}";
            await session.SaveChangesAsync();
        }

        await store.Maintenance.SendAsync(new DeleteRevisionsOperation(documentIds: [SubjectDocId]));
    }

    private static async Task SeedUnknownSinkDocumentOnHubAsync(LineageLab lab, IDocumentStore externalSink)
    {
        using (var session = externalSink.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"unknown-sink-{Guid.NewGuid():N}" }, SubjectDocId);
            await session.SaveChangesAsync();
        }

        Assert.True(lab.WaitForDoc(LineageNode.A, SubjectDocId, timeout: 60_000),
            userMessage: $"Expected unknown-sink document '{SubjectDocId}' to arrive on A.");
    }

    private async Task<DocumentSnapshot> SeedFlaggedUnknownSinkDocumentOnHubAsync(LineageLab lab, IDocumentStore externalSink)
    {
        using (var session = lab.StoreFor(LineageNode.C).OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"sibling-seed-{Guid.NewGuid():N}" }, SubjectDocId);
            await session.SaveChangesAsync();
        }

        Assert.True(WaitForDocument(externalSink, SubjectDocId, timeout: 60_000),
            userMessage: $"Expected sibling-seeded document '{SubjectDocId}' to arrive on the external sink from hub C.");

        var modifiedName = $"external-plus-sibling-{Guid.NewGuid():N}";
        using (var session = externalSink.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(SubjectDocId);
            Assert.NotNull(doc);
            doc.Name = modifiedName;
            await session.SaveChangesAsync();
        }

        Assert.True(WaitForDocument<User>(lab.StoreFor(LineageNode.A), SubjectDocId, user => user.Name == modifiedName, timeout: 60_000),
            userMessage: $"Expected flagged unknown-sink document '{SubjectDocId}' to arrive on A after the external sink modification.");

        return lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
    }

    private static async Task<List<RevisionTombstoneSnapshot>> CreateLocalHubRevisionTombstonesAsync(LineageLab lab)
    {
        for (var i = 0; i < 2; i++)
        {
            using var session = lab.StoreFor(LineageNode.A).OpenAsyncSession();
            var doc = await session.LoadAsync<User>(SubjectDocId);
            Assert.NotNull(doc);
            doc.Name = $"hub-local-revision-{i}-{Guid.NewGuid():N}";
            await session.SaveChangesAsync();
        }

        await lab.StoreFor(LineageNode.A).Maintenance.SendAsync(new DeleteRevisionsOperation(documentIds: [SubjectDocId]));

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.A, SubjectDocId, expectedCount: 1, timeout: 60_000),
            userMessage: $"Expected local hub revision tombstones for '{SubjectDocId}' on A.");

        return lab.GetRevisionTombstoneSnapshots(LineageNode.A, SubjectDocId)
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .ToList();
    }

    private static void AssertEquivalentRevisionTombstoneSets(
        List<RevisionTombstoneSnapshot> expected,
        List<RevisionTombstoneSnapshot> actual)
    {
        var expectedKeys = expected
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .Select(snapshot => snapshot.RawKey)
            .ToArray();
        var actualKeys = actual
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .Select(snapshot => snapshot.RawKey)
            .ToArray();

        Assert.Equal(
            expectedKeys,
            actualKeys);

        var expectedRows = expected
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .Select(snapshot => snapshot.RowChangeVector)
            .ToArray();
        var actualRows = actual
            .OrderBy(snapshot => snapshot.RawKey, StringComparer.Ordinal)
            .Select(snapshot => snapshot.RowChangeVector)
            .ToArray();

        Assert.Equal(
            expectedRows,
            actualRows);
    }

    private static bool ContainsDatabaseId(string changeVector, string dbId)
    {
        if (string.IsNullOrWhiteSpace(changeVector) || string.IsNullOrWhiteSpace(dbId))
            return false;

        foreach (var entry in changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var dashIndex = entry.IndexOf('-', StringComparison.Ordinal);
            if (dashIndex < 0)
                continue;

            if (string.Equals(entry[(dashIndex + 1)..], dbId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
