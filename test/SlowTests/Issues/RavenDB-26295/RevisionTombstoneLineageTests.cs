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

namespace SlowTests.Issues;

public class RevisionTombstoneLineageTests : TombstoneLineagePreservationTestBase
{
    private const string SubjectDocId = "tickets/revision-tombstone";
    private const int FilteredPullHandshakeTimeoutMs = 25_000;

    public RevisionTombstoneLineageTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    public async Task InternalReplication_ShouldReplicateRevisionTombstones(Options options, bool seedProtectedSiblingLineage)
    {
        await using var lab = await CreateLabAsync(options);
        using var writerToSource = lab.BlockLink(source: LineageNode.C, target: LineageNode.A);
        using var writerToReceiver = lab.BlockLink(source: LineageNode.C, target: LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);
        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var sourceSnapshots = await CreateSourceRevisionTombstonesAsync(lab, seedProtectedSiblingLineage);

        await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, blocker: sourceToReceiver);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

        var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
        AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    public async Task FilteredPullReplication_ShouldReplicateRevisionTombstones(Options options, bool seedProtectedSiblingLineage)
    {
        await using var lab = await CreateLabAsync(options);
        using var writerToSource = lab.BlockLink(source: LineageNode.C, target: LineageNode.A);
        using var writerToReceiver = lab.BlockLink(source: LineageNode.C, target: LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);
        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var sourceSnapshots = await CreateSourceRevisionTombstonesAsync(lab, seedProtectedSiblingLineage);

        await lab.InjectExistingTicketAsync(SubjectDocId, LineageNode.A, LineageNode.B);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

        var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
        AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task RevisionTombstone_FilteredPullHandshake_ShouldStillDeliverTombstone(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        using var source = lab.CreateIsolatedStore("filtered-pull-revision-tombstone");
        await source.Maintenance.SendAsync(new ConfigureRevisionsOperation(CreateRevisionsConfiguration()));
        await CreateRevisionTombstonesOnStoreAsync(source, SubjectDocId);
        await lab.PreconditionFullDatabaseChangeVectorFromStoreAsync(source, LineageNode.A);

        await lab.ConnectSinkToHubAsync(source, LineageNode.A);

        Assert.True(
            lab.WaitForRevisionTombstones(LineageNode.A, SubjectDocId, expectedCount: 1, timeout: FilteredPullHandshakeTimeoutMs),
            $"Expected filtered sink-to-hub replication to deliver revision tombstones for '{SubjectDocId}' after the hub had been preconditioned with the same source identity. " +
            "If this fails, legitimate revision tombstone items were skipped before they reached the hub.");
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    public async Task DirectFilteredRevisionTombstone_ShouldStillReachRequestedReceivers(Options options, bool deliverToInternalReceiver, bool deliverToFilteredReceiver)
    {
        await using var lab = await CreateLabAsync(options);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.A, PullReplicationMode.SinkToHub);
        await externalSink.Maintenance.SendAsync(new ConfigureRevisionsOperation(CreateRevisionsConfiguration()));

        await CreateDirectRevisionTombstonesOnExternalStoreAsync(externalSink);

        Assert.True(lab.WaitForRevisionTombstones(LineageNode.A, SubjectDocId, expectedCount: 1, timeout: 60_000),
            userMessage: $"Expected revision tombstones for '{SubjectDocId}' to reach hub A from the external sink.");

        var sourceSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.A, SubjectDocId);
        Assert.NotEmpty(sourceSnapshots);

        if (deliverToInternalReceiver)
        {
            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected direct filtered-pull revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }

        if (deliverToFilteredReceiver)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, LineageNode.A, LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected direct filtered-pull revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    public async Task LocalRevisionTombstone_OnUnknownSinkLineageWithoutSiblingEntries_ShouldStillReachRequestedReceivers(Options options, bool deliverToInternalReceiver, bool deliverToFilteredReceiver)
    {
        await using var lab = await CreateLabAsync(options);
        using var sourceToReceiver = lab.BlockLink(source: LineageNode.A, target: LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.A, PullReplicationMode.SinkToHub);
        await SeedUnknownSinkDocumentOnHubAsync(lab, externalSink);

        var sourceDoc = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
        Assert.True(sourceDoc.Exists, userMessage: $"Expected unknown-sink document '{SubjectDocId}' on A.");

        var sourceSnapshots = await CreateLocalHubRevisionTombstonesAsync(lab);

        if (deliverToInternalReceiver)
        {
            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected local unknown-sink revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }

        if (deliverToFilteredReceiver)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, LineageNode.A, LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected local unknown-sink revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    public async Task LocalRevisionTombstone_OnUnknownSinkLineageWithSiblingEntries_ShouldStillReachRequestedReceivers(Options options, bool deliverToInternalReceiver, bool deliverToFilteredReceiver)
    {
        await using var lab = await CreateLabAsync(options);
        using var writerToSource = lab.BlockLink(LineageNode.C, LineageNode.A);
        using var writerToReceiver = lab.BlockLink(LineageNode.C, LineageNode.B);
        using var sourceToReceiver = lab.BlockLink(LineageNode.A, LineageNode.B);

        await lab.ConfigureRevisionsAsync(CreateRevisionsConfiguration());

        var externalSink = await lab.CreateExternalSinkStoreAsync(LineageNode.C, PullReplicationMode.HubToSink);
        await lab.ConfigureExternalSinkConnectionAsync(externalSink, LineageNode.A, PullReplicationMode.SinkToHub);

        var sourceDoc = await SeedProtectedUnknownSinkDocumentOnHubAsync(lab, externalSink);
        Assert.True(sourceDoc.Exists, $"Expected protected unknown-sink document '{SubjectDocId}' on A.");

        var sourceSnapshots = await CreateLocalHubRevisionTombstonesAsync(lab);

        if (deliverToInternalReceiver)
        {
            await lab.WriteSyncMarkerAndReleaseAsync(sender: LineageNode.A, waitTarget: LineageNode.B, sourceToReceiver);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected maximal-lineage revision tombstones for '{SubjectDocId}' to reach B via internal replication.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }

        if (deliverToFilteredReceiver)
        {
            await lab.InjectExistingTicketAsync(SubjectDocId, LineageNode.A, LineageNode.B);

            Assert.True(lab.WaitForRevisionTombstones(LineageNode.B, SubjectDocId, sourceSnapshots.Count, timeout: 60_000),
                userMessage: $"Expected maximal-lineage revision tombstones for '{SubjectDocId}' to reach B via filtered pull.");

            var receiverSnapshots = lab.GetRevisionTombstoneSnapshots(LineageNode.B, SubjectDocId);
            AssertEquivalentRevisionTombstoneSets(sourceSnapshots, receiverSnapshots);
        }
    }

    private static RevisionsConfiguration CreateRevisionsConfiguration() =>
        new() { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };

    private static async Task<List<RevisionTombstoneSnapshot>> CreateSourceRevisionTombstonesAsync(LineageLab lab, bool seedProtectedSiblingLineage)
    {
        if (seedProtectedSiblingLineage)
        {
            await lab.WriteAndInjectTicketAsync(SubjectDocId, LineageNode.C, LineageNode.A);

            Assert.True(lab.WaitForDoc(LineageNode.A, SubjectDocId, timeout: 60_000),
                userMessage: $"Expected seeded document '{SubjectDocId}' to arrive on A via filtered pull.");

            var seeded = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
            Assert.True(seeded.Exists, userMessage: $"Expected seeded document '{SubjectDocId}' on A.");
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

    private static async Task CreateRevisionTombstonesOnStoreAsync(IDocumentStore store, string docId)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = $"revision-source-{Guid.NewGuid():N}" }, docId);
            await session.SaveChangesAsync();
        }

        for (var i = 0; i < 2; i++)
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<User>(docId);
            Assert.NotNull(doc);
            doc.Name = $"revision-source-{i}-{Guid.NewGuid():N}";
            await session.SaveChangesAsync();
        }

        await store.Maintenance.SendAsync(new DeleteRevisionsOperation(documentIds: [docId]));
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

    private async Task<DocumentSnapshot> SeedProtectedUnknownSinkDocumentOnHubAsync(LineageLab lab, IDocumentStore externalSink)
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
            userMessage: $"Expected protected unknown-sink document '{SubjectDocId}' to arrive on A after the external sink modification.");

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
    }
}
