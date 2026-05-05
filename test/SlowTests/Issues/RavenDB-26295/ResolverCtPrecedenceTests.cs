using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class ResolverCtPrecedenceTests : NonDocumentDbCvProtectionTestBase
{
    private const LineageNode HubEntry = LineageNode.A;
    private const LineageNode SiblingSource = LineageNode.B;
    private const LineageNode HubInternal = LineageNode.C;
    private const LineageNode ConflictSource = LineageNode.D;

    private sealed record ResolverScenarioResult(
        string ExpectedWinnerName,
        DocumentSnapshot HubSnapshot,
        DocumentSnapshot ReceiverSnapshot);

    private sealed record ClusterTransactionScenarioResult(
        string ExpectedUpdatedName,
        DocumentSnapshot HubSnapshot,
        DocumentSnapshot ReceiverSnapshot);

    public ResolverCtPrecedenceTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ResolverIntersection_LocalResolution_ShouldKeepWinningDocumentOnHubEntry(Options options, bool protectedLineage)
    {
        var result = await RunResolverScenarioAsync(options, protectedLineage, propagateToInternal: false, preloadReceiverBeforeResolution: false);

        Assert.True(
            result.HubSnapshot.Exists,
            $"Expected resolved winner to remain on hub-entry {HubEntry}.");
        Assert.Equal(result.ExpectedWinnerName, result.HubSnapshot.Name);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ResolverIntersection_PropagationToInternalNode_ShouldReplicateWinningDocument(Options options, bool protectedLineage)
    {
        var result = await RunResolverScenarioAsync(options, protectedLineage, propagateToInternal: true, preloadReceiverBeforeResolution: true);

        Assert.True(
            result.ReceiverSnapshot.Exists,
            $"Expected resolved winner to reach hub-internal {HubInternal}.");
        Assert.Equal(result.ExpectedWinnerName, result.ReceiverSnapshot.Name);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ProtectedLineageResolverIntersection_ShouldStillConvergeWhenReceiverStartsEmpty(Options options)
    {
        var result = await RunResolverScenarioAsync(options, protectedLineage: true, propagateToInternal: true, preloadReceiverBeforeResolution: false);

        Assert.True(
            result.ReceiverSnapshot.Exists,
            $"Expected resolved winner to reach hub-internal {HubInternal}.");
        Assert.Equal(result.ExpectedWinnerName, result.ReceiverSnapshot.Name);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.ClusterTransactions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ClusterTransactionIntersection_ShouldReplicateUpdatedDocument(Options options, bool protectedLineage)
    {
        var result = await RunClusterTransactionScenarioAsync(options, protectedLineage);

        Assert.True(
            result.HubSnapshot.Exists,
            $"Expected cluster transaction update to remain on hub-entry {HubEntry}.");
        Assert.Equal(result.ExpectedUpdatedName, result.HubSnapshot.Name);

        Assert.True(
            result.ReceiverSnapshot.Exists,
            $"Expected cluster transaction update to reach hub-internal {HubInternal}.");
        Assert.Equal(result.ExpectedUpdatedName, result.ReceiverSnapshot.Name);
    }

    private async Task<ResolverScenarioResult> RunResolverScenarioAsync(Options options, bool protectedLineage, bool propagateToInternal, bool preloadReceiverBeforeResolution)
    {
        var docId = protectedLineage
            ? "tickets/resolver-ct-protected-resolver"
            : "tickets/resolver-ct-resolver-baseline";
        var expectedWinnerName = protectedLineage ? "protected-winner" : "baseline-winner";

        await using var lab = await CreateLabAsync(options);

        InternalLinkBlocker siblingToHubEntry = null;
        using var siblingToHubInternal = lab.BlockLink(SiblingSource, HubInternal);
        using var siblingToConflictSource = lab.BlockLink(SiblingSource, ConflictSource);
        using var conflictToHubEntry = lab.BlockLink(ConflictSource, HubEntry);
        using var conflictToSibling = lab.BlockLink(ConflictSource, SiblingSource);
        using var conflictToHubInternal = lab.BlockLink(ConflictSource, HubInternal);
        using var hubEntryToSibling = lab.BlockLink(HubEntry, SiblingSource);
        using var hubEntryToConflictSource = lab.BlockLink(HubEntry, ConflictSource);
        using var hubEntryToHubInternal = preloadReceiverBeforeResolution || propagateToInternal == false
            ? null
            : lab.BlockLink(HubEntry, HubInternal);

        if (protectedLineage)
            siblingToHubEntry = lab.BlockLink(SiblingSource, HubEntry);

        try
        {
            await StoreUserAsync(lab, ConflictSource, docId, "conflict-local");
            await StoreUserAsync(lab, SiblingSource, docId, expectedWinnerName);

            if (protectedLineage)
                await lab.InjectExistingTicketAsync(docId, SiblingSource, HubEntry);

            WaitForDocumentWithName(
                lab,
                HubEntry,
                docId,
                expectedWinnerName,
                $"source '{docId}' on hub-entry {HubEntry}");

            if (propagateToInternal && preloadReceiverBeforeResolution)
            {
                WaitForDocumentWithName(
                    lab,
                    HubInternal,
                    docId,
                    expectedWinnerName,
                    $"preloaded source '{docId}' on hub-internal {HubInternal}");
            }

            await SetReplicationConflictResolutionAsync((DocumentStore)lab.StoreFor(HubEntry), StraightforwardConflictResolution.ResolveToLatest);

            conflictToHubEntry.Release();

            Assert.Equal(
                0,
                WaitForValue(
                    () => ((DocumentStore)lab.StoreFor(HubEntry)).Commands().GetConflictsForAsync(docId).GetAwaiter().GetResult().Length,
                    expectedVal: 0,
                    timeout: 60_000));

            var hubSnapshot = WaitForDocumentWithName(
                lab,
                HubEntry,
                docId,
                expectedWinnerName,
                $"resolved winner '{docId}' on hub-entry {HubEntry}");

            var receiverSnapshot = new DocumentSnapshot(false, null, default, null);

            if (propagateToInternal)
            {
                hubEntryToHubInternal?.Release();

                receiverSnapshot = WaitForDocumentWithName(
                    lab,
                    HubInternal,
                    docId,
                    expectedWinnerName,
                    $"resolved winner '{docId}' on hub-internal {HubInternal}");

                Assert.Equal(
                    0,
                    WaitForValue(
                        () => ((DocumentStore)lab.StoreFor(HubInternal)).Commands().GetConflictsForAsync(docId).GetAwaiter().GetResult().Length,
                        expectedVal: 0,
                        timeout: 60_000));
            }

            return new ResolverScenarioResult(expectedWinnerName, hubSnapshot, receiverSnapshot);
        }
        finally
        {
            siblingToHubEntry?.Dispose();
        }
    }

    private async Task<ClusterTransactionScenarioResult> RunClusterTransactionScenarioAsync(Options options, bool protectedLineage)
    {
        var docId = protectedLineage
            ? "tickets/resolver-ct-protected-cluster-tx"
            : "tickets/resolver-ct-cluster-tx";
        var initialName = protectedLineage ? "protected-cluster-source" : "cluster-source";
        var updatedName = protectedLineage ? "protected-cluster-tx" : "cluster-tx";

        await using var lab = await CreateLabAsync(options);

        InternalLinkBlocker siblingToHubEntry = null;
        using var siblingToHubInternal = lab.BlockLink(SiblingSource, HubInternal);
        using var siblingToConflictSource = lab.BlockLink(SiblingSource, ConflictSource);

        if (protectedLineage)
            siblingToHubEntry = lab.BlockLink(SiblingSource, HubEntry);

        try
        {
            await StoreUserAsync(lab, SiblingSource, docId, initialName);

            if (protectedLineage)
                await lab.InjectExistingTicketAsync(docId, SiblingSource, HubEntry);

            WaitForDocumentWithName(
                lab,
                HubEntry,
                docId,
                initialName,
                $"cluster transaction source '{docId}' on hub-entry {HubEntry}");

            using (var session = lab.StoreFor(HubEntry).OpenAsyncSession(new SessionOptions
                   {
                       TransactionMode = TransactionMode.ClusterWide
                   }))
            {
                var user = await session.LoadAsync<User>(docId);
                Assert.NotNull(user);
                user.Name = updatedName;
                await session.SaveChangesAsync();
            }

            var hubSnapshot = WaitForDocumentWithName(
                lab,
                HubEntry,
                docId,
                updatedName,
                $"cluster transaction update '{docId}' on hub-entry {HubEntry}");

            var receiverSnapshot = WaitForDocumentWithName(
                lab,
                HubInternal,
                docId,
                updatedName,
                $"cluster transaction update '{docId}' on hub-internal {HubInternal}");

            return new ClusterTransactionScenarioResult(updatedName, hubSnapshot, receiverSnapshot);
        }
        finally
        {
            siblingToHubEntry?.Dispose();
        }
    }

    private DocumentSnapshot WaitForDocumentWithName(NonDocumentLab lab, LineageNode node, string docId, string expectedName, string subject)
    {
        DocumentSnapshot snapshot = default;
        var found = WaitForValue(
            () =>
            {
                snapshot = lab.GetDocumentSnapshot(node, docId);
                return snapshot.Exists && string.Equals(snapshot.Name, expectedName, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: 60_000);

        Assert.True(
            found,
            $"Expected {subject} to exist with name '{expectedName}', but the final snapshot was '{snapshot}'.");
        return snapshot;
    }
}
