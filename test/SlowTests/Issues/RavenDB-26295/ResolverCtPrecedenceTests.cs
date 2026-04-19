using System;
using System.Globalization;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

// This is a characterization suite, not a proof that any other future behavior would be wrong.
// If one of these assertions starts failing, the most likely interpretation is that precedence
// semantics changed. That change may be intentional, but it must be reviewed to ensure that
// FromFilteredPullReplicationHub is not now blocking resolver / cluster-transaction DB CV updates.
public class ResolverCtPrecedenceTests : NonDocumentDbCvProtectionTestBase
{
    private const LineageNode HubEntry = LineageNode.A;
    private const LineageNode SiblingSource = LineageNode.B;
    private const LineageNode HubInternal = LineageNode.C;
    private const LineageNode ConflictSource = LineageNode.D;
    private const string ResolverChangeHint =
        "Current characterization says that FromFilteredPullReplicationHub does not preempt resolver DB CV advancement in this flow. " +
        "If this assertion now fails, treat it as a precedence semantic change: inspect the new runtime path and confirm that filtered lineage is not unintentionally blocking resolver merge logic.";
    private const string ClusterTransactionChangeHint =
        "Current characterization says that FromFilteredPullReplicationHub does not preempt cluster transaction RAFT advancement in this flow. " +
        "If this assertion now fails, treat it as a precedence semantic change: inspect the new runtime path and confirm that filtered lineage is not unintentionally blocking cluster transaction merge logic.";

    private sealed record ResolverScenarioResult(
        string HubDbCvBefore,
        string HubDbCvAfter,
        string ReceiverDbCvBefore,
        string ReceiverDbCvAfter,
        DocumentSnapshot HubSnapshot,
        DocumentSnapshot ReceiverSnapshot);

    private sealed record ClusterTransactionScenarioResult(
        string HubDbCvBefore,
        string HubDbCvAfter,
        DocumentSnapshot UpdatedSnapshot);

    public ResolverCtPrecedenceTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task ResolverIntersection_LocalResolution_ShouldAdvanceConflictSourceEntryIntoHubDbCv(bool flagged)
    {
        var result = await RunResolverScenarioAsync(flagged, propagateToInternal: false, preloadReceiverBeforeResolution: false);

        Assert.True(result.HubSnapshot.Exists,
            userMessage: $"Expected resolved winner on hub-entry {HubEntry}. {ResolverChangeHint}");
        Assert.True(ContainsEntry(result.HubSnapshot.ChangeVector, ConflictSource.ToString()),
            userMessage: $"Expected resolved document CV on hub-entry {HubEntry} to contain conflict source '{ConflictSource}'. " +
            $"This asserts the current behavior that resolver keeps the non-sibling conflict-source component even when the document is flagged={flagged}. " +
            $"Actual CV: '{result.HubSnapshot.ChangeVector ?? "<null>"}'. {ResolverChangeHint}");

        if (flagged)
            AssertFlagged(result.HubSnapshot.Flags, $"resolved winner on hub-entry {HubEntry}");
        else
            AssertNotFlagged(result.HubSnapshot.Flags, $"resolved winner on hub-entry {HubEntry}");

        AssertEntryEtagAdvanced(
            result.HubDbCvBefore,
            result.HubDbCvAfter,
            ConflictSource.ToString(),
            $"hub-entry {HubEntry} DB CV after {(flagged ? "flagged" : "baseline")} resolver intersection",
            $"hub-entry {HubEntry} DB CV before {(flagged ? "flagged" : "baseline")} resolver intersection");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task ResolverIntersection_PropagationToInternalNode_ShouldAdvanceConflictSourceEntryIntoReceiverDbCv(bool flagged)
    {
        var result = await RunResolverScenarioAsync(flagged, propagateToInternal: true, preloadReceiverBeforeResolution: true);

        Assert.True(result.ReceiverSnapshot.Exists,
            userMessage: $"Expected resolved winner to reach hub-internal {HubInternal}. {ResolverChangeHint}");
        Assert.True(ContainsEntry(result.ReceiverSnapshot.ChangeVector, ConflictSource.ToString()),
            userMessage: $"Expected resolved winner on hub-internal {HubInternal} to retain conflict source '{ConflictSource}' in its CV. " +
            $"This asserts the current behavior that a receiver which already held the pre-resolve source document still observes the resolved winner with the conflict-source entry intact. " +
            $"Actual CV: '{result.ReceiverSnapshot.ChangeVector ?? "<null>"}'. {ResolverChangeHint}");

        if (flagged)
            AssertFlagged(result.ReceiverSnapshot.Flags, $"resolved winner on hub-internal {HubInternal}");
        else
            AssertNotFlagged(result.ReceiverSnapshot.Flags, $"resolved winner on hub-internal {HubInternal}");

        AssertEntryEtagAdvanced(
            result.ReceiverDbCvBefore,
            result.ReceiverDbCvAfter,
            ConflictSource.ToString(),
            $"hub-internal {HubInternal} DB CV after receiving {(flagged ? "flagged" : "baseline")} resolved winner",
            $"hub-internal {HubInternal} DB CV before receiving {(flagged ? "flagged" : "baseline")} resolved winner");
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FlaggedResolverIntersection_ShouldStillConvergeDespiteDeferredReceiverDbCvEntry()
    {
        var result = await RunResolverScenarioAsync(flagged: true, propagateToInternal: true, preloadReceiverBeforeResolution: false);

        Assert.True(result.ReceiverSnapshot.Exists,
            userMessage: $"Expected resolved winner to reach hub-internal {HubInternal}. {ResolverChangeHint}");
        AssertFlagged(result.ReceiverSnapshot.Flags, $"resolved winner on hub-internal {HubInternal}");
        Assert.True(ContainsEntry(result.ReceiverSnapshot.ChangeVector, ConflictSource.ToString()),
            userMessage: $"Expected resolved winner on hub-internal {HubInternal} to retain conflict source '{ConflictSource}' in its CV. " +
            $"This asserts the current backlog-convergence behavior for a flagged resolved winner. " +
            $"Actual CV: '{result.ReceiverSnapshot.ChangeVector ?? "<null>"}'. {ResolverChangeHint}");

        AssertEntryEtagAdvanced(
            result.ReceiverDbCvBefore,
            result.ReceiverDbCvAfter,
            ConflictSource.ToString(),
            $"hub-internal {HubInternal} DB CV after backlog convergence of resolved flagged winner",
            $"hub-internal {HubInternal} DB CV before backlog convergence of resolved flagged winner");
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.ClusterTransactions)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task ClusterTransactionIntersection_ShouldAddRaftEntryToHubDbCv(bool flagged)
    {
        var result = await RunClusterTransactionScenarioAsync(flagged);

        Assert.True(result.UpdatedSnapshot.Exists,
            userMessage: $"Expected cluster transaction update on hub-entry {HubEntry}. {ClusterTransactionChangeHint}");
        Assert.True(result.UpdatedSnapshot.Flags.Contain(DocumentFlags.FromClusterTransaction),
            userMessage: $"Expected cluster transaction update to keep {nameof(DocumentFlags.FromClusterTransaction)}, but flags were '{result.UpdatedSnapshot.Flags}'. " +
            $"{ClusterTransactionChangeHint}");

        if (flagged)
            AssertFlagged(result.UpdatedSnapshot.Flags, $"flagged cluster transaction document on hub-entry {HubEntry}");
        else
            AssertNotFlagged(result.UpdatedSnapshot.Flags, $"baseline cluster transaction document on hub-entry {HubEntry}");

        AssertEntryEtagAdvanced(
            result.HubDbCvBefore,
            result.HubDbCvAfter,
            "RAFT",
            $"hub-entry {HubEntry} DB CV after {(flagged ? "flagged" : "baseline")} cluster transaction",
            $"hub-entry {HubEntry} DB CV before {(flagged ? "flagged" : "baseline")} cluster transaction");
    }

    private async Task<ResolverScenarioResult> RunResolverScenarioAsync(bool flagged, bool propagateToInternal, bool preloadReceiverBeforeResolution)
    {
        var docId = flagged
            ? "tickets/resolver-ct-flagged-resolver"
            : "tickets/resolver-ct-resolver-baseline";
        var expectedWinnerName = flagged ? "flagged-winner" : "baseline-winner";

        await using var lab = await CreateLabAsync(new Options());

        InternalLinkBlocker siblingToHubEntry = null;
        using var siblingToHubInternal = lab.BlockLink(source: SiblingSource, target: HubInternal);
        using var siblingToConflictSource = lab.BlockLink(source: SiblingSource, target: ConflictSource);
        using var conflictToHubEntry = lab.BlockLink(source: ConflictSource, target: HubEntry);
        using var conflictToSibling = lab.BlockLink(source: ConflictSource, target: SiblingSource);
        using var conflictToHubInternal = lab.BlockLink(source: ConflictSource, target: HubInternal);
        using var hubEntryToSibling = lab.BlockLink(source: HubEntry, target: SiblingSource);
        using var hubEntryToConflictSource = lab.BlockLink(source: HubEntry, target: ConflictSource);
        using var hubEntryToHubInternal = preloadReceiverBeforeResolution || propagateToInternal == false
            ? null
            : lab.BlockLink(source: HubEntry, target: HubInternal);

        if (flagged)
            siblingToHubEntry = lab.BlockLink(source: SiblingSource, target: HubEntry);

        try
        {
            await StoreUserAsync(lab, ConflictSource, docId, "conflict-local");
            await StoreUserAsync(lab, SiblingSource, docId, expectedWinnerName);

            if (flagged)
            {
                await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingSource, targetNode: HubEntry);
                WaitForFlaggedDocument(lab, HubEntry, docId, $"flagged source '{docId}'");
            }
            else
            {
                Assert.True(lab.WaitForDocumentName(HubEntry, docId, expectedWinnerName, timeout: 60_000),
                    userMessage: $"Expected baseline source '{docId}' to arrive on hub-entry {HubEntry}. {ResolverChangeHint}");

                var baselineSnapshot = lab.GetDocumentSnapshot(HubEntry, docId);
                Assert.True(baselineSnapshot.Exists, userMessage: $"Expected baseline source '{docId}' on hub-entry {HubEntry}.");
                AssertNotFlagged(baselineSnapshot.Flags, $"baseline source '{docId}' on hub-entry {HubEntry}");
            }

            var hubDbCvBefore = lab.GetDatabaseChangeVector(HubEntry);
            var receiverDbCvBefore = lab.GetDatabaseChangeVector(HubInternal);
            if (propagateToInternal && preloadReceiverBeforeResolution)
            {
                var receiverSourceSnapshot = WaitForSourceDocument(
                    lab,
                    HubInternal,
                    docId,
                    expectedWinnerName,
                    flagged,
                    $"preloaded source '{docId}' on hub-internal {HubInternal}");

                Assert.True(receiverSourceSnapshot.Flags.Contain(DocumentFlags.Resolved) == false,
                    userMessage: $"Expected preloaded source '{docId}' on hub-internal {HubInternal} to arrive before resolution, but flags were '{receiverSourceSnapshot.Flags}'. " +
                    $"This test relies on the receiver already holding the pre-resolve source document before the resolved update is sent. {ResolverChangeHint}");
                receiverDbCvBefore = lab.GetDatabaseChangeVector(HubInternal);
            }

            await SetReplicationConflictResolutionAsync((DocumentStore)lab.StoreFor(HubEntry), StraightforwardConflictResolution.ResolveToLatest);

            conflictToHubEntry.Release();

            Assert.Equal(
                0,
                WaitForValue(
                    () => ((DocumentStore)lab.StoreFor(HubEntry)).Commands().GetConflictsForAsync(docId).GetAwaiter().GetResult().Length,
                    expectedVal: 0,
                    timeout: 60_000));

            var hubSnapshot = WaitForResolvedDocument(
                lab,
                HubEntry,
                docId,
                expectedWinnerName,
                flagged,
                $"resolved winner '{docId}' on hub-entry {HubEntry}");

            Assert.True(ContainsEntry(hubSnapshot.ChangeVector, SiblingSource.ToString()),
                userMessage: $"Expected resolved winner '{docId}' on hub-entry {HubEntry} to retain sibling source '{SiblingSource}' in its CV, but CV was '{hubSnapshot.ChangeVector ?? "<null>"}'.");

            var receiverSnapshot = new DocumentSnapshot(false, null, default, null);

            if (propagateToInternal)
            {
                hubEntryToHubInternal?.Release();

                receiverSnapshot = WaitForResolvedDocument(
                    lab,
                    HubInternal,
                    docId,
                    expectedWinnerName,
                    flagged,
                    $"resolved winner '{docId}' on hub-internal {HubInternal}");
            }

            return new ResolverScenarioResult(
                hubDbCvBefore,
                lab.GetDatabaseChangeVector(HubEntry),
                receiverDbCvBefore,
                lab.GetDatabaseChangeVector(HubInternal),
                hubSnapshot,
                receiverSnapshot);
        }
        finally
        {
            siblingToHubEntry?.Dispose();
        }
    }

    private async Task<ClusterTransactionScenarioResult> RunClusterTransactionScenarioAsync(bool flagged)
    {
        var docId = flagged
            ? "tickets/resolver-ct-flagged-cluster-tx"
            : "tickets/resolver-ct-cluster-tx";
        var initialName = flagged ? "flagged-cluster-source" : "cluster-source";
        var updatedName = flagged ? "flagged-cluster-tx" : "cluster-tx";

        await using var lab = await CreateLabAsync(new Options());

        InternalLinkBlocker siblingToHubEntry = null;
        using var siblingToHubInternal = lab.BlockLink(source: SiblingSource, target: HubInternal);
        using var siblingToConflictSource = lab.BlockLink(source: SiblingSource, target: ConflictSource);

        if (flagged)
            siblingToHubEntry = lab.BlockLink(source: SiblingSource, target: HubEntry);

        try
        {
            await StoreUserAsync(lab, SiblingSource, docId, initialName);

            if (flagged)
            {
                await lab.InjectExistingTicketAsync(docId, sourceNode: SiblingSource, targetNode: HubEntry);
                WaitForFlaggedDocument(lab, HubEntry, docId, $"flagged cluster source '{docId}'");
            }
            else
            {
                Assert.True(lab.WaitForDocumentName(HubEntry, docId, initialName, timeout: 60_000),
                    userMessage: $"Expected baseline cluster source '{docId}' to arrive on hub-entry {HubEntry}. {ClusterTransactionChangeHint}");

                var baselineSnapshot = lab.GetDocumentSnapshot(HubEntry, docId);
                Assert.True(baselineSnapshot.Exists, userMessage: $"Expected baseline cluster source '{docId}' on hub-entry {HubEntry}.");
                AssertNotFlagged(baselineSnapshot.Flags, $"baseline cluster source '{docId}' on hub-entry {HubEntry}");
            }

            var hubDbCvBefore = lab.GetDatabaseChangeVector(HubEntry);

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

            Assert.True(lab.WaitForDocumentName(HubEntry, docId, updatedName, timeout: 60_000),
                userMessage: $"Expected cluster transaction update for '{docId}' to be visible on hub-entry {HubEntry}.");

            var updatedSnapshot = lab.GetDocumentSnapshot(HubEntry, docId);
            Assert.True(updatedSnapshot.Exists, userMessage: $"Expected updated cluster transaction document '{docId}' on hub-entry {HubEntry}.");

            return new ClusterTransactionScenarioResult(
                hubDbCvBefore,
                lab.GetDatabaseChangeVector(HubEntry),
                updatedSnapshot);
        }
        finally
        {
            siblingToHubEntry?.Dispose();
        }
    }

    private static void WaitForFlaggedDocument(NonDocumentLab lab, LineageNode node, string docId, string subject)
    {
        Assert.True(lab.WaitForDoc(node, docId, timeout: 60_000), userMessage: $"Expected '{docId}' on {subject}.");
        var snapshot = lab.GetDocumentSnapshot(node, docId);
        Assert.True(snapshot.Exists, userMessage: $"Expected '{docId}' to exist for {subject}.");
        AssertFlagged(snapshot.Flags, subject);
    }

    private DocumentSnapshot WaitForSourceDocument(NonDocumentLab lab, LineageNode node, string docId, string expectedName, bool flagged, string subject)
    {
        DocumentSnapshot snapshot = null;
        var found = WaitForValue(
            () =>
            {
                snapshot = lab.GetDocumentSnapshot(node, docId);
                if (snapshot.Exists == false)
                    return false;

                if (string.Equals(snapshot.Name, expectedName, StringComparison.Ordinal) == false)
                    return false;

                if (flagged)
                    return snapshot.Flags.Contain(DocumentFlags.FromFilteredPullReplicationHub);

                return snapshot.Flags.Contain(DocumentFlags.FromFilteredPullReplicationHub) == false;
            },
            expectedVal: true,
            timeout: 60_000);

        Assert.True(found,
            userMessage: $"Expected {subject} to arrive with name '{expectedName}', but the final snapshot was '{snapshot}'. " +
            $"This source-stage wait is part of the precedence characterization harness. {ResolverChangeHint}");
        return snapshot;
    }

    private DocumentSnapshot WaitForResolvedDocument(NonDocumentLab lab, LineageNode node, string docId, string expectedName, bool flagged, string subject)
    {
        DocumentSnapshot snapshot = null;
        var found = WaitForValue(
            () =>
            {
                snapshot = lab.GetDocumentSnapshot(node, docId);
                if (snapshot.Exists == false)
                    return false;

                if (string.Equals(snapshot.Name, expectedName, StringComparison.Ordinal) == false)
                    return false;

                if (snapshot.Flags.Contain(DocumentFlags.Resolved) == false)
                    return false;

                if (flagged)
                    return snapshot.Flags.Contain(DocumentFlags.FromFilteredPullReplicationHub);

                return snapshot.Flags.Contain(DocumentFlags.FromFilteredPullReplicationHub) == false;
            },
            expectedVal: true,
            timeout: 60_000);

        Assert.True(found,
            userMessage: $"Expected {subject} to be resolved with name '{expectedName}', but the final snapshot was '{snapshot}'. " +
            $"If this now fails, the runtime ordering or precedence semantics likely changed and the characterization must be revisited. {ResolverChangeHint}");
        return snapshot;
    }

    private static void AssertFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(flags.Contain(DocumentFlags.FromFilteredPullReplicationHub),
            userMessage: $"Expected {subject} to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'. " +
            "The current characterization says this flow preserves the filtered-pull marker while still allowing resolver / cluster-transaction semantics to proceed.");
    }

    private static void AssertNotFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(flags.Contain(DocumentFlags.FromFilteredPullReplicationHub) == false,
            userMessage: $"Expected {subject} NOT to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'. " +
            "The current characterization says this baseline flow is unflagged; if this changes, revisit whether the harness is now taking a different replication path.");
    }

    private static void AssertEntryEtagAdvanced(string beforeChangeVector, string afterChangeVector, string tag, string afterSubject, string beforeSubject)
    {
        var beforeEtag = GetEntryEtag(beforeChangeVector, tag);
        var afterEtag = GetEntryEtag(afterChangeVector, tag);

        Assert.True(afterEtag.HasValue && (beforeEtag.HasValue == false || afterEtag.Value > beforeEtag.Value),
            userMessage: $"Expected {afterSubject} to advance the '{tag}' etag from {beforeSubject}, but expected a value greater than {beforeEtag?.ToString() ?? "<missing>"} and got {afterEtag?.ToString() ?? "<missing>"}. " +
            $"Before CV: '{beforeChangeVector ?? "<null>"}'. After CV: '{afterChangeVector ?? "<null>"}'. " +
            $"{GetPrecedenceChangeHint(tag)}");
    }

    private static bool ContainsEntry(string changeVector, string tag)
    {
        return GetEntryEtag(changeVector, tag).HasValue;
    }

    private static long? GetEntryEtag(string changeVector, string tag)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return null;

        foreach (var entry in changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colonIndex = entry.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            if (string.Equals(entry[..colonIndex], tag, StringComparison.OrdinalIgnoreCase) == false)
                continue;

            var dashIndex = entry.IndexOf('-', colonIndex + 1);
            var etagText = dashIndex > colonIndex
                ? entry.Substring(colonIndex + 1, dashIndex - colonIndex - 1)
                : entry[(colonIndex + 1)..];

            return long.TryParse(etagText, NumberStyles.None, CultureInfo.InvariantCulture, out var etag) ? etag : null;
        }

        return null;
    }

    private static string GetPrecedenceChangeHint(string tag)
    {
        return string.Equals(tag, "RAFT", StringComparison.OrdinalIgnoreCase)
            ? ClusterTransactionChangeHint
            : ResolverChangeHint;
    }
}
