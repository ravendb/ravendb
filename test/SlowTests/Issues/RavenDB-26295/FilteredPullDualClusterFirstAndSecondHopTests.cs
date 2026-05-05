using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Replication;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public class FilteredPullDualClusterFirstAndSecondHopTests : FilteredPullDualClusterTestBase
{
    public FilteredPullDualClusterFirstAndSecondHopTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task Document_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "document";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);
        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);

        var originalIncomingChangeVector = lab.GetFilteredRoundTripDocument(LabNode.B).ChangeVector;
        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        Assert.True(nodeADocument.Exists, $"Expected passed document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");

        // Verify the filtered pass into node A does not advance node B in the database change vector.
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered document pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeADocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document pass", nodeADbCvAfterPass, nodeADocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify the filtered item can still move from node A to node C through ordinary internal replication.
        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeBDocument = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);

        Assert.True(
            nodeCDocument.Exists,
            $"Expected filtered pass document '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.B)}: exists={nodeBDocument.Exists}, name='{nodeBDocument.Name ?? "<null>"}', CV='{nodeBDocument.ChangeVector ?? "<null>"}'. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.A)}: exists={nodeADocument.Exists}, name='{nodeADocument.Name ?? "<null>"}', CV='{nodeADocument.ChangeVector ?? "<null>"}'. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.C)}: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"originalIncomingCV='{originalIncomingChangeVector}'.");
        Assert.True(
            string.Equals(nodeCDocument.Name, itemName, StringComparison.Ordinal),
            $"Expected filtered pass document on {NodeTag(filteredPassReceiveSide, LabNode.C)} to keep name '{itemName}'. " +
            $"actualName='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in document Version only after the internal hop.
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered document internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCDocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document after internal hop", nodeCDocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document after internal hop", nodeCDocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document after internal hop", nodeCDbCvAfterPass, nodeCDocument.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local change on node A preserves filtered document Version lineage without inflating database change vectors.
        const string modifiedOnNodeAName = itemName + "-modified-on-node-a";

        var nodeADocumentBeforeLocalChange = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document before local change", nodeADocumentBeforeLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document before local change", nodeADocumentBeforeLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document before local change", nodeADbCvBeforeLocalChange, nodeADocumentBeforeLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.A, modifiedOnNodeAName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, modifiedOnNodeAName);

        var nodeADocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local change", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local change", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local change", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "document after local change", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripDocumentNameOrConflictAsync(LabNode.C, modifiedOnNodeAName);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "document update from filtered document", nodeCDocument.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCDocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "document update from filtered document", nodeCDocument.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCDocumentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "document update from filtered document", nodeCDocument.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local change replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local change replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local change replicated from node A", nodeCDbCvAfterLocalChange, nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task ConflictDocument_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "conflict-document";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        const string initialName = itemName + "-initial";
        var nodeBName = itemName + "-" + NodeTagLower(filteredPassReceiveSide, LabNode.B);
        var nodeCName = itemName + "-" + NodeTagLower(filteredPassReceiveSide, LabNode.C);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, initialName);

        var cToABlocker = await lab.BlockInternalReplicationAsync(from: LabNode.C, to: LabNode.A);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, nodeBName);
        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.C, nodeCName);
        await cToABlocker.WaitForBlockedAsync();

        var nodeBConflicts = await lab.WaitForFilteredRoundTripConflictsAsync(LabNode.B, expectedCount: 2);
        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBConflict = nodeBConflicts
            .OrderByDescending(x => GetEtag(x.ChangeVector, nodeBDatabaseId))
            .First();
        var originalIncomingChangeVector = nodeBConflict.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected conflict on node B for '{lab.FilteredRoundTripTicketId}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBConflicts='{FormatConflicts(nodeBConflicts)}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedConflicts(expectedCount: 2);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeAConflicts = lab.GetFilteredRoundTripConflicts(LabNode.A);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeAConflict = nodeAConflicts
            .OrderByDescending(x => GetVersionEtag(x.ChangeVector, nodeBDatabaseId))
            .FirstOrDefault();

        Assert.True(nodeAConflicts.Count >= 2, $"Expected passed conflict document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.NotNull(nodeAConflict);

        // Verify the filtered pass into node A keeps conflict lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered conflict pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict after pass", nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict after pass", nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict pass", nodeADbCvAfterPass, nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCConflicts = lab.GetFilteredRoundTripConflicts(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeCConflict = nodeCConflicts
            .OrderByDescending(x => GetVersionEtag(x.ChangeVector, nodeBDatabaseId))
            .FirstOrDefault();

        Assert.True(
            nodeCConflicts.Count >= 2,
            $"Expected filtered conflict pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} and " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.C)}->{NodeTag(filteredPassReceiveSide, LabNode.A)} are blocked. " +
            $"nodeBConflicts='{FormatConflicts(nodeBConflicts)}'. nodeAConflicts='{FormatConflicts(nodeAConflicts)}'. " +
            $"nodeCConflicts='{FormatConflicts(nodeCConflicts)}'. originalIncomingCV='{originalIncomingChangeVector}'.");
        Assert.NotNull(nodeCConflict);

        // Verify node C also keeps node B lineage in conflict Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered conflict internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered conflict after internal hop", nodeCConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered conflict after internal hop", nodeCConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered conflict internal hop", nodeCDbCvAfterPass, nodeCConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local conflict resolution on node A preserves filtered document Version lineage without inflating database change vectors.
        const string resolvedOnNodeAName = itemName + "-resolved-on-node-a";

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict before local resolution", nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict before local resolution", nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered conflict before local resolution", nodeADbCvBeforeLocalChange, nodeAConflict.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.A, resolvedOnNodeAName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, resolvedOnNodeAName);

        var nodeADocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local conflict resolution", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local conflict resolution", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local conflict resolution", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "document after local conflict resolution", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, resolvedOnNodeAName);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "document conflict resolution from filtered conflict", nodeCConflict.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCDocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "document conflict resolution from filtered conflict", nodeCConflict.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCDocumentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "document conflict resolution from filtered conflict", nodeCConflict.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local conflict resolution replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local conflict resolution replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local conflict resolution replicated from node A", nodeCDbCvAfterLocalChange, nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task DocumentTombstone_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "document-tombstone";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.DeleteFilteredRoundTripDocumentAsync(LabNode.B);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBDocumentAfterDelete = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeBTombstone = lab.GetFilteredRoundTripDocumentTombstone(LabNode.B);
        var originalIncomingChangeVector = nodeBTombstone.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.False(
            nodeBDocumentAfterDelete.Exists,
            $"Expected deleted document '{lab.FilteredRoundTripTicketId}' not to exist on {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBDocumentExists={nodeBDocumentAfterDelete.Exists}, nodeBDocumentCV='{nodeBDocumentAfterDelete.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBTombstone.Exists,
            $"Expected document tombstone on node B '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBDocumentExists={nodeBDocumentAfterDelete.Exists}, nodeBTombstoneExists={nodeBTombstone.Exists}, nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected document tombstone change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedDocumentTombstone();
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeATombstone = lab.GetFilteredRoundTripDocumentTombstone(LabNode.A);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.False(
            nodeADocument.Exists,
            $"Expected document tombstone '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)} and remove the live document. " +
            $"nodeADocumentExists={nodeADocument.Exists}, nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', " +
            $"nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeATombstone.Exists,
            $"Expected document tombstone '{lab.FilteredRoundTripTicketId}' to exist on {NodeTag(filteredPassReceiveSide, LabNode.A)} after filtered pass. " +
            $"nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps tombstone lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered document tombstone pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone pass", nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCTombstone = lab.GetFilteredRoundTripDocumentTombstone(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.False(
            nodeCDocument.Exists,
            $"Expected filtered document tombstone '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstone: exists={nodeCTombstone.Exists}, CV='{nodeCTombstone.ChangeVector ?? "<null>"}'. " +
            $"nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCTombstone.Exists,
            $"Expected document tombstone '{lab.FilteredRoundTripTicketId}' to exist on {NodeTag(filteredPassReceiveSide, LabNode.C)} after the internal hop. " +
            $"nodeCTombstoneExists={nodeCTombstone.Exists}, nodeCTombstoneCV='{nodeCTombstone.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in tombstone Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered document tombstone internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered document tombstone internal hop", nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify recreating the document on node A preserves filtered tombstone Version lineage without inflating database change vectors.
        const string recreatedOnNodeAName = itemName + "-recreated-on-node-a";

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone before local recreate", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone before local recreate", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered document tombstone before local recreate", nodeADbCvBeforeLocalChange, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.A, recreatedOnNodeAName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, recreatedOnNodeAName);

        var nodeADocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local recreate", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local recreate", nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "document after local recreate", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "document after local recreate", nodeADbCvAfterLocalChange, nodeADocumentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripDocumentNameOrConflictAsync(LabNode.C, recreatedOnNodeAName);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "document recreated from filtered tombstone", nodeCTombstone.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCDocumentAfterLocalChange = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "document recreated from filtered tombstone", nodeCTombstone.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector, nodeCDocumentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "document recreated from filtered tombstone", nodeCTombstone.ChangeVector, nodeADocumentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local recreate replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local recreate replicated from node A", nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "document after local recreate replicated from node A", nodeCDbCvAfterLocalChange, nodeCDocumentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task RevisionDocument_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "revision-document";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.EnableRevisionsAsync(ClusterSide.Hub);
        await lab.EnableRevisionsAsync(ClusterSide.Sink);
        var initialName = itemName + "-initial";
        var revisionName = itemName + "-revision";

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, initialName);

        await lab.ForceFilteredRoundTripRevisionAsync(LabNode.B);
        var initialRevision = lab.GetFilteredRoundTripLatestRevision(LabNode.B);
        await lab.WaitForFilteredRoundTripRevisionAsync(LabNode.A, initialRevision.ChangeVector);
        await lab.WaitForFilteredRoundTripRevisionAsync(LabNode.C, initialRevision.ChangeVector);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, revisionName);
        await lab.ForceFilteredRoundTripRevisionAsync(LabNode.B);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBDocumentAfterRevision = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeBRevision = lab.GetFilteredRoundTripLatestRevision(LabNode.B);
        var originalIncomingChangeVector = nodeBRevision.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBRevision.Exists,
            $"Expected revision on node B for '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBRevisionExists={nodeBRevision.Exists}, nodeBRevisionName='{nodeBRevision.Name ?? "<null>"}', " +
            $"nodeBRevisionCV='{nodeBRevision.ChangeVector ?? "<null>"}', nodeBRevisionCount={nodeBRevision.Count}, " +
            $"nodeBDocumentName='{nodeBDocumentAfterRevision.Name ?? "<null>"}', nodeBDocumentCV='{nodeBDocumentAfterRevision.ChangeVector ?? "<null>"}', " +
            $"initialRevisionCV='{initialRevision.ChangeVector ?? "<null>"}'.");
        Assert.True(
            string.Equals(nodeBRevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected revision on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have name '{revisionName}' before filtered pass. " +
            $"actualName='{nodeBRevision.Name ?? "<null>"}', revisionCV='{nodeBRevision.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected revision document change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBRevisionCV='{nodeBRevision.ChangeVector}', nodeBDocumentAfterRevisionCV='{nodeBDocumentAfterRevision.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedLatestRevisionName(revisionName);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeARevision = lab.GetFilteredRoundTripLatestRevision(LabNode.A);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected revision owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeARevision.Exists,
            $"Expected revision for '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeARevisionExists={nodeARevision.Exists}, nodeARevisionName='{nodeARevision.Name ?? "<null>"}', nodeARevisionCV='{nodeARevision.ChangeVector ?? "<null>"}', " +
            $"nodeBRevisionCV='{nodeBRevision.ChangeVector}', nodeARevisionCount={nodeARevision.Count}.");
        Assert.True(
            string.Equals(nodeARevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected latest revision on {NodeTag(filteredPassReceiveSide, LabNode.A)} to keep name '{revisionName}'. " +
            $"actualName='{nodeARevision.Name ?? "<null>"}', CV='{nodeARevision.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps revision lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered revision document pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision after pass", nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision after pass", nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision document pass", nodeADbCvAfterPass, nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCRevision = lab.GetFilteredRoundTripLatestRevision(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCDocument.Exists,
            $"Expected filtered revision document '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCRevision: exists={nodeCRevision.Exists}, CV='{nodeCRevision.ChangeVector ?? "<null>"}', count={nodeCRevision.Count}. " +
            $"nodeARevisionCV='{nodeARevision.ChangeVector ?? "<null>"}', nodeBRevisionCV='{nodeBRevision.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCRevision.Exists,
            $"Expected latest revision for '{lab.FilteredRoundTripTicketId}' to exist on {NodeTag(filteredPassReceiveSide, LabNode.C)} after internal hop. " +
            $"nodeCRevisionExists={nodeCRevision.Exists}, nodeCRevisionCV='{nodeCRevision.ChangeVector ?? "<null>"}'.");
        Assert.True(
            string.Equals(nodeCRevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected latest revision on {NodeTag(filteredPassReceiveSide, LabNode.C)} to keep name '{revisionName}'. " +
            $"actualName='{nodeCRevision.Name ?? "<null>"}', CV='{nodeCRevision.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in revision Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered revision document internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCRevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision after internal hop", nodeCRevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision after internal hop", nodeCRevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision document internal hop", nodeCDbCvAfterPass, nodeCRevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local revision on node A preserves filtered revision Version lineage without inflating database change vectors.
        const string localRevisionName = itemName + "-revision-on-node-a";

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision before local revision", nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision before local revision", nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision before local revision", nodeADbCvBeforeLocalChange, nodeARevision.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.A, localRevisionName);
        await lab.ForceFilteredRoundTripRevisionAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.A, localRevisionName);

        var nodeARevisionAfterLocalChange = lab.GetFilteredRoundTripLatestRevision(LabNode.A);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "revision after local revision", nodeARevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "revision after local revision", nodeARevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "revision after local revision", nodeADbCvAfterLocalChange, nodeARevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "revision after local revision", nodeADbCvAfterLocalChange, nodeARevisionAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.C, localRevisionName);

        var nodeCRevisionAfterLocalChange = lab.GetFilteredRoundTripLatestRevision(LabNode.C);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "revision update from filtered revision", nodeCRevision.ChangeVector, nodeARevisionAfterLocalChange.ChangeVector, nodeCRevisionAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "revision update from filtered revision", nodeCRevision.ChangeVector, nodeARevisionAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "revision after local revision replicated from node A", nodeCRevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "revision after local revision replicated from node A", nodeCRevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "revision after local revision replicated from node A", nodeCDbCvAfterLocalChange, nodeCRevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task RevisionTombstone_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "revision-tombstone";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.EnableRevisionsAsync(ClusterSide.Hub);
        await lab.EnableRevisionsAsync(ClusterSide.Sink);
        var initialName = itemName + "-initial";
        var revisionOneName = itemName + "-revision-one";
        var revisionTwoName = itemName + "-revision-two";

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, initialName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, initialName);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, revisionOneName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, revisionOneName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, revisionOneName);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.B, revisionTwoName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A, revisionTwoName);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C, revisionTwoName);

        var revisionToDelete = lab.GetFilteredRoundTripLatestRevision(LabNode.B);
        Assert.True(
            revisionToDelete.Exists,
            $"Expected revision on node B for '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before deleting one revision.");

        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.A, revisionToDelete.Name);
        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.C, revisionToDelete.Name);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.A, to: [LabNode.B]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        await lab.DeleteFilteredRoundTripRevisionAsync(LabNode.B, revisionToDelete.ChangeVector);

        var nodeBDocumentAfterDeleteRevision = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeBTombstones = lab.GetFilteredRoundTripRevisionTombstones(LabNode.B);
        var nodeBTombstone = nodeBTombstones.OrderByDescending(x => x.Etag).FirstOrDefault();
        var originalIncomingChangeVector = nodeBTombstone?.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        Assert.True(
            nodeBTombstone != null,
            $"Expected revision tombstone on node B for '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} after deleting revision '{revisionToDelete.ChangeVector}'. " +
            $"nodeBTombstones='{FormatRevisionTombstones(nodeBTombstones)}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected revision tombstone change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', revisionToDeleteCV='{revisionToDelete.ChangeVector}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBTombstoneKeyCV='{nodeBTombstone.KeyChangeVector}', nodeBDocumentAfterDeleteRevisionCV='{nodeBDocumentAfterDeleteRevision.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedRevisionTombstone(revisionTwoName);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeATombstones = lab.GetFilteredRoundTripRevisionTombstones(LabNode.A);
        var nodeATombstone = nodeATombstones.OrderByDescending(x => x.Etag).FirstOrDefault();
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected revision tombstone owner document '{lab.FilteredRoundTripTicketId}' to remain on {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeATombstones.Count > 0,
            $"Expected revision tombstone for '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeATombstones='{FormatRevisionTombstones(nodeATombstones)}', nodeBTombstones='{FormatRevisionTombstones(nodeBTombstones)}'.");
        Assert.NotNull(nodeATombstone);

        // Verify the filtered pass into node A keeps revision tombstone lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone pass", nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCTombstones = lab.GetFilteredRoundTripRevisionTombstones(LabNode.C);
        var nodeCTombstone = nodeCTombstones.OrderByDescending(x => x.Etag).FirstOrDefault();
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCTombstones.Count > 0,
            $"Expected filtered revision tombstone '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstones='{FormatRevisionTombstones(nodeCTombstones)}', nodeATombstones='{FormatRevisionTombstones(nodeATombstones)}'.");
        Assert.NotNull(nodeCTombstone);

        // Verify node C also keeps node B lineage in revision tombstone Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered revision tombstone internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered revision tombstone internal hop", nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local revision on node A after the filtered tombstone does not leak tombstone lineage into database change vectors.
        const string localRevisionName = itemName + "-revision-on-node-a";

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone before local revision", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone before local revision", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered revision tombstone before local revision", nodeADbCvBeforeLocalChange, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.StoreFilteredRoundTripTicketWithNameAsync(LabNode.A, localRevisionName);
        await lab.ForceFilteredRoundTripRevisionAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.A, localRevisionName);

        var nodeARevisionAfterLocalChange = lab.GetFilteredRoundTripLatestRevision(LabNode.A);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "revision after local revision following filtered tombstone", nodeADbCvAfterLocalChange, nodeARevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "revision after local revision following filtered tombstone", nodeADbCvAfterLocalChange, nodeARevisionAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripLatestRevisionNameAsync(LabNode.C, localRevisionName);

        var nodeCRevisionAfterLocalChange = lab.GetFilteredRoundTripLatestRevision(LabNode.C);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "revision after local revision following filtered tombstone replicated from node A", nodeCDbCvAfterLocalChange, nodeCRevisionAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Counters)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task Counter_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "counter";
        const string counterName = "views";
        const long expectedCounterValue = 1;

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);

        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);
        await lab.IncrementFilteredRoundTripCounterAsync(LabNode.B, counterName);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBCounter = lab.GetFilteredRoundTripCounter(LabNode.B, counterName);
        var nodeBDbCvAfterCounter = lab.GetDatabaseChangeVector(LabNode.B);
        var originalIncomingChangeVector = nodeBCounter.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(nodeBCounter.Exists, $"Expected counter '{counterName}' on '{lab.FilteredRoundTripTicketId}' to exist on {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass.");
        Assert.True(
            nodeBCounter.Value == expectedCounterValue,
            $"Expected counter '{counterName}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have value {expectedCounterValue} before filtered pass. " +
            $"actualValue={nodeBCounter.Value}, counterCV='{nodeBCounter.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected counter change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBCounterCV='{nodeBCounter.ChangeVector}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedCounter(counterName, expectedCounterValue);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeACounter = lab.GetFilteredRoundTripCounter(LabNode.A, counterName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected counter owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeACounter.Exists,
            $"Expected counter '{counterName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeACounterExists={nodeACounter.Exists}, nodeACounterValue={nodeACounter.Value}, nodeACounterCV='{nodeACounter.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeACounter.Value == expectedCounterValue,
            $"Expected counter '{counterName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} to have value {expectedCounterValue} after filtered pass. " +
            $"actualValue={nodeACounter.Value}, counterCV='{nodeACounter.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps counter lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered counter pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter after pass", nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter after pass", nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter pass", nodeADbCvAfterPass, nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCCounter = lab.GetFilteredRoundTripCounter(LabNode.C, counterName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCCounter.Exists,
            $"Expected filtered counter pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCCounter: exists={nodeCCounter.Exists}, value={nodeCCounter.Value}, CV='{nodeCCounter.ChangeVector ?? "<null>"}'. " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}'.");
        Assert.True(
            nodeCCounter.Value == expectedCounterValue,
            $"Expected counter '{counterName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} to have value {expectedCounterValue} after internal hop. " +
            $"actualValue={nodeCCounter.Value}, counterCV='{nodeCCounter.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in counter Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered counter internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCCounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered counter after internal hop", nodeCCounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered counter after internal hop", nodeCCounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered counter internal hop", nodeCDbCvAfterPass, nodeCCounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local counter increment on node A preserves filtered counter Version lineage without inflating database change vectors.
        const long expectedCounterValueAfterLocalChange = expectedCounterValue + 1;

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter before local increment", nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter before local increment", nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered counter before local increment", nodeADbCvBeforeLocalChange, nodeACounter.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.IncrementFilteredRoundTripCounterAsync(LabNode.A, counterName);
        await lab.WaitForFilteredRoundTripCounterAsync(LabNode.A, counterName, expectedCounterValueAfterLocalChange);

        var nodeACounterAfterLocalChange = lab.GetFilteredRoundTripCounter(LabNode.A, counterName);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "counter after local increment", nodeACounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "counter after local increment", nodeACounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "counter after local increment", nodeADbCvAfterLocalChange, nodeACounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "counter after local increment", nodeADbCvAfterLocalChange, nodeACounterAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripCounterOrConflictAsync(LabNode.C, counterName, expectedCounterValueAfterLocalChange);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "counter increment from filtered counter", nodeCCounter.ChangeVector, nodeACounterAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCCounterAfterLocalChange = lab.GetFilteredRoundTripCounter(LabNode.C, counterName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "counter increment from filtered counter", nodeCCounter.ChangeVector, nodeACounterAfterLocalChange.ChangeVector, nodeCCounterAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "counter increment from filtered counter", nodeCCounter.ChangeVector, nodeACounterAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "counter after local increment replicated from node A", nodeCCounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "counter after local increment replicated from node A", nodeCCounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "counter after local increment replicated from node A", nodeCDbCvAfterLocalChange, nodeCCounterAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Attachments)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task Attachment_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "attachment";
        const string attachmentName = "data.bin";
        const string contentType = "application/octet-stream";
        var content = new byte[] { 1, 2, 3, 4, 5 };

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);

        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.PutFilteredRoundTripAttachmentAsync(LabNode.B, attachmentName, content, contentType);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBAttachment = lab.GetFilteredRoundTripAttachment(LabNode.B, attachmentName);
        var originalIncomingChangeVector = nodeBAttachment.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBAttachment.Exists,
            $"Expected attachment on node B '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBAttachmentExists={nodeBAttachment.Exists}, nodeBAttachmentSize={nodeBAttachment.Size}, nodeBAttachmentCV='{nodeBAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBAttachment.Size == content.Length,
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have size {content.Length} before filtered pass. " +
            $"actualSize={nodeBAttachment.Size}, CV='{nodeBAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected attachment change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBAttachmentCV='{nodeBAttachment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedAttachment(attachmentName, nodeBAttachment.Hash, content.Length);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeAAttachment = lab.GetFilteredRoundTripAttachment(LabNode.A, attachmentName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected attachment owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeAAttachment.Exists,
            $"Expected attachment '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeAAttachmentExists={nodeAAttachment.Exists}, nodeAAttachmentHash='{nodeAAttachment.Hash ?? "<null>"}', nodeAAttachmentSize={nodeAAttachment.Size}, nodeBAttachmentHash='{nodeBAttachment.Hash}'.");
        Assert.True(
            string.Equals(nodeAAttachment.Hash, nodeBAttachment.Hash, StringComparison.Ordinal),
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} to keep hash from {NodeTag(filteredPassReceiveSide, LabNode.B)}. " +
            $"expectedHash='{nodeBAttachment.Hash}', actualHash='{nodeAAttachment.Hash ?? "<null>"}', CV='{nodeAAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeAAttachment.Size == content.Length,
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} to have size {content.Length}. " +
            $"actualSize={nodeAAttachment.Size}, CV='{nodeAAttachment.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps attachment lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered attachment pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment after pass", nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment after pass", nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment pass", nodeADbCvAfterPass, nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCAttachment = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCAttachment.Exists,
            $"Expected filtered attachment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCAttachment: exists={nodeCAttachment.Exists}, hash='{nodeCAttachment.Hash ?? "<null>"}', size={nodeCAttachment.Size}, CV='{nodeCAttachment.ChangeVector ?? "<null>"}'. " +
            $"nodeAAttachmentCV='{nodeAAttachment.ChangeVector ?? "<null>"}', nodeBAttachmentCV='{nodeBAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            string.Equals(nodeCAttachment.Hash, nodeBAttachment.Hash, StringComparison.Ordinal),
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} to keep hash from {NodeTag(filteredPassReceiveSide, LabNode.B)}. " +
            $"expectedHash='{nodeBAttachment.Hash}', actualHash='{nodeCAttachment.Hash ?? "<null>"}', CV='{nodeCAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCAttachment.Size == content.Length,
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} to have size {content.Length}. " +
            $"actualSize={nodeCAttachment.Size}, CV='{nodeCAttachment.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in attachment Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered attachment internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment after internal hop", nodeCAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment after internal hop", nodeCAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment internal hop", nodeCDbCvAfterPass, nodeCAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local attachment update on node A preserves filtered attachment Version lineage without inflating database change vectors.
        var localContent = new byte[] { 6, 7, 8, 9 };

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment before local update", nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment before local update", nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment before local update", nodeADbCvBeforeLocalChange, nodeAAttachment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.PutFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, localContent, contentType);

        var nodeAAttachmentAfterLocalChange = lab.GetFilteredRoundTripAttachment(LabNode.A, attachmentName);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, nodeAAttachmentAfterLocalChange.Hash, localContent.Length);

        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local update", nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local update", nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local update", nodeADbCvAfterLocalChange, nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "attachment after local update", nodeADbCvAfterLocalChange, nodeAAttachmentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripAttachmentOrConflictAsync(LabNode.C, attachmentName, nodeAAttachmentAfterLocalChange.Hash, localContent.Length);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "attachment update from filtered attachment", nodeCAttachment.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCAttachmentAfterLocalChange = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "attachment update from filtered attachment", nodeCAttachment.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector, nodeCAttachmentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "attachment update from filtered attachment", nodeCAttachment.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local update replicated from node A", nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local update replicated from node A", nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local update replicated from node A", nodeCDbCvAfterLocalChange, nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Attachments)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task AttachmentTombstone_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "attachment-tombstone";
        const string attachmentName = "deleted.bin";
        const string contentType = "application/octet-stream";
        var content = new byte[] { 9, 8, 7 };

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);

        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.PutFilteredRoundTripAttachmentAsync(LabNode.B, attachmentName, content, contentType);
        var nodeBAttachmentBeforeDelete = lab.GetFilteredRoundTripAttachment(LabNode.B, attachmentName);

        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, nodeBAttachmentBeforeDelete.Hash, content.Length);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.C, attachmentName, nodeBAttachmentBeforeDelete.Hash, content.Length);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.DeleteFilteredRoundTripAttachmentAsync(LabNode.B, attachmentName);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBDocumentAfterDelete = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeBTombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.B, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var originalIncomingChangeVector = nodeBTombstone.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBAttachmentBeforeDelete.Exists,
            $"Expected attachment on node B '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before deleting it. " +
            $"nodeBAttachmentExists={nodeBAttachmentBeforeDelete.Exists}, nodeBAttachmentSize={nodeBAttachmentBeforeDelete.Size}, nodeBAttachmentCV='{nodeBAttachmentBeforeDelete.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBAttachmentBeforeDelete.Size == content.Length,
            $"Expected attachment '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have size {content.Length} before deleting it. " +
            $"actualSize={nodeBAttachmentBeforeDelete.Size}, CV='{nodeBAttachmentBeforeDelete.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBTombstone.Exists,
            $"Expected attachment tombstone on node B '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBTombstoneExists={nodeBTombstone.Exists}, nodeBDocumentAfterDeleteCV='{nodeBDocumentAfterDelete.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected attachment tombstone change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', nodeBDocumentAfterDeleteCV='{nodeBDocumentAfterDelete.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedAttachmentTombstone(attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeAAttachment = lab.GetFilteredRoundTripAttachment(LabNode.A, attachmentName);
        var nodeATombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.A, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected attachment tombstone owner document '{lab.FilteredRoundTripTicketId}' to remain on {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.False(
            nodeAAttachment.Exists,
            $"Expected attachment tombstone '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)} and remove the live attachment. " +
            $"nodeAAttachmentExists={nodeAAttachment.Exists}, nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeATombstone.Exists,
            $"Expected attachment tombstone '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} after filtered pass. " +
            $"nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps attachment tombstone lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone after pass", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone pass", nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCAttachment = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCTombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.C, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.False(
            nodeCAttachment.Exists,
            $"Expected filtered attachment tombstone pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCAttachment: exists={nodeCAttachment.Exists}, CV='{nodeCAttachment.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstone: exists={nodeCTombstone.Exists}, CV='{nodeCTombstone.ChangeVector ?? "<null>"}'. " +
            $"nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCTombstone.Exists,
            $"Expected attachment tombstone '{attachmentName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} after internal hop. " +
            $"nodeCTombstoneExists={nodeCTombstone.Exists}, nodeCTombstoneCV='{nodeCTombstone.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in attachment tombstone Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone after internal hop", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop", nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify recreating the attachment on node A preserves filtered attachment tombstone Version lineage without inflating database change vectors.
        var recreatedContent = content;

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before local recreate", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before local recreate", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before local recreate", nodeADbCvBeforeLocalChange, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.PutFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, recreatedContent, contentType);

        var nodeAAttachmentAfterLocalChange = lab.GetFilteredRoundTripAttachment(LabNode.A, attachmentName);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, nodeAAttachmentAfterLocalChange.Hash, recreatedContent.Length);

        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local recreate", nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local recreate", nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment after local recreate", nodeADbCvAfterLocalChange, nodeAAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "attachment after local recreate", nodeADbCvAfterLocalChange, nodeAAttachmentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripAttachmentOrConflictAsync(LabNode.C, attachmentName, nodeAAttachmentAfterLocalChange.Hash, recreatedContent.Length);

        var nodeCConflictsAfterLocalChange = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "attachment recreated from filtered attachment tombstone", nodeCTombstone.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector, nodeCConflictsAfterLocalChange);

        var nodeCAttachmentAfterLocalChange = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "attachment recreated from filtered attachment tombstone", nodeCTombstone.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector, nodeCAttachmentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "attachment recreated from filtered attachment tombstone", nodeCTombstone.ChangeVector, nodeAAttachmentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local recreate replicated from node A", nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local recreate replicated from node A", nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment after local recreate replicated from node A", nodeCDbCvAfterLocalChange, nodeCAttachmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Attachments | RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task AttachmentRestoredFromRevisionAfterFilteredTombstone_ShouldPreserveAttachmentTombstoneLineage(Options options)
    {
        const ClusterSide filteredPassReceiveSide = ClusterSide.Hub;
        const string itemName = "attachment-reverted-from-revision";
        const string attachmentName = "reverted.bin";
        const string contentType = "application/octet-stream";
        var content = new byte[] { 5, 4, 3, 2, 1 };

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.EnableRevisionsAsync(ClusterSide.Hub);
        await lab.EnableRevisionsAsync(ClusterSide.Sink);

        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.PutFilteredRoundTripAttachmentAsync(LabNode.B, attachmentName, content, contentType);
        var nodeBAttachmentBeforeDelete = lab.GetFilteredRoundTripAttachment(LabNode.B, attachmentName);

        await lab.ForceFilteredRoundTripRevisionAsync(LabNode.B);
        var revisionWithAttachment = lab.GetFilteredRoundTripLatestRevision(LabNode.B);

        Assert.True(
            nodeBAttachmentBeforeDelete.Exists,
            $"Expected attachment '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before creating the revision. " +
            $"nodeBAttachmentExists={nodeBAttachmentBeforeDelete.Exists}, nodeBAttachmentSize={nodeBAttachmentBeforeDelete.Size}, nodeBAttachmentCV='{nodeBAttachmentBeforeDelete.ChangeVector ?? "<null>"}'.");
        Assert.True(
            revisionWithAttachment.Exists,
            $"Expected forced revision with attachment metadata for '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before deleting the attachment. " +
            $"revisionExists={revisionWithAttachment.Exists}, revisionName='{revisionWithAttachment.Name ?? "<null>"}', revisionCV='{revisionWithAttachment.ChangeVector ?? "<null>"}', revisionCount={revisionWithAttachment.Count}.");

        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, nodeBAttachmentBeforeDelete.Hash, content.Length);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.C, attachmentName, nodeBAttachmentBeforeDelete.Hash, content.Length);
        await lab.WaitForFilteredRoundTripRevisionAsync(LabNode.A, revisionWithAttachment.ChangeVector);
        await lab.WaitForFilteredRoundTripRevisionAsync(LabNode.C, revisionWithAttachment.ChangeVector);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.DeleteFilteredRoundTripAttachmentAsync(LabNode.B, attachmentName);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBTombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.B, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var originalIncomingChangeVector = nodeBTombstone.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBTombstone.Exists,
            $"Expected attachment tombstone '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBTombstoneExists={nodeBTombstone.Exists}, nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected attachment tombstone on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedAttachmentTombstone(attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeATombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.A, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(
            nodeATombstone.Exists,
            $"Expected filtered attachment tombstone '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)} before reverting the document revision. " +
            $"nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps attachment tombstone lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before revision revert", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before revision revert", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before revision revert", nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered attachment tombstone before revision revert", nodeADbCvAfterPass, nodeATombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCTombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.C, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCTombstone.Exists,
            $"Expected filtered attachment tombstone '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} before reverting the document revision. " +
            $"nodeCTombstoneExists={nodeCTombstone.Exists}, nodeCTombstoneCV='{nodeCTombstone.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in attachment tombstone Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop before revision revert", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop before revision revert", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop before revision revert", nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered attachment tombstone internal hop before revision revert", nodeCDbCvAfterPass, nodeCTombstone.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify ClientAPI revision revert restores the attachment through PutAttachmentRevert -> PutDirect(null) without losing tombstone Version lineage.
        var nodeADbCvBeforeRevert = lab.GetDatabaseChangeVector(LabNode.A);

        await lab.RevertFilteredRoundTripDocumentToRevisionAsync(LabNode.A, revisionWithAttachment.ChangeVector);
        await lab.WaitForFilteredRoundTripAttachmentAsync(LabNode.A, attachmentName, nodeBAttachmentBeforeDelete.Hash, content.Length);

        var nodeAAttachmentAfterRevert = lab.GetFilteredRoundTripAttachment(LabNode.A, attachmentName);
        var nodeADbCvAfterRevert = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment restored by revision revert", nodeAAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment restored by revision revert", nodeAAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "attachment restored by revision revert", nodeADbCvAfterRevert, nodeAAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "attachment restored by revision revert", nodeADbCvAfterRevert, nodeAAttachmentAfterRevert.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "attachment restored by revision revert", nodeADbCvBeforeRevert, nodeADbCvAfterRevert, nodeAAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);

        await lab.WaitForFilteredRoundTripAttachmentOrConflictAsync(LabNode.C, attachmentName, nodeAAttachmentAfterRevert.Hash, content.Length);

        var nodeCConflictsAfterRevert = lab.GetFilteredRoundTripConflicts(LabNode.C);
        AssertNoConflictsAfterLocalChange(filteredPassReceiveSide, "attachment restored by revision revert", nodeCTombstone.ChangeVector, nodeAAttachmentAfterRevert.ChangeVector, nodeCConflictsAfterRevert);

        var nodeCAttachmentAfterRevert = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCDbCvAfterRevert = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "attachment restored by revision revert", nodeCTombstone.ChangeVector, nodeAAttachmentAfterRevert.ChangeVector, nodeCAttachmentAfterRevert.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "attachment restored by revision revert", nodeCTombstone.ChangeVector, nodeAAttachmentAfterRevert.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment restored by revision revert replicated from node A", nodeCAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment restored by revision revert replicated from node A", nodeCAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "attachment restored by revision revert replicated from node A", nodeCDbCvAfterRevert, nodeCAttachmentAfterRevert.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.TimeSeries)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task TimeSeriesSegment_NewSegment_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string timeSeriesName = "HeartRate";
        const string itemName = "time-series-new-segment";
        var firstTimestamp = new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var passedTimestamp = firstTimestamp.AddDays(30);
        const int expectedValueCount = 2;

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.B, timeSeriesName, firstTimestamp, 72);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.A, timeSeriesName, expectedValueCount: 1);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.C, timeSeriesName, expectedValueCount: 1);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.B, timeSeriesName, passedTimestamp, 73);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.B, timeSeriesName);
        var originalIncomingChangeVector = nodeBSegment.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBSegment.Exists,
            $"Expected time series segment on node B '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBSegmentExists={nodeBSegment.Exists}, nodeBSegmentValueCount={nodeBSegment.ValueCount}, nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBSegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have at least {expectedValueCount} values before filtered pass. " +
            $"actualValueCount={nodeBSegment.ValueCount}, segmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected time series segment change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedTimeSeriesSegment(timeSeriesName, expectedValueCount);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeASegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.A, timeSeriesName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected time series owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeASegment.Exists,
            $"Expected time series '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeASegmentExists={nodeASegment.Exists}, nodeASegmentValueCount={nodeASegment.ValueCount}, nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeASegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} to have at least {expectedValueCount} values after filtered pass. " +
            $"actualValueCount={nodeASegment.ValueCount}, segmentCV='{nodeASegment.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps time series segment lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered time series segment pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment after pass", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment after pass", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment pass", nodeADbCvAfterPass, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCSegment.Exists,
            $"Expected filtered time series segment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCSegment: exists={nodeCSegment.Exists}, valueCount={nodeCSegment.ValueCount}, CV='{nodeCSegment.ChangeVector ?? "<null>"}'. " +
            $"nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCSegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} to have at least {expectedValueCount} values after internal hop. " +
            $"actualValueCount={nodeCSegment.ValueCount}, segmentCV='{nodeCSegment.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in time series segment Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered time series segment internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered time series segment after internal hop", nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered time series segment after internal hop", nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered time series segment internal hop", nodeCDbCvAfterPass, nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local append on node A preserves filtered time series segment Version lineage without inflating database change vectors.
        var localTimestamp = passedTimestamp.AddMinutes(1);
        const int expectedValueCountAfterLocalChange = expectedValueCount + 1;

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment before local append", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment before local append", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered time series segment before local append", nodeADbCvBeforeLocalChange, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.A, timeSeriesName, localTimestamp, 74);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.A, timeSeriesName, expectedValueCountAfterLocalChange);

        var nodeASegmentAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.A, timeSeriesName);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "time series segment after local append", nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "time series segment after local append", nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "time series segment after local append", nodeADbCvAfterLocalChange, nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "time series segment after local append", nodeADbCvAfterLocalChange, nodeASegmentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.C, timeSeriesName, expectedValueCountAfterLocalChange);

        var nodeCSegmentAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "time series segment append from filtered segment", nodeCSegment.ChangeVector, nodeASegmentAfterLocalChange.ChangeVector, nodeCSegmentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "time series segment append from filtered segment", nodeCSegment.ChangeVector, nodeASegmentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "time series segment after local append replicated from node A", nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "time series segment after local append replicated from node A", nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "time series segment after local append replicated from node A", nodeCDbCvAfterLocalChange, nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.TimeSeries)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task TimeSeriesSegment_UpdateExistingSegment_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string timeSeriesName = "HeartRate";
        const string itemName = "time-series-update-segment";
        var firstTimestamp = new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var passedTimestamp = firstTimestamp.AddMinutes(1);
        const int expectedValueCount = 2;

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.B, timeSeriesName, firstTimestamp, 72);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.A, timeSeriesName, expectedValueCount: 1);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.C, timeSeriesName, expectedValueCount: 1);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.B, timeSeriesName, passedTimestamp, 73);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.B, timeSeriesName);
        var originalIncomingChangeVector = nodeBSegment.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBSegment.Exists,
            $"Expected time series segment on node B '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBSegmentExists={nodeBSegment.Exists}, nodeBSegmentValueCount={nodeBSegment.ValueCount}, nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBSegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to have at least {expectedValueCount} values before filtered pass. " +
            $"actualValueCount={nodeBSegment.ValueCount}, segmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected time series segment change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedTimeSeriesSegment(timeSeriesName, expectedValueCount);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeASegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.A, timeSeriesName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected time series owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeASegment.Exists,
            $"Expected time series '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeASegmentExists={nodeASegment.Exists}, nodeASegmentValueCount={nodeASegment.ValueCount}, nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeASegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.A)} to have at least {expectedValueCount} values after filtered pass. " +
            $"actualValueCount={nodeASegment.ValueCount}, segmentCV='{nodeASegment.ChangeVector ?? "<null>"}'.");

        // Verify the filtered pass into node A keeps updated time series segment lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment after pass", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment after pass", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment pass", nodeADbCvAfterPass, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCSegment.Exists,
            $"Expected filtered time series segment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCSegment: exists={nodeCSegment.Exists}, valueCount={nodeCSegment.ValueCount}, CV='{nodeCSegment.ChangeVector ?? "<null>"}'. " +
            $"nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeCSegment.ValueCount >= expectedValueCount,
            $"Expected time series '{timeSeriesName}' on {NodeTag(filteredPassReceiveSide, LabNode.C)} to have at least {expectedValueCount} values after internal hop. " +
            $"actualValueCount={nodeCSegment.ValueCount}, segmentCV='{nodeCSegment.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in updated time series segment Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered updated time series segment internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered updated time series segment after internal hop", nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered updated time series segment after internal hop", nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered updated time series segment internal hop", nodeCDbCvAfterPass, nodeCSegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify a local append on node A preserves filtered updated segment Version lineage without inflating database change vectors.
        var localTimestamp = passedTimestamp.AddMinutes(1);
        const int expectedValueCountAfterLocalChange = expectedValueCount + 1;

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment before local append", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment before local append", nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered updated time series segment before local append", nodeADbCvBeforeLocalChange, nodeASegment.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.A, timeSeriesName, localTimestamp, 74);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.A, timeSeriesName, expectedValueCountAfterLocalChange);

        var nodeASegmentAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.A, timeSeriesName);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "updated time series segment after local append", nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "updated time series segment after local append", nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "updated time series segment after local append", nodeADbCvAfterLocalChange, nodeASegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "updated time series segment after local append", nodeADbCvAfterLocalChange, nodeASegmentAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.C, timeSeriesName, expectedValueCountAfterLocalChange);

        var nodeCSegmentAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "updated time series segment append from filtered segment", nodeCSegment.ChangeVector, nodeASegmentAfterLocalChange.ChangeVector, nodeCSegmentAfterLocalChange.ChangeVector);
        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "updated time series segment append from filtered segment", nodeCSegment.ChangeVector, nodeASegmentAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "updated time series segment after local append replicated from node A", nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "updated time series segment after local append replicated from node A", nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "updated time series segment after local append replicated from node A", nodeCDbCvAfterLocalChange, nodeCSegmentAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.TimeSeries)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task TimeSeriesDeletedRange_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "time-series-deleted-range";
        const string timeSeriesName = "HeartRate";
        var firstTimestamp = new DateTime(2024, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var deleteFrom = firstTimestamp;
        var deleteTo = firstTimestamp.AddMinutes(1);

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        await lab.StoreFilteredRoundTripTicketAsync(LabNode.B);
        await lab.AppendFilteredRoundTripTimeSeriesAsync(LabNode.B, timeSeriesName, firstTimestamp, 72);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.A);
        await lab.WaitForFilteredRoundTripDocumentNameAsync(LabNode.C);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.A, timeSeriesName, expectedValueCount: 1);
        await lab.WaitForFilteredRoundTripTimeSeriesSegmentAsync(LabNode.C, timeSeriesName, expectedValueCount: 1);

        await lab.BlockInternalReplicationUntilBlockedAsync(from: LabNode.B, to: [LabNode.A, LabNode.C]);
        await lab.StoreAllowedTicketThenFilteredOutUserAsync(LabNode.B);

        await lab.DeleteFilteredRoundTripTimeSeriesRangeAsync(LabNode.B, timeSeriesName, deleteFrom, deleteTo);

        var nodeBDatabaseId = lab.GetDatabaseIdFor(LabNode.B);
        var nodeBDeletedRange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.B, timeSeriesName);
        var originalIncomingChangeVector = nodeBDeletedRange.ChangeVector;
        var nodeBEtagInPassedChangeCv = GetEtag(originalIncomingChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeBDeletedRange.Exists,
            $"Expected deleted time series range on node B '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBDeletedRangeExists={nodeBDeletedRange.Exists}, nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected deleted time series range change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedTimeSeriesDeletedRange(timeSeriesName);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var nodeADeletedRange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.A, timeSeriesName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);

        Assert.True(nodeADocument.Exists, $"Expected deleted range owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(nodeADeletedRange.Exists, $"Expected deleted time series range '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");

        // Verify the filtered pass into node A keeps deleted range lineage in Version only.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range pass", nodeADbCvBeforePass, nodeADbCvAfterPass, nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range after pass", nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range after pass", nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range pass", nodeADbCvAfterPass, nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCDeletedRange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);

        Assert.True(
            nodeCDeletedRange.Exists,
            $"Expected filtered deleted time series range pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCDeletedRange: exists={nodeCDeletedRange.Exists}, CV='{nodeCDeletedRange.ChangeVector ?? "<null>"}'. " +
            $"nodeADeletedRangeCV='{nodeADeletedRange.ChangeVector ?? "<null>"}', nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector ?? "<null>"}'.");

        // Verify node C also keeps node B lineage in deleted range Version only after the internal hop.
        AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(filteredPassReceiveSide, LabNode.C, "filtered deleted time series range internal hop", nodeCDbCvBeforePass, nodeCDbCvAfterPass, nodeCDeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered deleted time series range after internal hop", nodeCDeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered deleted time series range after internal hop", nodeCDeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "filtered deleted time series range internal hop", nodeCDbCvAfterPass, nodeCDeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        // Verify extending the deleted range on node A preserves filtered deleted range Version lineage without inflating database change vectors.
        var localDeleteTo = deleteTo.AddMinutes(1);

        var nodeADbCvBeforeLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range before local delete", nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range before local delete", nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "filtered deleted time series range before local delete", nodeADbCvBeforeLocalChange, nodeADeletedRange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);

        await lab.DeleteFilteredRoundTripTimeSeriesRangeAsync(LabNode.A, timeSeriesName, deleteFrom, localDeleteTo);
        await lab.WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(LabNode.A, timeSeriesName, deleteFrom, localDeleteTo);

        var nodeADeletedRangeAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.A, timeSeriesName);
        var nodeADbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.A);

        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.A, "deleted time series range after local delete", nodeADeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "deleted time series range after local delete", nodeADeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.A, "deleted time series range after local delete", nodeADbCvAfterLocalChange, nodeADeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorCoversLocalItemOrder(filteredPassReceiveSide, LabNode.A, "deleted time series range after local delete", nodeADbCvAfterLocalChange, nodeADeletedRangeAfterLocalChange.ChangeVector, lab.GetDatabaseIdFor(LabNode.A));

        await lab.WaitForFilteredRoundTripTimeSeriesDeletedRangeAsync(LabNode.C, timeSeriesName, deleteFrom, localDeleteTo);

        var nodeCDeletedRangeAfterLocalChange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterLocalChange = lab.GetDatabaseChangeVector(LabNode.C);

        AssertLocalChangeIsCausalSuccessor(filteredPassReceiveSide, "deleted time series range extended on node A", nodeCDeletedRange.ChangeVector, nodeADeletedRangeAfterLocalChange.ChangeVector);
        AssertReplicatedItemKeptSourceChangeVector(filteredPassReceiveSide, "deleted time series range extended on node A", nodeCDeletedRange.ChangeVector, nodeADeletedRangeAfterLocalChange.ChangeVector, nodeCDeletedRangeAfterLocalChange.ChangeVector);
        AssertItemVersionPreservesPassedLineage(filteredPassReceiveSide, LabNode.C, "deleted time series range after local delete replicated from node A", nodeCDeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertItemOrderDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "deleted time series range after local delete replicated from node A", nodeCDeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
        AssertDatabaseChangeVectorDoesNotCarryPassedLineage(filteredPassReceiveSide, LabNode.C, "deleted time series range after local delete replicated from node A", nodeCDbCvAfterLocalChange, nodeCDeletedRangeAfterLocalChange.ChangeVector, originalIncomingChangeVector, nodeBDatabaseId, nodeBEtagInPassedChangeCv);
    }

    private static void AssertNoConflictsAfterLocalChange(
        ClusterSide filteredPassReceiveSide,
        string itemDescription,
        string filteredPredecessorChangeVector,
        string localChangeVector,
        List<ConflictSnapshot> conflicts)
    {
        var filteredPredecessorVersion = GetVersionChangeVector(filteredPredecessorChangeVector);
        var localChangeVersion = GetVersionChangeVector(localChangeVector);
        var status = ChangeVectorUtils.GetConflictStatus(localChangeVersion, filteredPredecessorVersion);

        Assert.True(
            conflicts.Count == 0,
            $"Expected {itemDescription} from {NodeTag(filteredPassReceiveSide, LabNode.A)} to replicate into {NodeTag(filteredPassReceiveSide, LabNode.C)} without creating document conflicts. " +
            $"Actual conflict count={conflicts.Count}. This is the concrete replication consequence of broken lineage: the receiver cannot treat the local change as a clean successor of the filtered item it already has. " +
            $"versionStatusAgainstFilteredPredecessor={status}, filteredPredecessorCV='{filteredPredecessorChangeVector ?? "<null>"}', localChangeCV='{localChangeVector ?? "<null>"}', " +
            $"filteredPredecessorVersion='{filteredPredecessorVersion ?? "<null>"}', localChangeVersion='{localChangeVersion ?? "<null>"}', conflicts='{FormatConflicts(conflicts)}'.");
    }

    private static void AssertLocalChangeIsCausalSuccessor(
        ClusterSide filteredPassReceiveSide,
        string itemDescription,
        string filteredPredecessorChangeVector,
        string localChangeVector)
    {
        var filteredPredecessorVersion = GetVersionChangeVector(filteredPredecessorChangeVector);
        var localChangeVersion = GetVersionChangeVector(localChangeVector);
        var status = ChangeVectorUtils.GetConflictStatus(localChangeVersion, filteredPredecessorVersion);

        Assert.True(
            status is ConflictStatus.Update or ConflictStatus.AlreadyMerged,
            $"Expected {itemDescription} from {NodeTag(filteredPassReceiveSide, LabNode.A)} to be a causal successor of the filtered item already on {NodeTag(filteredPassReceiveSide, LabNode.C)} when comparing Version. " +
            $"Expected status Update or AlreadyMerged, actual status={status}. This is a replication consequence, not just a number mismatch: the receiver cannot classify the item Version as a clean update. " +
            $"filteredPredecessorCV='{filteredPredecessorChangeVector ?? "<null>"}', localChangeCV='{localChangeVector ?? "<null>"}', " +
            $"filteredPredecessorVersion='{filteredPredecessorVersion ?? "<null>"}', localChangeVersion='{localChangeVersion ?? "<null>"}'.");
    }

    private static void AssertReplicatedItemKeptSourceChangeVector(
        ClusterSide filteredPassReceiveSide,
        string itemDescription,
        string filteredPredecessorChangeVector,
        string sourceChangeVector,
        string replicatedChangeVector)
    {
        var filteredPredecessorVersion = GetVersionChangeVector(filteredPredecessorChangeVector);
        var sourceVersion = GetVersionChangeVector(sourceChangeVector);
        var status = ChangeVectorUtils.GetConflictStatus(sourceVersion, filteredPredecessorVersion);

        Assert.True(
            string.Equals(replicatedChangeVector, sourceChangeVector, StringComparison.Ordinal),
            $"Expected {itemDescription} to keep the exact source item CV when ordinary internal replication sends it from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)}. " +
            $"A different CV means {NodeTag(filteredPassReceiveSide, LabNode.C)} had to merge or rewrite the item instead of applying it as a clean successor. " +
            $"versionStatusAgainstFilteredPredecessor={status}, sourceCV='{sourceChangeVector ?? "<null>"}', replicatedCV='{replicatedChangeVector ?? "<null>"}', " +
            $"filteredPredecessorCV='{filteredPredecessorChangeVector ?? "<null>"}', sourceVersion='{sourceVersion ?? "<null>"}', " +
            $"filteredPredecessorVersion='{filteredPredecessorVersion ?? "<null>"}'.");
    }

    private static void AssertItemVersionPreservesPassedLineage(
        ClusterSide filteredPassReceiveSide,
        LabNode node,
        string itemDescription,
        string itemChangeVector,
        string originalIncomingChangeVector,
        string nodeBDatabaseId,
        long nodeBEtagInPassedChangeCv)
    {
        var actualVersionEtag = GetVersionEtag(itemChangeVector, nodeBDatabaseId);

        Assert.True(
            actualVersionEtag == nodeBEtagInPassedChangeCv,
            $"Expected {itemDescription} on {NodeTag(filteredPassReceiveSide, node)} to preserve {NodeTag(filteredPassReceiveSide, LabNode.B)} lineage in Version. " +
            $"Expected version etag={nodeBEtagInPassedChangeCv}, actual version etag={actualVersionEtag}. " +
            $"This points to the item being stored or forwarded without the original filtered Version lineage. " +
            $"itemCV='{itemChangeVector ?? "<null>"}', originalIncomingCV='{originalIncomingChangeVector ?? "<null>"}'.");
    }

    private static void AssertItemOrderDoesNotCarryPassedLineage(
        ClusterSide filteredPassReceiveSide,
        LabNode node,
        string itemDescription,
        string itemChangeVector,
        string originalIncomingChangeVector,
        string nodeBDatabaseId,
        long nodeBEtagInPassedChangeCv)
    {
        var actualOrderEtag = GetOrderEtag(itemChangeVector, nodeBDatabaseId);

        Assert.True(
            actualOrderEtag < nodeBEtagInPassedChangeCv,
            $"Expected {itemDescription} on {NodeTag(filteredPassReceiveSide, node)} not to carry full {NodeTag(filteredPassReceiveSide, LabNode.B)} lineage in Order. " +
            $"Expected order etag < {nodeBEtagInPassedChangeCv}, actual order etag={actualOrderEtag}. " +
            $"This points to filtered delivery Order being polluted with real item lineage. " +
            $"itemCV='{itemChangeVector ?? "<null>"}', originalIncomingCV='{originalIncomingChangeVector ?? "<null>"}'.");
    }

    private static void AssertDatabaseChangeVectorCoversLocalItemOrder(
        ClusterSide filteredPassReceiveSide,
        LabNode node,
        string itemDescription,
        string databaseChangeVector,
        string itemChangeVector,
        string localDatabaseId)
    {
        var localItemOrderEtag = GetOrderEtag(itemChangeVector, localDatabaseId);
        var localDatabaseEtag = GetEtag(databaseChangeVector, localDatabaseId);

        Assert.True(
            localItemOrderEtag > 0,
            $"Expected {itemDescription} on {NodeTag(filteredPassReceiveSide, node)} to have a local Order etag for the node database id. " +
            $"Actual item Order local etag={localItemOrderEtag}, localDatabaseId='{localDatabaseId ?? "<null>"}', itemCV='{itemChangeVector ?? "<null>"}'.");

        Assert.True(
            localDatabaseEtag >= localItemOrderEtag,
            $"Expected {NodeTag(filteredPassReceiveSide, node)} DB CV after {itemDescription} to cover the local item Order etag. " +
            $"Expected DB CV local etag >= item Order local etag, DB CV local etag={localDatabaseEtag}, item Order local etag={localItemOrderEtag}. " +
            $"This points to local item storage advancing the item CV but not advancing the database CV; outgoing internal replication can then fail to advertise or deliver the new local item. " +
            $"dbCV='{databaseChangeVector ?? "<null>"}', itemCV='{itemChangeVector ?? "<null>"}'.");
    }

    private static void AssertDatabaseChangeVectorDoesNotCarryPassedLineage(
        ClusterSide filteredPassReceiveSide,
        LabNode node,
        string itemDescription,
        string databaseChangeVector,
        string itemChangeVector,
        string originalIncomingChangeVector,
        string nodeBDatabaseId,
        long nodeBEtagInPassedChangeCv)
    {
        var actualDatabaseEtag = GetEtag(databaseChangeVector, nodeBDatabaseId);

        Assert.True(
            actualDatabaseEtag < nodeBEtagInPassedChangeCv,
            $"Expected {NodeTag(filteredPassReceiveSide, node)} DB CV after {itemDescription} not to advance {NodeTag(filteredPassReceiveSide, LabNode.B)} from filtered item Version. " +
            $"Expected DB CV etag < {nodeBEtagInPassedChangeCv}, actual DB CV etag={actualDatabaseEtag}. " +
            $"This points to filtered item lineage leaking into regular database change vector progress. " +
            $"dbCV='{databaseChangeVector ?? "<null>"}', itemCV='{itemChangeVector ?? "<null>"}', originalIncomingCV='{originalIncomingChangeVector ?? "<null>"}'.");
    }

    private static void AssertDatabaseChangeVectorDidNotAdvancePastBeforePass(
        ClusterSide filteredPassReceiveSide,
        LabNode node,
        string itemDescription,
        string databaseChangeVectorBeforePass,
        string databaseChangeVectorAfterPass,
        string itemChangeVector,
        string originalIncomingChangeVector,
        string nodeBDatabaseId)
    {
        var beforePassEtag = GetEtag(databaseChangeVectorBeforePass, nodeBDatabaseId);
        var afterPassEtag = GetEtag(databaseChangeVectorAfterPass, nodeBDatabaseId);

        Assert.True(
            afterPassEtag <= beforePassEtag,
            $"Expected {NodeTag(filteredPassReceiveSide, node)} DB CV after {itemDescription} not to advance {NodeTag(filteredPassReceiveSide, LabNode.B)} beyond the value seen before the filtered pass. " +
            $"Expected after-pass etag <= before-pass etag, before-pass etag={beforePassEtag}, after-pass etag={afterPassEtag}. " +
            $"This points to filtered delivery being merged as regular database progress. " +
            $"dbCVBefore='{databaseChangeVectorBeforePass ?? "<null>"}', dbCVAfter='{databaseChangeVectorAfterPass ?? "<null>"}', " +
            $"itemCV='{itemChangeVector ?? "<null>"}', originalIncomingCV='{originalIncomingChangeVector ?? "<null>"}'.");
    }

    private static long GetEtag(string changeVector, string databaseId)
    {
        if (string.IsNullOrEmpty(changeVector))
            return 0;

        foreach (var item in changeVector.ToChangeVectorList())
        {
            if (string.Equals(item.DbId, databaseId, StringComparison.Ordinal))
                return item.Etag;
        }

        return 0;
    }

    private static long GetOrderEtag(string changeVector, string databaseId)
    {
        if (string.IsNullOrEmpty(changeVector))
            return 0;

        var separator = changeVector.IndexOf('|');
        var orderChangeVector = separator < 0
            ? changeVector
            : changeVector.Substring(0, separator);

        return GetEtag(orderChangeVector, databaseId);
    }

    private static long GetVersionEtag(string changeVector, string databaseId)
    {
        if (string.IsNullOrEmpty(changeVector))
            return 0;

        var separator = changeVector.IndexOf('|');
        var versionChangeVector = separator < 0
            ? changeVector
            : changeVector.Substring(separator + 1);

        return GetEtag(versionChangeVector, databaseId);
    }

    private static string GetVersionChangeVector(string changeVector)
    {
        if (string.IsNullOrEmpty(changeVector))
            return changeVector;

        var separator = changeVector.IndexOf('|');
        return separator < 0
            ? changeVector
            : changeVector.Substring(separator + 1);
    }
}
