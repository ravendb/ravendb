using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Replication;
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
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);

        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        Assert.True(nodeADocument.Exists, $"Expected passed document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");

        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);
        Assert.True(nodeBEtagInNodeADbCvAfterPass < nodeBEtagInNodeADbCvBeforePass && nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"before='{nodeADbCvBeforePass}', after='{nodeADbCvAfterPass}', originalIncomingCV='{originalIncomingChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"storedCV='{nodeADocument.ChangeVector}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeBDocument = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);

        Assert.True(
            nodeCDocument.Exists && string.Equals(nodeCDocument.Name, itemName, StringComparison.Ordinal),
            $"Expected filtered pass document '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.B)}: exists={nodeBDocument.Exists}, name='{nodeBDocument.Name ?? "<null>"}', CV='{nodeBDocument.ChangeVector ?? "<null>"}'. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.A)}: exists={nodeADocument.Exists}, name='{nodeADocument.Name ?? "<null>"}', CV='{nodeADocument.ChangeVector ?? "<null>"}'. " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.C)}: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"originalIncomingCV='{originalIncomingChangeVector}'.");
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Hub])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [ClusterSide.Sink])]
    public async Task ConflictDocument_ShouldNotInflateDatabaseChangeVectorAndShouldReplicateThroughInternalReplication(Options options, ClusterSide filteredPassReceiveSide)
    {
        const string itemName = "conflict-document";

        await using var lab = await CreateDualClusterLabAsync(options, filteredPassReceiveSide, itemName);
        var initialName = itemName + "-initial";
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
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected conflict on node B for '{lab.FilteredRoundTripTicketId}' on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBConflicts='{FormatConflicts(nodeBConflicts)}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedConflicts(expectedCount: 2);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeAConflicts = lab.GetFilteredRoundTripConflicts(LabNode.A);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeAConflicts.Count >= 2, $"Expected passed conflict document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered conflict pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', originalIncomingCV='{originalIncomingChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeBConflicts='{FormatConflicts(nodeBConflicts)}', nodeAConflicts='{FormatConflicts(nodeAConflicts)}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCConflicts = lab.GetFilteredRoundTripConflicts(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCConflicts.Count >= 2,
            $"Expected filtered conflict pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} and " +
            $"{NodeTag(filteredPassReceiveSide, LabNode.C)}->{NodeTag(filteredPassReceiveSide, LabNode.A)} are blocked. " +
            $"nodeBConflicts='{FormatConflicts(nodeBConflicts)}'. nodeAConflicts='{FormatConflicts(nodeAConflicts)}'. " +
            $"nodeCConflicts='{FormatConflicts(nodeCConflicts)}'. originalIncomingCV='{originalIncomingChangeVector}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered conflict next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', originalIncomingCV='{originalIncomingChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCConflicts='{FormatConflicts(nodeCConflicts)}'.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBTombstone.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBDocumentAfterDelete.Exists == false && nodeBTombstone.Exists,
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeADocument.Exists == false && nodeATombstone.Exists,
            $"Expected document tombstone '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)} and remove the live document. " +
            $"nodeADocumentExists={nodeADocument.Exists}, nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', " +
            $"nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered document tombstone pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentExists={nodeADocument.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCTombstone = lab.GetFilteredRoundTripDocumentTombstone(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCDocument.Exists == false && nodeCTombstone.Exists,
            $"Expected filtered document tombstone '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstone: exists={nodeCTombstone.Exists}, CV='{nodeCTombstone.ChangeVector ?? "<null>"}'. " +
            $"nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered document tombstone next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentExists={nodeCDocument.Exists}, nodeCTombstoneExists={nodeCTombstone.Exists}, nodeCTombstoneCV='{nodeCTombstone.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBRevision.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBRevision.Exists && string.Equals(nodeBRevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected revision on node B for '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBRevisionExists={nodeBRevision.Exists}, nodeBRevisionName='{nodeBRevision.Name ?? "<null>"}', " +
            $"nodeBRevisionCV='{nodeBRevision.ChangeVector ?? "<null>"}', nodeBRevisionCount={nodeBRevision.Count}, " +
            $"nodeBDocumentName='{nodeBDocumentAfterRevision.Name ?? "<null>"}', nodeBDocumentCV='{nodeBDocumentAfterRevision.ChangeVector ?? "<null>"}', " +
            $"initialRevisionCV='{initialRevision.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected revision owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeARevision.Exists && string.Equals(nodeARevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected revision for '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeARevisionExists={nodeARevision.Exists}, nodeARevisionName='{nodeARevision.Name ?? "<null>"}', nodeARevisionCV='{nodeARevision.ChangeVector ?? "<null>"}', " +
            $"nodeBRevisionCV='{nodeBRevision.ChangeVector}', nodeARevisionCount={nodeARevision.Count}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered revision document pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBRevisionCV='{nodeBRevision.ChangeVector}', " +
            $"nodeBDocumentAfterRevisionCV='{nodeBDocumentAfterRevision.ChangeVector}', nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, " +
            $"nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', " +
            $"nodeARevisionCV='{nodeARevision.ChangeVector ?? "<null>"}', nodeARevisionCount={nodeARevision.Count}.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCRevision = lab.GetFilteredRoundTripLatestRevision(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCDocument.Exists && nodeCRevision.Exists && string.Equals(nodeCRevision.Name, revisionName, StringComparison.Ordinal),
            $"Expected filtered revision document '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCRevision: exists={nodeCRevision.Exists}, CV='{nodeCRevision.ChangeVector ?? "<null>"}', count={nodeCRevision.Count}. " +
            $"nodeARevisionCV='{nodeARevision.ChangeVector ?? "<null>"}', nodeBRevisionCV='{nodeBRevision.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered revision document next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBRevisionCV='{nodeBRevision.ChangeVector}', " +
            $"nodeBDocumentAfterRevisionCV='{nodeBDocumentAfterRevision.ChangeVector}', nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, " +
            $"nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', " +
            $"nodeCRevisionExists={nodeCRevision.Exists}, nodeCRevisionCV='{nodeCRevision.ChangeVector ?? "<null>"}', nodeCRevisionCount={nodeCRevision.Count}.");
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
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        await lab.DeleteFilteredRoundTripRevisionAsync(LabNode.B, revisionToDelete.ChangeVector);

        var nodeBDocumentAfterDeleteRevision = lab.GetFilteredRoundTripDocument(LabNode.B);
        var nodeBTombstones = lab.GetFilteredRoundTripRevisionTombstones(LabNode.B);
        var nodeBTombstone = nodeBTombstones.OrderByDescending(x => x.Etag).FirstOrDefault();
        var nodeBEtagInPassedChangeCv = nodeBTombstone == null ? 0 : GetEtag(nodeBTombstone.ChangeVector, nodeBDatabaseId);

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
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected revision tombstone owner document '{lab.FilteredRoundTripTicketId}' to remain on {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeATombstones.Count > 0,
            $"Expected revision tombstone for '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeATombstones='{FormatRevisionTombstones(nodeATombstones)}', nodeBTombstones='{FormatRevisionTombstones(nodeBTombstones)}'.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered revision tombstone pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', revisionToDeleteCV='{revisionToDelete.ChangeVector}', " +
            $"nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', nodeBTombstoneKeyCV='{nodeBTombstone.KeyChangeVector}', " +
            $"nodeBDocumentAfterDeleteRevisionCV='{nodeBDocumentAfterDeleteRevision.ChangeVector}', nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, " +
            $"nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', " +
            $"nodeATombstones='{FormatRevisionTombstones(nodeATombstones)}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCTombstones = lab.GetFilteredRoundTripRevisionTombstones(LabNode.C);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCTombstones.Count > 0,
            $"Expected filtered revision tombstone '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstones='{FormatRevisionTombstones(nodeCTombstones)}', nodeATombstones='{FormatRevisionTombstones(nodeATombstones)}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered revision tombstone next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', revisionToDeleteCV='{revisionToDelete.ChangeVector}', " +
            $"nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', nodeBTombstoneKeyCV='{nodeBTombstone.KeyChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', nodeCTombstones='{FormatRevisionTombstones(nodeCTombstones)}'.");
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
        var nodeBDbCvAfterCounter = lab.GetDatabaseChangeVector(LabNode.B);
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBDbCvAfterCounter, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBEtagInPassedChangeCv > nodeBEtagInNodeADbCvBeforePass,
            $"Expected counter change on {NodeTag(filteredPassReceiveSide, LabNode.B)} to carry a newer etag than {NodeTag(filteredPassReceiveSide, LabNode.A)} had before the filtered pass. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}.");

        lab.ExpectPassedCounter(counterName, expectedCounterValue);
        await lab.PassThroughFilteredReplicationAsync();

        var nodeADocument = lab.GetFilteredRoundTripDocument(LabNode.A);
        var entryCounter = lab.GetFilteredRoundTripCounter(LabNode.A, counterName);
        var nodeADbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected counter owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(entryCounter.Exists && entryCounter.Value == expectedCounterValue, $"Expected counter '{counterName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered counter pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', entryCounterExists={entryCounter.Exists}, entryCounterValue={entryCounter.Value}.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var observerCounter = lab.GetFilteredRoundTripCounter(LabNode.C, counterName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            observerCounter.Exists && observerCounter.Value == expectedCounterValue,
            $"Expected filtered counter pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"observerCounter: exists={observerCounter.Exists}, value={observerCounter.Value}. " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered counter next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBDbCvAfterCounter='{nodeBDbCvAfterCounter}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', observerCounterExists={observerCounter.Exists}, observerCounterValue={observerCounter.Value}.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBAttachment.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBAttachment.Exists && nodeBAttachment.Size == content.Length,
            $"Expected attachment on node B '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBAttachmentExists={nodeBAttachment.Exists}, nodeBAttachmentSize={nodeBAttachment.Size}, nodeBAttachmentCV='{nodeBAttachment.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected attachment owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeAAttachment.Exists &&
            string.Equals(nodeAAttachment.Hash, nodeBAttachment.Hash, StringComparison.Ordinal) &&
            nodeAAttachment.Size == content.Length,
            $"Expected attachment '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}. " +
            $"nodeAAttachmentExists={nodeAAttachment.Exists}, nodeAAttachmentHash='{nodeAAttachment.Hash ?? "<null>"}', nodeAAttachmentSize={nodeAAttachment.Size}, nodeBAttachmentHash='{nodeBAttachment.Hash}'.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered attachment pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBAttachmentCV='{nodeBAttachment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeAAttachmentCV='{nodeAAttachment.ChangeVector ?? "<null>"}', " +
            $"nodeAAttachmentHash='{nodeAAttachment.Hash ?? "<null>"}', nodeAAttachmentSize={nodeAAttachment.Size}.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCAttachment = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCAttachment.Exists &&
            string.Equals(nodeCAttachment.Hash, nodeBAttachment.Hash, StringComparison.Ordinal) &&
            nodeCAttachment.Size == content.Length,
            $"Expected filtered attachment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCAttachment: exists={nodeCAttachment.Exists}, hash='{nodeCAttachment.Hash ?? "<null>"}', size={nodeCAttachment.Size}, CV='{nodeCAttachment.ChangeVector ?? "<null>"}'. " +
            $"nodeAAttachmentCV='{nodeAAttachment.ChangeVector ?? "<null>"}', nodeBAttachmentCV='{nodeBAttachment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered attachment next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBAttachmentCV='{nodeBAttachment.ChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', nodeCAttachmentExists={nodeCAttachment.Exists}, " +
            $"nodeCAttachmentCV='{nodeCAttachment.ChangeVector ?? "<null>"}', nodeCAttachmentHash='{nodeCAttachment.Hash ?? "<null>"}', nodeCAttachmentSize={nodeCAttachment.Size}.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBTombstone.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBAttachmentBeforeDelete.Exists && nodeBAttachmentBeforeDelete.Size == content.Length,
            $"Expected attachment on node B '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before deleting it. " +
            $"nodeBAttachmentExists={nodeBAttachmentBeforeDelete.Exists}, nodeBAttachmentSize={nodeBAttachmentBeforeDelete.Size}, nodeBAttachmentCV='{nodeBAttachmentBeforeDelete.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected attachment tombstone owner document '{lab.FilteredRoundTripTicketId}' to remain on {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeAAttachment.Exists == false && nodeATombstone.Exists,
            $"Expected attachment tombstone '{attachmentName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)} and remove the live attachment. " +
            $"nodeAAttachmentExists={nodeAAttachment.Exists}, nodeATombstoneExists={nodeATombstone.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered attachment tombstone pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBDocumentAfterDeleteCV='{nodeBDocumentAfterDelete.ChangeVector}', nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, " +
            $"nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', " +
            $"nodeAAttachmentExists={nodeAAttachment.Exists}, nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCAttachment = lab.GetFilteredRoundTripAttachment(LabNode.C, attachmentName);
        var nodeCTombstone = lab.GetFilteredRoundTripAttachmentTombstone(LabNode.C, attachmentName, nodeBAttachmentBeforeDelete.Hash, nodeBAttachmentBeforeDelete.ContentType);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCAttachment.Exists == false && nodeCTombstone.Exists,
            $"Expected filtered attachment tombstone pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCAttachment: exists={nodeCAttachment.Exists}, CV='{nodeCAttachment.ChangeVector ?? "<null>"}'. " +
            $"nodeCTombstone: exists={nodeCTombstone.Exists}, CV='{nodeCTombstone.ChangeVector ?? "<null>"}'. " +
            $"nodeATombstoneCV='{nodeATombstone.ChangeVector ?? "<null>"}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered attachment tombstone next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBTombstoneCV='{nodeBTombstone.ChangeVector}', " +
            $"nodeBDocumentAfterDeleteCV='{nodeBDocumentAfterDelete.ChangeVector}', nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, " +
            $"nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', " +
            $"nodeCAttachmentExists={nodeCAttachment.Exists}, nodeCTombstoneExists={nodeCTombstone.Exists}, nodeCTombstoneCV='{nodeCTombstone.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBSegment.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBSegment.Exists && nodeBSegment.ValueCount >= expectedValueCount,
            $"Expected time series segment on node B '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBSegmentExists={nodeBSegment.Exists}, nodeBSegmentValueCount={nodeBSegment.ValueCount}, nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected time series owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(nodeASegment.Exists && nodeASegment.ValueCount >= expectedValueCount, $"Expected time series '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered time series segment pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeASegmentValueCount={nodeASegment.ValueCount}.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCSegment.Exists && nodeCSegment.ValueCount >= expectedValueCount,
            $"Expected filtered time series segment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCSegment: exists={nodeCSegment.Exists}, valueCount={nodeCSegment.ValueCount}, CV='{nodeCSegment.ChangeVector ?? "<null>"}'. " +
            $"nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered time series segment next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', nodeCSegmentExists={nodeCSegment.Exists}, nodeCSegmentValueCount={nodeCSegment.ValueCount}, " +
            $"nodeCSegmentCV='{nodeCSegment.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBSegment.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

        Assert.True(
            nodeBSegment.Exists && nodeBSegment.ValueCount >= expectedValueCount,
            $"Expected time series segment on node B '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' at {NodeTag(filteredPassReceiveSide, LabNode.B)} before filtered pass. " +
            $"nodeBSegmentExists={nodeBSegment.Exists}, nodeBSegmentValueCount={nodeBSegment.ValueCount}, nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected time series owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(nodeASegment.Exists && nodeASegment.ValueCount >= expectedValueCount, $"Expected time series '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered time series segment pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeASegmentValueCount={nodeASegment.ValueCount}.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCSegment = lab.GetFilteredRoundTripTimeSeriesSegment(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCSegment.Exists && nodeCSegment.ValueCount >= expectedValueCount,
            $"Expected filtered time series segment pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCSegment: exists={nodeCSegment.Exists}, valueCount={nodeCSegment.ValueCount}, CV='{nodeCSegment.ChangeVector ?? "<null>"}'. " +
            $"nodeASegmentCV='{nodeASegment.ChangeVector ?? "<null>"}', nodeBSegmentCV='{nodeBSegment.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered time series segment next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBSegmentCV='{nodeBSegment.ChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', nodeCSegmentExists={nodeCSegment.Exists}, nodeCSegmentValueCount={nodeCSegment.ValueCount}, " +
            $"nodeCSegmentCV='{nodeCSegment.ChangeVector ?? "<null>"}'.");
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
        var nodeBEtagInPassedChangeCv = GetEtag(nodeBDeletedRange.ChangeVector, nodeBDatabaseId);

        var nodeADbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.A);
        var nodeBEtagInNodeADbCvBeforePass = GetEtag(nodeADbCvBeforePass, nodeBDatabaseId);
        var nodeCDbCvBeforePass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvBeforePass = GetEtag(nodeCDbCvBeforePass, nodeBDatabaseId);

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
        var nodeBEtagInNodeADbCvAfterPass = GetEtag(nodeADbCvAfterPass, nodeBDatabaseId);

        Assert.True(nodeADocument.Exists, $"Expected deleted range owner document '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(nodeADeletedRange.Exists, $"Expected deleted time series range '{timeSeriesName}' on '{lab.FilteredRoundTripTicketId}' to reach {NodeTag(filteredPassReceiveSide, LabNode.A)}.");
        Assert.True(
            nodeBEtagInNodeADbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered deleted time series range pass into {NodeTag(filteredPassReceiveSide, LabNode.A)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeADbCvBefore='{nodeADbCvBeforePass}', nodeADbCvAfter='{nodeADbCvAfterPass}', nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector}', " +
            $"nodeBEtagInNodeADbCvBeforePass={nodeBEtagInNodeADbCvBeforePass}, nodeBEtagInNodeADbCvAfterPass={nodeBEtagInNodeADbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeADocumentCV='{nodeADocument.ChangeVector ?? "<null>"}', nodeADeletedRangeCV='{nodeADeletedRange.ChangeVector ?? "<null>"}', " +
            $"nodeADeletedRangeFrom='{nodeADeletedRange.From:O}', nodeADeletedRangeTo='{nodeADeletedRange.To:O}'.");

        await lab.WaitForExpectedFilteredRoundTripItemAsync(LabNode.C);

        var nodeCDocument = lab.GetFilteredRoundTripDocument(LabNode.C);
        var nodeCDeletedRange = lab.GetFilteredRoundTripTimeSeriesDeletedRange(LabNode.C, timeSeriesName);
        var nodeCDbCvAfterPass = lab.GetDatabaseChangeVector(LabNode.C);
        var nodeBEtagInNodeCDbCvAfterPass = GetEtag(nodeCDbCvAfterPass, nodeBDatabaseId);

        Assert.True(
            nodeCDeletedRange.Exists,
            $"Expected filtered deleted time series range pass on '{lab.FilteredRoundTripTicketId}' to propagate from {NodeTag(filteredPassReceiveSide, LabNode.A)} to {NodeTag(filteredPassReceiveSide, LabNode.C)} " +
            $"through ordinary internal replication while direct {NodeTag(filteredPassReceiveSide, LabNode.B)}->{NodeTag(filteredPassReceiveSide, LabNode.C)} is blocked. " +
            $"nodeCDocument: exists={nodeCDocument.Exists}, name='{nodeCDocument.Name ?? "<null>"}', CV='{nodeCDocument.ChangeVector ?? "<null>"}'. " +
            $"nodeCDeletedRange: exists={nodeCDeletedRange.Exists}, CV='{nodeCDeletedRange.ChangeVector ?? "<null>"}'. " +
            $"nodeADeletedRangeCV='{nodeADeletedRange.ChangeVector ?? "<null>"}', nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector ?? "<null>"}'.");
        Assert.True(
            nodeBEtagInNodeCDbCvAfterPass < nodeBEtagInPassedChangeCv,
            $"Expected filtered deleted time series range next hop into {NodeTag(filteredPassReceiveSide, LabNode.C)} not to advance the {NodeTag(filteredPassReceiveSide, LabNode.B)} component in DB CV. " +
            $"nodeCDbCvBefore='{nodeCDbCvBeforePass}', nodeCDbCvAfter='{nodeCDbCvAfterPass}', nodeBDeletedRangeCV='{nodeBDeletedRange.ChangeVector}', " +
            $"nodeBEtagInNodeCDbCvBeforePass={nodeBEtagInNodeCDbCvBeforePass}, nodeBEtagInNodeCDbCvAfterPass={nodeBEtagInNodeCDbCvAfterPass}, nodeBEtagInPassedChangeCv={nodeBEtagInPassedChangeCv}, " +
            $"nodeCDocumentCV='{nodeCDocument.ChangeVector ?? "<null>"}', nodeCDeletedRangeExists={nodeCDeletedRange.Exists}, " +
            $"nodeCDeletedRangeCV='{nodeCDeletedRange.ChangeVector ?? "<null>"}'.");
    }

    private static long GetEtag(string changeVector, string databaseId)
    {
        foreach (var item in changeVector.ToChangeVectorList())
        {
            if (string.Equals(item.DbId, databaseId, StringComparison.Ordinal))
                return item.Etag;
        }

        return 0;
    }
}
