using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public class FullVsRegularDbCvTests : NonDocumentDbCvProtectionTestBase
{
    private const LineageNode TargetHub = LineageNode.A;
    private const string SyntheticTag = "X";
    private const string SyntheticDbId = "OSKWIRBEDEGoAxbEIiFJeQ";

    public FullVsRegularDbCvTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FilteredClassification_WithoutDivergence_ShouldSinkTagUnknownDbId()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var source = lab.CreateIsolatedStore("full-vs-regular-classification-baseline");
        var docId = "tickets/full-vs-regular-classification-baseline";

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "classification-baseline", CreateSyntheticChangeVector(50)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(lab.WaitForDoc(TargetHub, docId, timeout: 60_000),
            userMessage: $"Expected filtered sink document '{docId}' to arrive on hub {TargetHub}.");

        var snapshot = lab.GetDocumentSnapshot(TargetHub, docId);
        Assert.True(snapshot.Exists, userMessage: $"Expected '{docId}' to exist on hub {TargetHub}.");
        Assert.True(snapshot.Flags.Contain(DocumentFlags.FromFilteredPullReplicationHub),
            userMessage: $"Expected '{docId}' on hub {TargetHub} to stay flagged after filtered pull classification, but flags were '{snapshot.Flags}'.");
        Assert.Contains(
            $"SINK:50-{SyntheticDbId}",
            snapshot.ChangeVector ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            $"{SyntheticTag}:50-{SyntheticDbId}",
            snapshot.ChangeVector ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FilteredClassification_FullOnlyDbId_IsCurrentlyTreatedAsKnownSibling()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var source = lab.CreateIsolatedStore("full-vs-regular-classification-divergent");
        var docId = "tickets/full-vs-regular-classification-divergent";
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        var after = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));
        Assert.DoesNotContain(
            SyntheticDbId,
            after.Regular ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            $"{SyntheticTag}:50-{SyntheticDbId}",
            after.Full ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "classification-divergent", CreateSyntheticChangeVector(51)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(lab.WaitForDoc(TargetHub, docId, timeout: 60_000),
            userMessage: $"Expected filtered sink document '{docId}' to arrive on hub {TargetHub}.");

        var snapshot = lab.GetDocumentSnapshot(TargetHub, docId);
        Assert.True(snapshot.Exists, userMessage: $"Expected '{docId}' to exist on hub {TargetHub}.");
        Assert.Contains(
            $"{SyntheticTag}:51-{SyntheticDbId}",
            snapshot.ChangeVector ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            $"SINK:51-{SyntheticDbId}",
            snapshot.ChangeVector ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FilteredPullHandshake_WithFullOnlyDbId_ShouldStillDeliverSinkDocument()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var source = lab.CreateIsolatedStore("full-vs-regular-filtered-handshake-divergent");
        var docId = "tickets/full-vs-regular-filtered-handshake-divergent";
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "filtered-handshake-divergent", CreateSyntheticChangeVector(50)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(lab.WaitForDoc(TargetHub, docId, timeout: 60_000),
            userMessage: $"Expected filtered sink-to-hub handshake to deliver '{docId}' even when full DB CV knows '{SyntheticDbId}' but regular DB CV does not. " +
            $"If this fails, the destination likely advertised a full-only starting point and the sink skipped a legitimate item before classification even ran.");
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FreshReplicationHandshake_WithoutDivergence_ShouldDeliverSyntheticBacklog()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var source = lab.CreateIsolatedStore("full-vs-regular-handshake-baseline");

        await PutDocumentsWithChangeVectorsAsync(
            source,
            ("tickets/full-vs-regular-handshake-baseline/1", "baseline-1", CreateSyntheticChangeVector(1)),
            ("tickets/full-vs-regular-handshake-baseline/2", "baseline-2", CreateSyntheticChangeVector(2)),
            ("tickets/full-vs-regular-handshake-baseline/3", "baseline-3", CreateSyntheticChangeVector(3)));

        await SetupReplicationAsync(source, lab.StoreFor(TargetHub));

        Assert.True(lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-handshake-baseline/3", "baseline-3", timeout: 60_000),
            userMessage: $"Expected fresh replication handshake without divergence to deliver the synthetic backlog to hub {TargetHub}.");
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task FreshReplicationHandshake_WithFullOnlyDbId_ShouldStillDeliverSyntheticBacklog()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var source = lab.CreateIsolatedStore("full-vs-regular-handshake-divergent");
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        await PutDocumentsWithChangeVectorsAsync(
            source,
            ("tickets/full-vs-regular-handshake-divergent/1", "divergent-1", CreateSyntheticChangeVector(1)),
            ("tickets/full-vs-regular-handshake-divergent/2", "divergent-2", CreateSyntheticChangeVector(2)),
            ("tickets/full-vs-regular-handshake-divergent/3", "divergent-3", CreateSyntheticChangeVector(3)));

        await SetupReplicationAsync(source, lab.StoreFor(TargetHub));

        Assert.True(lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-handshake-divergent/3", "divergent-3", timeout: 60_000),
            userMessage: $"Expected fresh replication handshake to deliver the synthetic backlog even when full DB CV knows '{SyntheticDbId}' but regular DB CV does not. " +
            $"If this fails, the destination likely advertised full-only lineage in its starting-point reply and the source skipped legitimate backlog.");
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task DivergenceAcrossClassificationAndFreshReplication_ShouldNotSkipLegitimateBacklog()
    {
        await using var lab = await CreateLabAsync(new Options());
        using var classificationSource = lab.CreateIsolatedStore("full-vs-regular-e2e-classification");
        using var replicationSource = lab.CreateIsolatedStore("full-vs-regular-e2e-replication");
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        await PutDocumentsWithChangeVectorsAsync(
            classificationSource,
            ("tickets/full-vs-regular-e2e/classification", "classification", CreateSyntheticChangeVector(51)));
        await lab.ConnectSinkToHubAsync(classificationSource, TargetHub);

        Assert.True(lab.WaitForDoc(TargetHub, "tickets/full-vs-regular-e2e/classification", timeout: 60_000),
            userMessage: $"Expected filtered classification probe to reach hub {TargetHub} before the fresh replication handshake.");

        var classificationSnapshot = lab.GetDocumentSnapshot(TargetHub, "tickets/full-vs-regular-e2e/classification");
        Assert.Contains(
            $"{SyntheticTag}:51-{SyntheticDbId}",
            classificationSnapshot.ChangeVector ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);

        var afterClassification = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));
        Assert.DoesNotContain(
            SyntheticDbId,
            afterClassification.Regular ?? string.Empty,
            StringComparison.OrdinalIgnoreCase);

        await PutDocumentsWithChangeVectorsAsync(
            replicationSource,
            ("tickets/full-vs-regular-e2e/backlog/1", "backlog-1", CreateSyntheticChangeVector(1)),
            ("tickets/full-vs-regular-e2e/backlog/2", "backlog-2", CreateSyntheticChangeVector(2)),
            ("tickets/full-vs-regular-e2e/backlog/3", "backlog-3", CreateSyntheticChangeVector(3)));

        await SetupReplicationAsync(replicationSource, lab.StoreFor(TargetHub));

        Assert.True(lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-e2e/backlog/3", "backlog-3", timeout: 60_000),
            userMessage: $"Expected the fresh replication handshake to deliver backlog docs even after filtered classification already observed the synthetic '{SyntheticDbId}' lineage. " +
            $"If this fails, divergence between full and regular DB CV is likely affecting the starting-point reply.");
    }

    private static string CreateSyntheticChangeVector(long etag)
    {
        return $"{SyntheticTag}:{etag}-{SyntheticDbId}";
    }

    private static string AppendEntry(string changeVector, string entry)
    {
        return string.IsNullOrWhiteSpace(changeVector) ? entry : $"{changeVector},{entry}";
    }

    private static (string Regular, string Full) ReadDatabaseChangeVectors(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return (DocumentsStorage.GetDatabaseChangeVector(context).AsString(), DocumentsStorage.GetFullDatabaseChangeVector(context));
        }
    }

    private static void SetDivergentDatabaseChangeVectors(DocumentDatabase database, string regularChangeVector, string fullChangeVector)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            database.DocumentsStorage.SetDatabaseChangeVector(context, context.GetChangeVector(regularChangeVector));
            database.DocumentsStorage.SetFullDatabaseChangeVector(context, fullChangeVector);
            tx.Commit();
        }
    }

    private async Task PutDocumentsWithChangeVectorsAsync(IDocumentStore store, params (string Id, string Name, string ChangeVector)[] docs)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            foreach (var doc in docs)
            {
                var document = context.ReadObject(
                    new DynamicJsonValue
                    {
                        ["Name"] = doc.Name,
                        ["@metadata"] = new DynamicJsonValue
                        {
                            ["@collection"] = "Users"
                        }
                    },
                    doc.Id);

                database.DocumentsStorage.Put(
                    context,
                    doc.Id,
                    expectedChangeVector: null,
                    document,
                    changeVector: doc.ChangeVector);
            }

            tx.Commit();
        }
    }
}
