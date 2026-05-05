using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class FilteredPullBacklogDeliveryTests : NonDocumentDbCvProtectionTestBase
{
    private const LineageNode TargetHub = LineageNode.A;
    private const string SyntheticTag = "X";
    private const string SyntheticDbId = "OSKWIRBEDEGoAxbEIiFJeQ";
    private const int RedBarReplicationTimeoutMs = 25_000;

    public FilteredPullBacklogDeliveryTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredClassification_WithoutDivergence_ShouldStoreIncomingDocument(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore("full-vs-regular-classification-baseline");
        var docId = "tickets/full-vs-regular-classification-baseline";

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "classification-baseline", CreateSyntheticChangeVector(50)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(
            lab.WaitForDoc(TargetHub, docId, timeout: 60_000),
            $"Expected filtered sink document '{docId}' to arrive on hub {TargetHub}.");

        var snapshot = lab.GetDocumentSnapshot(TargetHub, docId);
        Assert.True(snapshot.Exists, $"Expected '{docId}' to exist on hub {TargetHub}.");
        Assert.Equal("classification-baseline", snapshot.Name);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredClassification_WhenDestinationWasPreconditionedWithSameSource_ShouldStillStoreIncomingDocument(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore("full-vs-regular-classification-divergent");
        var docId = "tickets/full-vs-regular-classification-divergent";
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "classification-divergent", CreateSyntheticChangeVector(51)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(
            lab.WaitForDoc(TargetHub, docId, timeout: 60_000),
            $"Expected filtered sink document '{docId}' to arrive on hub {TargetHub}.");

        var snapshot = lab.GetDocumentSnapshot(TargetHub, docId);
        Assert.True(snapshot.Exists, $"Expected '{docId}' to exist on hub {TargetHub}.");
        Assert.Equal("classification-divergent", snapshot.Name);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredPullHandshake_WhenDestinationWasPreconditionedWithSameSource_ShouldStillDeliverSinkDocument(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore("full-vs-regular-filtered-handshake-divergent");
        var docId = "tickets/full-vs-regular-filtered-handshake-divergent";
        var before = ReadDatabaseChangeVectors(lab.DatabaseFor(TargetHub));

        SetDivergentDatabaseChangeVectors(
            lab.DatabaseFor(TargetHub),
            regularChangeVector: before.Regular,
            fullChangeVector: AppendEntry(before.Regular, CreateSyntheticChangeVector(50)));

        await PutDocumentsWithChangeVectorsAsync(source, (docId, "filtered-handshake-divergent", CreateSyntheticChangeVector(50)));
        await lab.ConnectSinkToHubAsync(source, TargetHub);

        Assert.True(
            lab.WaitForDoc(TargetHub, docId, timeout: RedBarReplicationTimeoutMs),
            $"Expected filtered sink-to-hub replication to deliver '{docId}' after the destination had been preconditioned with the same source identity. " +
            $"If this fails, a legitimate sink item was skipped before it reached the hub.");
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FreshReplicationHandshake_WithoutDivergence_ShouldDeliverSyntheticBacklog(Options options)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore("full-vs-regular-handshake-baseline");

        await PutDocumentsWithChangeVectorsAsync(
            source,
            ("tickets/full-vs-regular-handshake-baseline/1", "baseline-1", CreateSyntheticChangeVector(1)),
            ("tickets/full-vs-regular-handshake-baseline/2", "baseline-2", CreateSyntheticChangeVector(2)),
            ("tickets/full-vs-regular-handshake-baseline/3", "baseline-3", CreateSyntheticChangeVector(3)));

        await SetupReplicationAsync(source, lab.StoreFor(TargetHub));

        Assert.True(
            lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-handshake-baseline/3", "baseline-3", timeout: 60_000),
            $"Expected fresh replication handshake without divergence to deliver the synthetic backlog to hub {TargetHub}.");
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FreshReplicationHandshake_WhenDestinationWasPreconditionedWithSameSource_ShouldStillDeliverBacklog(Options options)
    {
        await using var lab = await CreateLabAsync(options);
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

        Assert.True(
            lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-handshake-divergent/3", "divergent-3", timeout: RedBarReplicationTimeoutMs),
            $"Expected fresh replication to deliver all backlog documents after the destination had been preconditioned with the same source identity. " +
            $"If this fails, legitimate backlog was skipped during replication startup.");
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredClassificationFollowedByFreshReplication_ShouldNotSkipLegitimateBacklog(Options options)
    {
        await using var lab = await CreateLabAsync(options);
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

        Assert.True(
            lab.WaitForDoc(TargetHub, "tickets/full-vs-regular-e2e/classification", timeout: 60_000),
            $"Expected filtered classification probe to reach hub {TargetHub} before the fresh replication handshake.");

        var classificationSnapshot = lab.GetDocumentSnapshot(TargetHub, "tickets/full-vs-regular-e2e/classification");
        Assert.True(classificationSnapshot.Exists, $"Expected classification probe document on hub {TargetHub}.");
        Assert.Equal("classification", classificationSnapshot.Name);

        await PutDocumentsWithChangeVectorsAsync(
            replicationSource,
            ("tickets/full-vs-regular-e2e/backlog/1", "backlog-1", CreateSyntheticChangeVector(1)),
            ("tickets/full-vs-regular-e2e/backlog/2", "backlog-2", CreateSyntheticChangeVector(2)),
            ("tickets/full-vs-regular-e2e/backlog/3", "backlog-3", CreateSyntheticChangeVector(3)));

        await SetupReplicationAsync(replicationSource, lab.StoreFor(TargetHub));

        Assert.True(
            lab.WaitForDocumentName(TargetHub, "tickets/full-vs-regular-e2e/backlog/3", "backlog-3", timeout: RedBarReplicationTimeoutMs),
            $"Expected fresh replication to deliver backlog docs even after a prior filtered sink-to-hub item from the same source identity reached the hub. " +
            $"If this fails, legitimate backlog was skipped after the filtered-pull path ran first.");
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
            return (
                DocumentsStorage.GetDatabaseChangeVector(context).AsString(),
                DocumentsStorage.GetFullDatabaseChangeVector(context));
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
