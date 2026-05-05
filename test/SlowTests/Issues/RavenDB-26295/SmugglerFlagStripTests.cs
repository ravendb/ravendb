using System;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class SmugglerFlagStripTests : TombstoneLineagePreservationTestBase
{
    private const string SubjectDocId = "tickets/smuggler-lineage";
    private const string SubjectCollection = "Users";
    private const string SubjectName = "Bob";
    private const string ImportedChangeVector = "A:1-OSKWIRBEDEGoAxbEIiFJeQ";
    private const int FilteredPullHandshakeTimeoutMs = 25_000;

    public SmugglerFlagStripTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task DirectImport_ShouldImportDocumentOrTombstone(Options options, bool importTombstone)
    {
        using var target = GetDocumentStore(options);
        var operateOnTypes = importTombstone ? DatabaseItemType.Tombstones : DatabaseItemType.Documents;
        await using var dump = importTombstone
            ? await CreateDirectTombstoneExportDumpAsync(options)
            : CreateDocumentDump();

        await ImportDumpAsync(target, dump, operateOnTypes);

        if (importTombstone)
        {
            await AssertTombstoneExistsAsync(target, SubjectDocId);
            return;
        }

        await AssertDocumentNameAsync(target, SubjectDocId, SubjectName);
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ExportImport_FromReplicatedSource_ShouldImportDocumentOrTombstone(Options options, bool exportTombstone)
    {
        await using var lab = await CreateLabAsync(options);
        using var blockCToA = lab.BlockLink(LineageNode.C, LineageNode.A);
        using var blockCToB = lab.BlockLink(LineageNode.C, LineageNode.B);

        await lab.WriteAndInjectTicketAsync(SubjectDocId, LineageNode.C, LineageNode.A);

        Assert.True(
            lab.WaitForDoc(LineageNode.A, SubjectDocId, timeout: 60_000),
            $"Expected source item '{SubjectDocId}' to arrive on hub A before export.");

        var sourceDocument = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
        Assert.True(sourceDocument.Exists, $"Expected source document '{SubjectDocId}' on hub A before export.");

        var expectedImportedName = sourceDocument.Name;
        if (exportTombstone)
        {
            using (var session = lab.StoreFor(LineageNode.A).OpenAsyncSession())
            {
                session.Delete(SubjectDocId);
                await session.SaveChangesAsync();
            }

            Assert.True(
                WaitForValue(
                    () => lab.GetDocumentTombstoneSnapshot(LineageNode.A, SubjectDocId).Exists,
                    expectedVal: true,
                    timeout: 60_000),
                $"Expected tombstone for '{SubjectDocId}' on hub A before export.");
        }

        using var target = lab.CreateIsolatedStore($"{SubjectDocId.Replace('/', '-')}-import-{Guid.NewGuid():N}");
        var operateOnTypes = exportTombstone ? DatabaseItemType.Tombstones : DatabaseItemType.Documents;
        await using var dump = await ExportDumpAsync(lab.StoreFor(LineageNode.A), operateOnTypes);
        await ImportDumpAsync(target, dump, operateOnTypes);

        if (exportTombstone)
        {
            await AssertTombstoneExistsAsync(target, SubjectDocId);
            return;
        }

        await AssertDocumentNameAsync(target, SubjectDocId, expectedImportedName);
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ImportThenFilteredPullHandshake_ShouldStillDeliverImportedItem(Options options, bool importTombstone)
    {
        await using var lab = await CreateLabAsync(options);
        using var source = lab.CreateIsolatedStore($"filtered-pull-smuggler-{(importTombstone ? "tombstone" : "document")}");

        var operateOnTypes = importTombstone ? DatabaseItemType.Tombstones : DatabaseItemType.Documents;
        await using var dump = importTombstone
            ? await CreateDirectTombstoneExportDumpAsync(lab)
            : CreateDocumentDump();

        await ImportDumpAsync(source, dump, operateOnTypes);
        await lab.PreconditionFullDatabaseChangeVectorFromStoreAsync(source, LineageNode.A);

        await lab.ConnectSinkToHubAsync(source, LineageNode.A);

        if (importTombstone)
        {
            var delivered = WaitForValue(
                () => lab.GetDocumentTombstoneSnapshot(LineageNode.A, SubjectDocId).Exists,
                expectedVal: true,
                timeout: FilteredPullHandshakeTimeoutMs);

            Assert.True(
                delivered,
                $"Expected filtered sink-to-hub replication to deliver smuggler-imported tombstone '{SubjectDocId}' after the hub had been preconditioned with the same source identity. " +
                "If this fails, a legitimate smuggler-imported tombstone was skipped before it reached the hub.");
            return;
        }

        Assert.True(
            lab.WaitForDocumentName(LineageNode.A, SubjectDocId, SubjectName, timeout: FilteredPullHandshakeTimeoutMs),
            $"Expected filtered sink-to-hub replication to deliver smuggler-imported document '{SubjectDocId}' after the hub had been preconditioned with the same source identity. " +
            "If this fails, a legitimate smuggler-imported document was skipped before it reached the hub.");
    }

    private async Task<MemoryStream> CreateDirectTombstoneExportDumpAsync(LineageLab lab)
    {
        using var source = lab.CreateIsolatedStore($"smuggler-tombstone-dump-{Guid.NewGuid():N}");
        await CreateDocumentTombstoneAsync(source);
        return await ExportDumpAsync(source, DatabaseItemType.Tombstones);
    }

    private static async Task ImportDumpAsync(IDocumentStore target, Stream dump, DatabaseItemType operateOnTypes)
    {
        dump.Position = 0;
        var import = await target.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
        {
            OperateOnTypes = operateOnTypes
        }, dump);
        await import.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
    }

    private static async Task<MemoryStream> ExportDumpAsync(IDocumentStore source, DatabaseItemType operateOnTypes)
    {
        var stream = new MemoryStream();
        var export = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
        {
            OperateOnTypes = operateOnTypes
        }, stream);
        await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        stream.Position = 0;
        return stream;
    }

    private static MemoryStream CreateDocumentDump()
    {
        var metadata = new JsonObject
        {
            ["@collection"] = SubjectCollection,
            ["@change-vector"] = ImportedChangeVector,
            ["@id"] = SubjectDocId,
            ["@last-modified"] = "2026-04-10T00:00:00.0000000Z"
        };

        var root = CreateBaseDumpRoot();
        root["Docs"] = new JsonArray
        {
            new JsonObject
            {
                ["Name"] = SubjectName,
                ["@metadata"] = metadata
            }
        };

        return new MemoryStream(Encoding.UTF8.GetBytes(root.ToJsonString()));
    }

    private async Task<MemoryStream> CreateDirectTombstoneExportDumpAsync(Options options)
    {
        using var source = GetDocumentStore(options);
        await CreateDocumentTombstoneAsync(source);
        return await ExportDumpAsync(source, DatabaseItemType.Tombstones);
    }

    private static JsonObject CreateBaseDumpRoot()
    {
        return new JsonObject
        {
            ["BuildVersion"] = 60,
            ["DatabaseRecord"] = new JsonObject
            {
                ["DatabaseName"] = "smuggler-lineage",
                ["Encrypted"] = false,
                ["SupportedFeatures"] = new JsonArray(),
                ["UnusedDatabaseIds"] = new JsonArray(),
                ["LockMode"] = "Unlock",
                ["ConflictSolverConfig"] = null,
                ["Settings"] = new JsonArray(),
                ["Revisions"] = null,
                ["TimeSeries"] = new JsonObject(),
                ["DocumentsCompression"] = null,
                ["Expiration"] = null,
                ["Refresh"] = null,
                ["Client"] = null,
                ["Sorters"] = new JsonObject(),
                ["Analyzers"] = new JsonObject(),
                ["RavenConnectionStrings"] = new JsonObject(),
                ["SqlConnectionStrings"] = new JsonObject(),
                ["PeriodicBackups"] = new JsonArray(),
                ["ExternalReplications"] = new JsonArray(),
                ["RavenEtls"] = new JsonArray(),
                ["SqlEtls"] = new JsonArray(),
                ["HubPullReplications"] = new JsonArray(),
                ["SinkPullReplications"] = new JsonArray(),
                ["OlapConnectionStrings"] = new JsonObject(),
                ["OlapEtls"] = new JsonArray(),
                ["ElasticSearchConnectionStrings"] = new JsonObject(),
                ["ElasticSearchEtls"] = new JsonArray(),
                ["QueueConnectionStrings"] = new JsonObject(),
                ["QueueEtls"] = new JsonArray()
            }
        };
    }

    private Task AssertDocumentNameAsync(IDocumentStore store, string id, string expectedName)
    {
        User document = null;
        var replicated = WaitForValue(
            () =>
            {
                using var session = store.OpenSession();
                document = session.Load<User>(id);
                return document != null && string.Equals(document.Name, expectedName, StringComparison.Ordinal);
            },
            expectedVal: true,
            timeout: 60_000);

        Assert.True(
            replicated,
            $"Expected document '{id}' with name '{expectedName}' to exist after smuggler import.");
        return Task.CompletedTask;
    }

    private async Task AssertTombstoneExistsAsync(IDocumentStore store, string id)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var exists = WaitForValue(
            () =>
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var documentOrTombstone = database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
                    try
                    {
                        return documentOrTombstone.Tombstone != null;
                    }
                    finally
                    {
                        documentOrTombstone.Document?.Dispose();
                        documentOrTombstone.Tombstone?.Dispose();
                    }
                }
            },
            expectedVal: true,
            timeout: 60_000);

        Assert.True(exists, $"Expected tombstone '{id}' to exist after smuggler import.");
    }

    private async Task CreateDocumentTombstoneAsync(IDocumentStore store)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        using (DocumentIdWorker.GetLoweredIdSliceFromId(context, SubjectDocId, out Slice lowerId))
        {
            var deleteResult = database.DocumentsStorage.Delete(
                context,
                lowerId,
                SubjectDocId,
                expectedChangeVector: null,
                lastModifiedTicks: DateTime.UtcNow.Ticks,
                changeVector: context.GetChangeVector(ImportedChangeVector),
                collectionName: new CollectionName(SubjectCollection),
                newFlags: DocumentFlags.None);

            Assert.NotNull(deleteResult);
            tx.Commit();
        }
    }
}
