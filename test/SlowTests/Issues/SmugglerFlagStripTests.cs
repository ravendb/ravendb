using System;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class SmugglerFlagStripTests : TombstoneLineagePreservationTestBase
{
    private const string SubjectDocId = "tickets/smuggler-flag-strip";
    private const string SubjectCollection = "Users";
    private const string SubjectName = "Bob";
    private const string ImportedChangeVector = "A:1-OSKWIRBEDEGoAxbEIiFJeQ";

    public SmugglerFlagStripTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    public async Task DirectImport_ShouldNormalizeFilteredPullFlag_ForDocumentsAndTombstones(
        Options options,
        bool importTombstone,
        bool includeFlag)
    {
        using var target = GetDocumentStore(options);
        var operateOnTypes = importTombstone ? DatabaseItemType.Tombstones : DatabaseItemType.Documents;
        await using var dump = importTombstone
            ? await CreateDirectTombstoneExportDumpAsync(options, includeFlag)
            : CreateDocumentDump(includeFlag);

        await ImportDumpAsync(target, dump, operateOnTypes);

        if (importTombstone)
        {
            var importedFlags = await GetDocumentTombstoneFlagsAsync(target, SubjectDocId);
            AssertNotFlagged(
                importedFlags,
                includeFlag
                    ? "direct-set document tombstone imported from smuggler dump"
                    : "unflagged document tombstone imported from smuggler dump");
            return;
        }

        var documentFlags = await GetDocumentFlagsAsync(target, SubjectDocId);
        AssertNotFlagged(
            documentFlags,
            includeFlag
                ? "direct-set document imported from smuggler dump"
                : "unflagged document imported from smuggler dump");
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ExportImport_FromActualFilteredPullSource_ShouldNormalizeFilteredPullFlag_ForDocumentsAndTombstones(
        Options options,
        bool exportTombstone)
    {
        await using var lab = await CreateLabAsync(options);

        using (var blockCToA = lab.BlockLink(LineageNode.C, LineageNode.A))
        using (var blockCToB = lab.BlockLink(LineageNode.C, LineageNode.B))
        {
            await lab.WriteAndInjectTicketAsync(SubjectDocId, LineageNode.C, LineageNode.A);
        }

        Assert.True(
            lab.WaitForDoc(LineageNode.A, SubjectDocId, timeout: 60_000),
            $"Expected actual filtered-pull document '{SubjectDocId}' to arrive on source hub A.");

        var sourceDoc = lab.GetDocumentSnapshot(LineageNode.A, SubjectDocId);
        Assert.True(sourceDoc.Exists, $"Expected source document '{SubjectDocId}' on hub A before export.");
        AssertFlagged(sourceDoc.Flags, "source document on hub A after filtered pull injection");

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
                $"Expected filtered-lineage tombstone for '{SubjectDocId}' on source hub A.");

            var sourceTombstone = lab.GetDocumentTombstoneSnapshot(LineageNode.A, SubjectDocId);
            Assert.True(sourceTombstone.Exists, $"Expected source tombstone '{SubjectDocId}' on hub A before export.");
            AssertFlagged(sourceTombstone.Flags, "source tombstone on hub A before smuggler export from a live filtered-pull topology");
        }

        var operateOnTypes = exportTombstone ? DatabaseItemType.Tombstones : DatabaseItemType.Documents;
        using var target = lab.CreateIsolatedStore($"{SubjectDocId.Replace('/', '-')}-import-{Guid.NewGuid():N}");
        await using var dump = await ExportDumpAsync(lab.StoreFor(LineageNode.A), operateOnTypes);
        await ImportDumpAsync(target, dump, operateOnTypes);

        if (exportTombstone)
        {
            var importedFlags = await GetDocumentTombstoneFlagsAsync(target, SubjectDocId);
            AssertNotFlagged(importedFlags, "tombstone imported from actual filtered-pull source export");
            return;
        }

        var documentFlags = await GetDocumentFlagsAsync(target, SubjectDocId);
        AssertNotFlagged(documentFlags, "document imported from actual filtered-pull source export");
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task ImportedDocument_LocalUpdate_ShouldNotPreserveFilteredPullFlag(
        Options options,
        bool targetHasActiveFilteredPull)
    {
        using var directDump = CreateDocumentDump(includeFlag: true);

        if (targetHasActiveFilteredPull == false)
        {
            using var target = GetDocumentStore(options);

            await ImportDumpAsync(target, directDump, DatabaseItemType.Documents);

            var dbCvBefore = await GetDatabaseChangeVectorAsync(target);
            await UpdateDocumentAsync(target, SubjectDocId, "fresh-local-update");

            var dbCvAfter = await GetDatabaseChangeVectorAsync(target);
            var flagsAfter = await GetDocumentFlagsAsync(target, SubjectDocId);

            AssertNotFlagged(flagsAfter, "document updated locally after import into a fresh database");
            Assert.NotEqual(dbCvBefore, dbCvAfter);
            return;
        }

        await using var lab = await CreateLabAsync(options);
        _ = await lab.CreateExternalSinkStoreAsync(LineageNode.B, PullReplicationMode.HubToSink);

        var targetStore = lab.StoreFor(LineageNode.B);
        await ImportDumpAsync(targetStore, directDump, DatabaseItemType.Documents);

        var dbCvBeforeActive = await GetDatabaseChangeVectorAsync(targetStore);
        await UpdateDocumentAsync(targetStore, SubjectDocId, "active-filtered-local-update");

        var dbCvAfterActive = await GetDatabaseChangeVectorAsync(targetStore);
        var documentAfterActive = lab.GetDocumentSnapshot(LineageNode.B, SubjectDocId);
        Assert.True(documentAfterActive.Exists, $"Expected imported document '{SubjectDocId}' on active filtered target B after local update.");

        AssertNotFlagged(documentAfterActive.Flags, "document updated locally after import into a database with active filtered pull configuration");
        Assert.NotEqual(dbCvBeforeActive, dbCvAfterActive);
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

    private static MemoryStream CreateDocumentDump(bool includeFlag)
    {
        var metadata = new JsonObject
        {
            ["@collection"] = SubjectCollection,
            ["@change-vector"] = ImportedChangeVector,
            ["@id"] = SubjectDocId,
            ["@last-modified"] = "2026-04-10T00:00:00.0000000Z"
        };

        if (includeFlag)
            metadata["@flags"] = nameof(DocumentFlags.FromFilteredPullReplicationHub);

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

    private async Task<MemoryStream> CreateDirectTombstoneExportDumpAsync(Options options, bool includeFlag)
    {
        using var source = GetDocumentStore(options);
        var flags = includeFlag ? DocumentFlags.FromFilteredPullReplicationHub : DocumentFlags.None;
        await CreateDocumentTombstoneAsync(source, flags);
        return await ExportDumpAsync(source, DatabaseItemType.Tombstones);
    }

    private static JsonObject CreateBaseDumpRoot()
    {
        return new JsonObject
        {
            ["BuildVersion"] = 60,
            ["DatabaseRecord"] = new JsonObject
            {
                ["DatabaseName"] = "smuggler-flag-strip",
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

    private async Task UpdateDocumentAsync(IDocumentStore store, string id, string newName)
    {
        using var session = store.OpenAsyncSession();
        var user = await session.LoadAsync<User>(id);
        Assert.NotNull(user);
        user.Name = newName;
        await session.SaveChangesAsync();
    }

    private async Task<DocumentFlags> GetDocumentFlagsAsync(IDocumentStore store, string id)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (var document = database.DocumentsStorage.Get(context, id))
        {
            Assert.NotNull(document);
            return document.Flags;
        }
    }

    private async Task<DocumentFlags> GetDocumentTombstoneFlagsAsync(IDocumentStore store, string id)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var documentOrTombstone = database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
            try
            {
                Assert.NotNull(documentOrTombstone.Tombstone);
                return documentOrTombstone.Tombstone.Flags;
            }
            finally
            {
                documentOrTombstone.Document?.Dispose();
                documentOrTombstone.Tombstone?.Dispose();
            }
        }
    }

    private async Task<string> GetDatabaseChangeVectorAsync(IDocumentStore store)
    {
        var statistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        return statistics.DatabaseChangeVector;
    }

    private async Task CreateDocumentTombstoneAsync(IDocumentStore store, DocumentFlags flags)
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
                newFlags: flags);

            Assert.NotNull(deleteResult);
            tx.Commit();
        }
    }

    private static void AssertFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(
            flags.Contain(DocumentFlags.FromFilteredPullReplicationHub),
            $"Expected {subject} to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
    }

    private static void AssertNotFlagged(DocumentFlags flags, string subject)
    {
        Assert.True(
            flags.Contain(DocumentFlags.FromFilteredPullReplicationHub) == false,
            $"Expected {subject} NOT to keep {nameof(DocumentFlags.FromFilteredPullReplicationHub)}, but flags were '{flags}'.");
    }
}
