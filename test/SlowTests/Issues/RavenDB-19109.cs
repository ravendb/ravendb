using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19109 : RavenTestBase
{
    public RavenDB_19109(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Name { get; set; }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void DatabaseThrowsOnOpenWhenDisableMarkerIsInDirectory()
    {
        DoNotReuseServer();

        using var store = GetDocumentStore(out var databasePath);

        DoCommand(store, true, out var exception);
        var disableMarkerPath = Path.Combine(databasePath, "disable.marker");
        File.Create(disableMarkerPath).Dispose();

        try
        {
            DoCommand(store, false, out exception, false);
            Assert.NotNull(exception);
            Assert.Contains(
                $"Unable to open database: '{store.Database}', it has been manually disabled via the file: '{disableMarkerPath}'. To re-enable, remove the disable.marker and reload the database.",
                exception.Message);


            File.Delete(disableMarkerPath);
            DoCommand(store, false, out exception);
            Assert.Null(exception);

            {
                using var session = store.OpenSession();
                Assert.Equal(1, session.Query<Item>().Count());
            }
        }
        finally
        {
            IOExtensions.DeleteFile(disableMarkerPath);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void IndexThrowsOnOpenWhenDisableMarkerIsInDirectory()
    {
        DoNotReuseServer();

        using var store = GetDocumentStore(out var databasePath);
        var index = new IndexToDisable();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        DoCommand(store, true, out var exception);
        Assert.Null(exception);
        var disableMarkerPath = Path.Combine(databasePath, "Indexes", index.IndexName, "disable.marker");
        File.Create(disableMarkerPath).Dispose();

        try
        {
            DoCommand(store, false, out exception, false);

            var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var errorMessage =
                $"Unable to open index: '{index.IndexName}', it has been manually disabled via the file: '{disableMarkerPath}'. To re-enable, remove the disable.marker file and enable indexing.";


            Assert.NotEqual(0, indexErrors.Count(i => i.Errors.Any(p => p.Error.Contains(errorMessage))));
            File.Delete(disableMarkerPath);
            DoCommand(store, true, out exception);
            DoCommand(store, false, out exception);
            {
                using var session = store.OpenSession();
                Assert.Equal(1, session.Query<Item, IndexToDisable>().Count());
            }
        }
        finally
        {
            IOExtensions.DeleteFile(disableMarkerPath);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task IdleOperationsShouldNotThrowWhenIndexIsDisabledByMarker()
    {
        DoNotReuseServer();

        using var store = GetDocumentStore(out var databasePath);
        var index = new IndexToDisable();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.IsNotType<FaultyInMemoryIndex>(database.IndexStore.GetIndex(index.IndexName));

        // sanity: a healthy index survives idle operations
        database.IndexStore.RunIdleOperations(DatabaseCleanupMode.Regular);
        database.IndexStore.RunIdleOperations(DatabaseCleanupMode.Deep);

        var disableMarkerPath = Path.Combine(databasePath, "Indexes", index.IndexName, "disable.marker");
        try
        {
            DoCommand(store, true, out _);
            File.Create(disableMarkerPath).Dispose();
            DoCommand(store, false, out _);

            database = await Databases.GetDocumentDatabaseInstanceFor(store);
            Assert.IsType<FaultyInMemoryIndex>(database.IndexStore.GetIndex(index.IndexName));

            // the faulty placeholder must not break the database-wide idle operations
            database.IndexStore.RunIdleOperations(DatabaseCleanupMode.Regular);
            database.IndexStore.RunIdleOperations(DatabaseCleanupMode.Deep);
        }
        finally
        {
            IOExtensions.DeleteFile(disableMarkerPath);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Cluster)]
    public async Task ClusterMaintenanceReportShouldNotThrowWhenIndexIsDisabledByMarker()
    {
        DoNotReuseServer();

        using var store = GetDocumentStore(out var databasePath);
        var index = new IndexToDisable();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.IsNotType<FaultyInMemoryIndex>(database.IndexStore.GetIndex(index.IndexName));

        // sanity: a healthy index can be reported
        DatabaseStatusReport.ObservedIndexStatus status = FillIndexInfo(database, index.IndexName);
        Assert.Equal(IndexState.Normal, status.State);
        Assert.False(status.IsStale);

        var disableMarkerPath = Path.Combine(databasePath, "Indexes", index.IndexName, "disable.marker");
        try
        {
            DoCommand(store, true, out _);
            File.Create(disableMarkerPath).Dispose();
            DoCommand(store, false, out _);

            database = await Databases.GetDocumentDatabaseInstanceFor(store);
            Assert.IsType<FaultyInMemoryIndex>(database.IndexStore.GetIndex(index.IndexName));

            // the faulty placeholder must still produce a status entry for the cluster observer
            status = FillIndexInfo(database, index.IndexName);
            Assert.Equal(IndexState.Error, status.State);
            Assert.True(status.IsStale);
            Assert.Equal((long)Raven.Server.Documents.Indexes.Index.IndexProgressStatus.Faulty, status.LastIndexedEtag);
        }
        finally
        {
            IOExtensions.DeleteFile(disableMarkerPath);
        }
    }

    private static DatabaseStatusReport.ObservedIndexStatus FillIndexInfo(DocumentDatabase database, string indexName)
    {
        var index = database.IndexStore.GetIndex(indexName);
        Assert.NotNull(index);

        var report = new DatabaseStatusReport();
        using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
        using (context.OpenReadTransaction())
        {
            ClusterMaintenanceWorker.FillIndexInfo(index, context, DateTime.UtcNow, report);
        }

        return report.LastIndexStats[indexName];
    }

    private IDocumentStore GetDocumentStore(out string databasePath, [CallerMemberName] string caller = null)
    {
        databasePath = NewDataPath();
        var store = GetDocumentStore(new Options()
        {
            RunInMemory = false,
            Path = databasePath,
            ModifyDatabaseRecord = databaseRecord =>
            {
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
            }
        }, caller);
        {
            using var session = store.OpenSession();
            session.Store(new Item() { Name = "Maciej" });
            session.SaveChanges();
        }

        return store;
    }

    private static void DoCommand(IDocumentStore store, bool disable, out Exception exception, bool shouldThrow = true)
    {

        try
        {
            store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable));
            exception = null;
        }
        catch (Exception e)
        {
            exception = e;
            if (shouldThrow)
                throw;
        }
    }

    private class IndexToDisable : AbstractIndexCreationTask<Item>
    {
        public IndexToDisable()
        {
            Map = items => items.Select(i => new Item() { Name = i.Name });
        }
    }
}
