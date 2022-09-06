using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Utils;
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

    [Fact]
    public void DatabaseThrowsOnOpenWhenDisableMarkerIsInDirectory()
    {
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

    [Fact]
    public void IndexThrowsOnOpenWhenDisableMarkerIsInDirectory()
    {
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
