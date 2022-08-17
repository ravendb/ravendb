using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19109 : RavenTestBase
{
    public RavenDB_19109(ITestOutputHelper output) : base(output)
    {
    }

    private record Item(string Name);

    [Fact]
    public void DatabaseThrowsOnOpenWhenDisableMarkerIsInDirectory()
    {
        var databasePath = NewDataPath();
        using var store = GetDocumentStore(new Options() {RunInMemory = false, Path = databasePath});
        {
            using var session = store.OpenSession();
            session.Store(new Item("Maciej"));
            session.SaveChanges();
        }


        Exception exception = null;
        DatabaseToggle(true);
        var disableMarkerPath = Path.Combine(databasePath, "disable.marker");
        File.Create(disableMarkerPath).Dispose();

        DatabaseToggle(false, false);
        Assert.NotNull(exception);
        Assert.Contains(
            $"Unable to open database: '{store.Database}', it has been manually disabled via the file: '{disableMarkerPath}'. To re-enable, remove the disable.marker and reload the database.",
            exception.Message);


        File.Delete(disableMarkerPath);
        DatabaseToggle(false);
        Assert.Null(exception);
        
        {
            using var session = store.OpenSession();
            Assert.Equal(1, session.Query<Item>().Count());
        }
        void DatabaseToggle(bool disable, bool shouldThrow = true)
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
    }
}
