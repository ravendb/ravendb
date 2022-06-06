using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18740 : RavenTestBase
{
    public RavenDB_18740(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Notifications_Are_Isolated()
    {
        using (var store1 = GetDocumentStore())
        using (var store2 = GetDocumentStore())
        {
            var database1 = await GetDocumentDatabaseInstanceFor(store1);
            var database2 = await GetDocumentDatabaseInstanceFor(store2);

            Assert.Equal(0, database1.NotificationCenter.GetAlertCount());
            Assert.Equal(0, database1.NotificationCenter.GetPerformanceHintCount());

            using (database1.NotificationCenter.GetStored(out var actions))
                Assert.Equal(0, actions.ToList().Count);

            Assert.Equal(0, database2.NotificationCenter.GetAlertCount());
            Assert.Equal(0, database2.NotificationCenter.GetPerformanceHintCount());

            using (database2.NotificationCenter.GetStored(out var actions))
                Assert.Equal(0, actions.ToList().Count);

            database1.NotificationCenter.Add(AlertRaised.Create(store1.Database, "Test", "Test Message", AlertType.Etl_Error, NotificationSeverity.Warning));

            Assert.Equal(1, database1.NotificationCenter.GetAlertCount());
            Assert.Equal(0, database1.NotificationCenter.GetPerformanceHintCount());

            using (database1.NotificationCenter.GetStored(out var actions))
                Assert.Equal(1, actions.ToList().Count);

            Assert.Equal(0, database2.NotificationCenter.GetAlertCount());
            Assert.Equal(0, database2.NotificationCenter.GetPerformanceHintCount());

            using (database2.NotificationCenter.GetStored(out var actions))
                Assert.Equal(0, actions.ToList().Count);
        }
    }

    [Fact]
    public async Task Notifications_Are_Persisted()
    {
        var databaseName = GetDatabaseName();

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseName = _ => databaseName,
            RunInMemory = false,
            DeleteDatabaseOnDispose = false
        }))
        {
            var database = await GetDocumentDatabaseInstanceFor(store);

            database.NotificationCenter.Add(AlertRaised.Create(store.Database, "Test", "Test Message", AlertType.Etl_Error, NotificationSeverity.Warning));

            using (database.NotificationCenter.GetStored(out var actions))
                Assert.Equal(1, actions.ToList().Count);
        }

        using (var store = GetDocumentStore(new Options
        {
            CreateDatabase = false,
            ModifyDatabaseName = _ => databaseName,
            RunInMemory = false
        }))
        {
            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.NotificationCenter.GetStored(out var actions))
                Assert.Equal(1, actions.ToList().Count);
        }

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseName = _ => databaseName,
            RunInMemory = false
        }))
        {
            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.NotificationCenter.GetStored(out var actions))
                Assert.Equal(0, actions.ToList().Count);
        }
    }
}
