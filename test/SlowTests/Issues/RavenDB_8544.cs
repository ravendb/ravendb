using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8544 : RavenTestBase
    {
        [Fact]
        public async Task Tombstone_cleaner_must_not_prevent_from_unloading_idle_database()
        {
            DoNotReuseServer();

            var name = "RavenDB_8544" + Guid.NewGuid();
            
            var landlord = Server.ServerStore.DatabasesLandlord;
            
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord(name);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                try
                {
                    var database = await landlord.TryGetOrCreateResourceStore(name);

                    database.Configuration.Core.RunInMemory = false; // in memory databases aren't unloaded

                    database.ResetIdleTime();

                    landlord.LastRecentlyUsed.AddOrUpdate(database.Name, DateTime.MinValue, (_, time) => DateTime.MinValue);

                    foreach (var env in database.GetAllStoragesEnvironment())
                        env.Environment.ResetLastWorkTime();

                    database.LastAccessTime = DateTime.MinValue;

                    await database.TombstoneCleaner.ExecuteCleanup();

                    Server.ServerStore.IdleOperations(null);

                    Assert.False(landlord.LastRecentlyUsed.TryGetValue(name, out _));
                }
                finally
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, true));
                }
            }
        }
    }
}
