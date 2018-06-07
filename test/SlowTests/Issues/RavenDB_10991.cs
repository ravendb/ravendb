using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10991 : RavenTestBase
    {
        [Fact]
        public async Task PreventFromUnloadingShouldWork()
        {
            DoNotReuseServer();

            var name = "RavenDB_10991" + Guid.NewGuid();

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

                    using (database.PreventFromUnloading()) // should prevent from unloading
                        Server.ServerStore.IdleOperations(null);

                    Assert.True(landlord.LastRecentlyUsed.TryGetValue(name, out _));

                    Server.ServerStore.IdleOperations(null); // should unload

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
