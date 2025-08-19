using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Web.System;
using Sparrow;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Server.Basic
{
    public class IdleOperations : RavenTestBase
    {
        public IdleOperations(ITestOutputHelper output) : base(output)
        {
        }

        private class Product
        {
            public string ProductName { get; set; }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task Should_Update_LastIdle()
        {
            using (var store = GetDocumentStore())
            {
                var db = await GetDatabase(store.Database);

                var lastIdleTime = db.LastIdleTime;

                db.RunIdleOperations();

                Assert.NotEqual(lastIdleTime, db.LastIdleTime);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task Should_Update_LastWork()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                foreach (var env in db.GetAllStoragesEnvironment())
                {
                    env.Environment.ResetLastWorkTime();
                }

                session.Store(new Product()
                {
                    ProductName = "coffee"
                }, "products/1");

                session.SaveChanges();

                var newWorkTime = db.GetAllStoragesEnvironment().Max(env => env.Environment.LastWorkTime);

                Assert.NotEqual(DateTime.MinValue, newWorkTime);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task Should_Cleanup_Resources()
        {
            DoNotReuseServer();

            DateTime outTime;
            var landlord = Server.ServerStore.DatabasesLandlord;

            using (var store = GetDocumentStore())
            {
                for (var i = 0; i < 10; i++)
                {
                    var name = "IdleOperations_CleanupResources_DB_" + i;
                    var doc = new DatabaseRecord(name);

                    store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                    var documentDatabase = await landlord.TryGetOrCreateResourceStore("IdleOperations_CleanupResources_DB_" + i);

                    documentDatabase.Configuration.Core.RunInMemory = false;

                    if (i % 2 == 0)
                    {
                        documentDatabase.ResetIdleTime();

                        landlord.LastRecentlyUsed.AddOrUpdate(documentDatabase.Name, DateTime.MinValue, (_, time) => DateTime.MinValue);

                        foreach (var env in documentDatabase.GetAllStoragesEnvironment())
                            env.Environment.ResetLastWorkTime();

                        documentDatabase.LastAccessTime = DateTime.MinValue;
                    }
                }

                var stats = new Dictionary<StringSegment, DatabasesDebugHandler.IdleDatabaseStatistics>();
                Server.ServerStore.IdleOperations(stats);

                for (var i = 0; i < 10; i++)
                {
                    var name = "IdleOperations_CleanupResources_DB_" + i;

                    try
                    {
                        if (i % 2 == 1)
                            Assert.True(landlord.LastRecentlyUsed.TryGetValue(name, out outTime), name);
                        else
                            Assert.False(landlord.LastRecentlyUsed.TryGetValue(name, out outTime), name);
                    }
                    catch (Exception)
                    {
                        foreach (var kvp in stats)
                            Output.WriteLine($"[{kvp.Key}]. Explanations: {string.Join(", ", kvp.Value.Explanations)}");
                        
                        throw;
                    }
                    finally
                    {
                        store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, true));
                    }
                }
            }
        }
    }
}
