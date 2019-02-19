using System;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace FastTests.Server.Basic
{
    public class IdleOperations : RavenTestBase
    {
        private class Product
        {
            public string ProductName { get; set; }
        }

        [Fact]
        public void Should_Update_LastIdle()
        {
            using (var store = GetDocumentStore())
            {
                var db = GetDatabase(store.Database).Result;

                var lastIdleTime = db.LastIdleTime;

                db.RunIdleOperations();

                Assert.NotEqual(lastIdleTime, db.LastIdleTime);
            }
        }

        [Fact]
        public void Should_Update_LastWork()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var db = GetDocumentDatabaseInstanceFor(store).Result;

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

        [Fact]
        public void Should_Cleanup_Resources()
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

                    var documentDatabase = landlord.TryGetOrCreateResourceStore("IdleOperations_CleanupResources_DB_" + i).Result;

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

                Server.ServerStore.IdleOperations(null);

                for (var i = 0; i < 10; i++)
                {
                    var name = "IdleOperations_CleanupResources_DB_" + i;

                    if (i % 2 == 1)
                        Assert.True(landlord.LastRecentlyUsed.TryGetValue(name, out outTime));
                    else
                        Assert.False(landlord.LastRecentlyUsed.TryGetValue(name, out outTime));

                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, true));
                }
            }
        }
    }
}
