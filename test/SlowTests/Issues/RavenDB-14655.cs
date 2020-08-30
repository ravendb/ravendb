using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14655 : RavenTestBase
    {
        public RavenDB_14655(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public async Task TombstonesCleanUpWithHubDefinition()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("hub"));
                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-12);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), "user/" + i);
                    }
                    session.SaveChanges();
                }
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(0, count);
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Delete("user/3");
                        session.SaveChanges();
                    }
                    
                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(1, count);
                    }

                    await database.TombstoneCleaner.ExecuteCleanup();

                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(1, count);
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow;
                    using (var session = store.OpenSession())
                    {
                        for (int i = 10; i < 20; i++)
                        {
                            session.Store(new User(), "user/" + i);
                        }
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Delete("user/2");
                        session.Delete("user/12");
                        session.SaveChanges();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(3, count);
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(4);

                    await database.TombstoneCleaner.ExecuteCleanup();
                    
                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(2, count);
                    }

                }
            }
        }

        [Fact]
        public async Task TombstonesCleanUpWithHubDefinition2()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("hub"));
                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-17);

                await database.TombstoneCleaner.ExecuteCleanup();

            }
        }

        [Fact]
        public async Task TombstonesCleanUpWithHubDefinition3()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("hub"));
                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-15);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), "user/" + i);
                    }
                    session.SaveChanges();
                }
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Delete("user/9");
                        session.SaveChanges();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(1, count);
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow;
                    await database.TombstoneCleaner.ExecuteCleanup();

                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(0, count);
                    }
                }
            }
        }

        [Fact]
        public async Task TombstonesCleanUpWithHubDefinition4()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("hub"));
                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-12);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), "user/" + i);
                    }
                    session.SaveChanges();
                }
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (context.OpenReadTransaction())
                    {
                        await database.TombstoneCleaner.ExecuteCleanup();
                        var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                        Assert.Equal(0, count);
                    }
                }
            }
        }

        [Fact]
        public async Task GetTombstoneAtOrBeforeTest()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("hub"));
                var database = await GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        session.Store(new User(), "user/" + i);
                    }
                    session.SaveChanges();
                }
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Delete("user/3");
                        session.Delete("user/4");
                        session.SaveChanges();
                    }
                    using (var session = store.OpenSession())
                    {
                        for (int i = 5; i < 10; i++)
                        {
                            session.Store(new User(), "user/" + i);
                        }
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Delete("user/7");
                        session.Delete("user/9");
                        session.SaveChanges();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var tombstone = database.DocumentsStorage.GetTombstoneAtOrBefore(context, 7);
                        Assert.Equal(7, tombstone.Etag);
                        tombstone = database.DocumentsStorage.GetTombstoneAtOrBefore(context, 8);
                        Assert.Equal(7, tombstone.Etag);
                    }

                }
            }
        }
    }
}
