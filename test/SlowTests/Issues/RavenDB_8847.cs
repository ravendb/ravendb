using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8847 : RavenTestBase
    {
        public RavenDB_8847(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_set_first_batch_timeout_of_newly_created_auto_index()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                database.IndexStore.StopIndexing();

                Raven.Server.Documents.Indexes.Index index;

                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>().Statistics(out var stats).Where(x => x.Name != "joe").ToListAsync();

                    index = database.IndexStore.GetIndex(stats.IndexName);

                    Assert.True(index._firstBatchTimeout.HasValue);
                }

                database.IndexStore.StartIndexing();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "B"
                    });

                    await session.SaveChangesAsync();

                    await session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name != "ema").ToListAsync();

                    Indexes.WaitForIndexBatchCompleted(store, x => x.DidWork).Wait(TimeSpan.FromSeconds(2));

                    // ensure the timeout was reset after the run completed
                    Assert.False(index._firstBatchTimeout.HasValue);
                }
            }
        }
        
        [Fact]
        public async Task Should_set_first_batch_timeout_of_newly_created_static_index()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var usersByname = "users/byname";

                await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition()
                {
                    Name = usersByname,
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    }
                }));

                database.IndexStore.StopIndexing();

                var index = database.IndexStore.GetIndex(usersByname);

                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>(usersByname).Customize(x => x.WaitForNonStaleResults()).ToListAsync();

                    Assert.True(index._firstBatchTimeout.HasValue);
                }

                database.IndexStore.StartIndexing();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "B"
                    });

                    await session.SaveChangesAsync();

                    await session.Query<User>(usersByname).Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name != "ema").ToListAsync();

                    Indexes.WaitForIndexBatchCompleted(store, x => x.DidWork).Wait(TimeSpan.FromSeconds(2));

                    // ensure the timeout was reset after the run completed
                    Assert.False(index._firstBatchTimeout.HasValue);
                }
            }
        }
    }
}
