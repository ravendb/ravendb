using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class AsyncTest : RavenTestBase
    {
        public AsyncTest(ITestOutputHelper output) : base(output)
        {
        }

        private class Dummy
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void SyncQuery()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var results = session.Query<Dummy>().ToList();
                Assert.Equal(0, results.Count);
                results = session.Query<Dummy>().ToList();
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public async Task AsyncQuery()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                var results = await session.Query<Dummy>().ToListAsync();
                Assert.Equal(0, results.Count);
                var results2 = await session.Query<Dummy>().ToListAsync();
                Assert.Equal(0, results2.Count);
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public async Task AsyncQuery_WithWhereClause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Dummy { Name = "oren" });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<Dummy>()
                                               .Customize(x => x.WaitForNonStaleResults())
                                               .Where(x => x.Name == "oren")
                                               .ToListAsync();
                    Assert.Equal(1, results.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task AsyncLoadNonExistant()
        {
            // load a non-existant entity
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<Dummy>("dummies/-1337");
                Assert.Null(loaded);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task AsyncLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Dummy());
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<Dummy>("dummies/1-A");
                    Assert.Equal(1, store.GetRequestExecutor().Cache.NumberOfItems);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<Dummy>("dummies/1-A");
                    Assert.Equal(1, store.GetRequestExecutor().Cache.NumberOfItems);
                }
            }
        }
    }
}
