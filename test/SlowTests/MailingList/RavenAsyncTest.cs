using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class AsyncTest : RavenTestBase
    {
        private class Dummy
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
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

        [Fact]
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

        [Fact]
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

        [Fact]
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

        [Fact]
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
                    await session.LoadAsync<Dummy>("dummies/1");
                    Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<Dummy>("dummies/1");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
                }
            }
        }
    }
}
