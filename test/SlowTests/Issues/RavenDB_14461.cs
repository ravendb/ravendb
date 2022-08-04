using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14461 : RavenTestBase
    {
        public RavenDB_14461(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task LoadWithNoTracking_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                AssertLoad(store, noTracking: false);
                AssertLoad(store, noTracking: true);

                await AssertLoadAsync(store, noTracking: false);
                await AssertLoadAsync(store, noTracking: true);

                AssertLoadStartsWith(store, noTracking: false);
                AssertLoadStartsWith(store, noTracking: true);

                await AssertLoadStartsWithAsync(store, noTracking: false);
                await AssertLoadStartsWithAsync(store, noTracking: true);
            }

            void AssertLoad(DocumentStore store, bool noTracking)
            {
                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Null(session.Load<Company>("orders/1"));
                }

                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Null(session.Advanced.Lazily.Load<Company>("orders/1").Value);
                }
            }

            async Task AssertLoadAsync(DocumentStore store, bool noTracking)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Null(await session.LoadAsync<Company>("orders/1"));
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Null(await session.Advanced.Lazily.LoadAsync<Company>("orders/1").Value);
                }
            }

            void AssertLoadStartsWith(DocumentStore store, bool noTracking)
            {
                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Empty(session.Advanced.LoadStartingWith<Company>("orders/"));
                }

                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Empty(session.Advanced.Lazily.LoadStartingWith<Company>("orders/").Value);
                }
            }

            async Task AssertLoadStartsWithAsync(DocumentStore store, bool noTracking)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Empty(await session.Advanced.LoadStartingWithAsync<Company>("orders/"));
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    Assert.Empty(await session.Advanced.Lazily.LoadStartingWithAsync<Company>("orders/").Value);
                }
            }
        }

        [Fact]
        public async Task FindByEmptyCollectionShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                AssertLoad(store, ids: new string[] { }, noTracking: false);
                AssertLoad(store, ids: new string[] { }, noTracking: true);

                AssertLoad(store, ids: new string[] { string.Empty }, noTracking: false);
                AssertLoad(store, ids: new string[] { string.Empty }, noTracking: true);

                await AssertLoadAsync(store, ids: new string[] { }, noTracking: false);
                await AssertLoadAsync(store, ids: new string[] { }, noTracking: true);

                await AssertLoadAsync(store, ids: new string[] { string.Empty }, noTracking: false);
                await AssertLoadAsync(store, ids: new string[] { string.Empty }, noTracking: true);
            }

            void AssertLoad(DocumentStore store, string[] ids, bool noTracking)
            {
                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    var objs = session.Load<Company>(ids);

                    Assert.NotNull(objs);
                    Assert.Empty(objs);
                }

                using (var session = store.OpenSession(new SessionOptions { NoTracking = noTracking }))
                {
                    var objs = session.Advanced.Lazily.Load<Company>(ids).Value;

                    Assert.NotNull(objs);
                    Assert.Empty(objs);
                }
            }

            async Task AssertLoadAsync(DocumentStore store, string[] ids, bool noTracking)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    var objs = await session.LoadAsync<Company>(ids);

                    Assert.NotNull(objs);
                    Assert.Empty(objs);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { NoTracking = noTracking }))
                {
                    var objs = await session.Advanced.Lazily.LoadAsync<Company>(ids).Value;

                    Assert.NotNull(objs);
                    Assert.Empty(objs);
                }
            }
        }
    }
}
