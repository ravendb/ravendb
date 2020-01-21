using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14461 : RavenTestBase
    {
        public RavenDB_14461(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task LoadWithNoTracking_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Company>("orders/1"));
                }

                using (var session = store.OpenSession())
                {
                    Assert.Empty(session.Advanced.LoadStartingWith<Company>("orders/"));
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Null(await session.LoadAsync<Company>("orders/1"));
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Empty(await session.Advanced.LoadStartingWithAsync<Company>("orders/"));
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Null(session.Load<Company>("orders/1"));
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Empty(session.Advanced.LoadStartingWith<Company>("orders/"));
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Null(await session.LoadAsync<Company>("orders/1"));
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Empty(await session.Advanced.LoadStartingWithAsync<Company>("orders/"));
                }
            }
        }

        [Fact]
        public async Task FindByEmptyCollectionShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ids = new string[] { };
                    var objs = session.Load<Company>(ids);

                    Assert.NotNull(objs);
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    var ids = new string[] { };
                    var objs = session.Load<Company>(ids);

                    Assert.NotNull(objs);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ids = new string[] { };
                    var objs = await session.LoadAsync<Company>(ids);

                    Assert.NotNull(objs);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    var ids = new string[] { };
                    var objs = await session.LoadAsync<Company>(ids);

                    Assert.NotNull(objs);
                }
            }
        }
    }
}
