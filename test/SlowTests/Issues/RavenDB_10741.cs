using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10741 : RavenTestBase
    {
        public RavenDB_10741(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void LazyLoadingWithDuplicateIdsWhenAlreadyInSessionShouldSucceed()
        {
            using (var store = GetDocumentStore())
            {
                var doc = new Document
                {
                    Name = "document"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Document>(new [] { doc.Id, doc.Id });
                    Assert.Equal(1, loaded.Count);
                    Assert.Contains(doc.Id, loaded.Keys);

                    loaded = session.Advanced.Lazily.Load<Document>(new [] { doc.Id, doc.Id }).Value;
                    Assert.Equal(1, loaded.Count);
                    Assert.Contains(doc.Id, loaded.Keys);
                }
            }
        }
        
        [Fact]
        public async Task AsyncLazyLoadingWithDuplicateIdsWhenAlreadyInSessionShouldSucceed()
        {
            using (var store = GetDocumentStore())
            {
                var doc = new Document
                {
                    Name = "document"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<Document>(new [] { doc.Id, doc.Id });
                    Assert.Equal(1, loaded.Count);
                    Assert.Contains(doc.Id, loaded.Keys);

                    var promise = session.Advanced.Lazily.LoadAsync<Document>(new [] { doc.Id, doc.Id });
                    loaded = await promise.Value;
                    Assert.Equal(1, loaded.Count);
                    Assert.Contains(doc.Id, loaded.Keys);
                }
            }
        }

    }
}
