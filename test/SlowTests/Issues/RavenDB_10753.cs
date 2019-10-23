using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10753 : RavenTestBase
    {
        public RavenDB_10753(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void WhenPendingLazyLoadsAndNormalLoadIssued_MultiLazyLoadShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var doc1 = new Document
                {
                    Name = "document 1"
                };

                var doc2 = new Document
                {
                    Name = "document 2"
                };
                
                using (var session = store.OpenSession())
                {
                    session.Store(doc1);
                    session.Store(doc2);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var documentPromise1 = session.Advanced.Lazily.Load<Document>(doc1.Id);
                    var documentPromise2 = session.Advanced.Lazily.Load<Document>(doc2.Id);
                    var loaded3 = session.Load<Document>(doc1.Id);
                    
                    var loaded1 = documentPromise1.Value;
                    var loaded2 = documentPromise2.Value;

                    Assert.NotNull(loaded1);
                    Assert.NotNull(loaded2);
                    Assert.NotNull(loaded3);
                }
            }
        }

        [Fact]
        public async Task WhenPendingLazyLoadsAndNormalLoadIssued_AsyncMultiLazyLoadShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var doc1 = new Document
                {
                    Name = "document 1"
                };

                var doc2 = new Document
                {
                    Name = "document 2"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(doc1);
                    session.Store(doc2);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var documentPromise1 = session.Advanced.Lazily.LoadAsync<Document>(doc1.Id);
                    var documentPromise2 = session.Advanced.Lazily.LoadAsync<Document>(doc2.Id);
                    var loaded3 = await session.LoadAsync<Document>(doc1.Id);

                    var loaded1 = await documentPromise1.Value;
                    var loaded2 = await documentPromise2.Value;

                    Assert.NotNull(loaded1);
                    Assert.NotNull(loaded2);
                    Assert.NotNull(loaded3);
                }
            }
        }
    }
}
