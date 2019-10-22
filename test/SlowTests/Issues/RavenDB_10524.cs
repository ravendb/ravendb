using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10524 : RavenTestBase
    {
        public RavenDB_10524(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void LazyLoadReallyShouldNotLoadTheEntireDatabaseIfWeFindDocumentFromSession()
        {
            using (var store = GetDocumentStore())
            {
                var doc = new Document
                {
                    Id = "myDocuments/123",
                    Name = "document"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<Document>(doc.Id);

                    session.Load<Document>(doc.Id); // eagerly load it

                    var before = session.Advanced.NumberOfRequests;

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    Assert.Equal(before, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncLazyLoadReallyShouldNotLoadTheEntireDatabaseIfWeFindDocumentFromSession()
        {
            using (var store = GetDocumentStore())
            {
                var doc = new Document
                {
                    Id = "myDocuments/123",
                    Name = "document"
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(doc);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Lazily.LoadAsync<Document>(doc.Id);

                    await session.LoadAsync<Document>(doc.Id); // eagerly load it

                    var before = session.Advanced.NumberOfRequests;

                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                    Assert.Equal(before, session.Advanced.NumberOfRequests);

                }
            }
        }
    }
}
