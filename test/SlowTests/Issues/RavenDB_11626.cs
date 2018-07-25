using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11626 : RavenTestBase
    {
        [Fact]
        public async Task ExistsShouldReturnFalseWhenSessionContainsDocumentAsDeleted()
        {
            const string id = "docs/1";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Id = id
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>(id);
                    Assert.NotNull(doc);

                    session.Delete(doc);
                    doc = session.Load<Document>(id);
                    Assert.Null(doc);

                    Assert.False(session.Advanced.Exists(id));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<Document>(id);
                    Assert.NotNull(doc);

                    session.Delete(doc);
                    doc = await session.LoadAsync<Document>(id);
                    Assert.Null(doc);

                    Assert.False(await session.Advanced.ExistsAsync(id));
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
        }
    }
}
