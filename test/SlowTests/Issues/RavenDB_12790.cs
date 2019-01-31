using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12790 : RavenTestBase
    {
        [Fact]
        public void LazyQueryAgainstMissingIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var document = new Document
                    {
                        Name = "name"
                    };
                    session.Store(document);
                    session.SaveChanges();
                }

                // intentionally not creating the index that we query against

                using (var session = store.OpenSession())
                {
                    Assert.Throws<IndexDoesNotExistException>(
                        () => session.Query<Document, DocumentIndex>().ToList());
                }

                using (var session = store.OpenSession())
                {
                    var lazyQuery = session.Query<Document, DocumentIndex>()
                        .Lazily();

                    Assert.Throws<IndexDoesNotExistException>(() => lazyQuery.Value);
                }
            }
        }

        private class DocumentIndex : AbstractIndexCreationTask<Document>
        {
        }

        private class Document
        {
            public string Name;
        }
    }
}
