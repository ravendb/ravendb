using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12798 : RavenTestBase
    {
        public RavenDB_12798(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void LengthFromArrayToCustomProjectionClass()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryable = session.Query<Document>()
                        .Select(x => new DocumentProjection
                        {
                            IdCount = x.IdCollection.Length
                        });

                    Assert.Equal("from 'Documents' select IdCollection.Length as IdCount", queryable.ToString());

                    var doc = queryable.SingleOrDefault();

                    Assert.NotNull(doc);
                    Assert.Equal(2, doc.IdCount);
                }
            }
        }

        [Fact]
        public void LengthFromArrayToAnonymousProjectionClassShouldNotGenerateJsProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryable = session.Query<Document>()
                        .Select(x => new
                        {
                            IdCount = x.IdCollection.Length
                        });

                    Assert.Equal("from 'Documents' select IdCollection.Length as IdCount", queryable.ToString());

                    var doc = queryable.SingleOrDefault();

                    Assert.NotNull(doc);
                    Assert.Equal(2, doc.IdCount);
                }
            }
        }

        private class DocumentProjection
        {
            public int IdCount { get; set; }
        }

        private class Document
        {
            public string[] IdCollection { get; set; } = { "items/1", "items/2" };
        }
    }
}
