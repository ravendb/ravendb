using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10502 : RavenTestBase
    {
        public RavenDB_10502(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CollectionQueriesWithShouldReturnEmptyResultWhenNullIsPassed(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Document>()
                        .Where(x => x.Id.In(null, null, null));

                    var iq = RavenTestHelper.GetIndexQuery(query);
                    Assert.Equal("from 'Documents' where id() in ($p0)", iq.Query);
                    Assert.Equal(new object[] { null, null, null }, iq.QueryParameters["p0"]);

                    Assert.Equal(0, query.ToList().Count);

                    query = session.Query<Document>()
                        .Where(x => x.Id == null);

                    iq = RavenTestHelper.GetIndexQuery(query);
                    Assert.Equal("from 'Documents' where id() = $p0", iq.Query);
                    Assert.Null(iq.QueryParameters["p0"]);

                    Assert.Equal(0, query.ToList().Count);

                    query = session.Query<Document>()
                        .Where(x => x.Id.In(string.Empty, string.Empty, string.Empty));

                    iq = RavenTestHelper.GetIndexQuery(query);
                    Assert.Equal("from 'Documents' where id() in ($p0)", iq.Query);
                    Assert.Equal(new[] { string.Empty, string.Empty, string.Empty }, iq.QueryParameters["p0"]);

                    Assert.Equal(0, query.ToList().Count);

                    query = session.Query<Document>()
                        .Where(x => x.Id == string.Empty);

                    iq = RavenTestHelper.GetIndexQuery(query);
                    Assert.Equal("from 'Documents' where id() = $p0", iq.Query);
                    Assert.Equal(string.Empty, iq.QueryParameters["p0"]);

                    Assert.Equal(0, query.ToList().Count);

                    query = session.Query<Document>()
                        .Where(x => x.Name.In(null, null, null));

                    iq = RavenTestHelper.GetIndexQuery(query);
                    Assert.Equal("from 'Documents' where Name in ($p0)", iq.Query);
                    Assert.Equal(new object[] { null, null, null }, iq.QueryParameters["p0"]);

                    Assert.Equal(0, query.ToList().Count);
                }
            }
        }
    }
}
