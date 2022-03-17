using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class OrderById : RavenTestBase
    {
        public OrderById(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task OrderByIdTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    Indexes.WaitForIndexing(store);

                    // RQL ordering works with "Id"
                    var rquery = session.Advanced.AsyncRawQuery<Doc>("from index DocsIndex order by Id");
                    var rresults = await rquery.ToListAsync();

                    var query = session.Query<Doc, DocsIndex>().OrderBy(x => x.Id);

                    Assert.Equal("from index 'DocsIndex' order by Id", query.ToString());

                    var results = await query.ToListAsync();
                }
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }

        private class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.StrVal,
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Id
                    into g
                    let doc = g.First()
                    select new
                    {
                        Id = g.Key,
                        doc.StrVal,
                    };
            }
        }
    }
}
