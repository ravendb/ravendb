using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB10590 : RavenTestBase
    {
        [Fact]
        public async Task TestIdQueryTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new Doc { Id = "doc-" + i, StrVal = i.ToString() });
                    }
                    await session.SaveChangesAsync();

                    WaitForIndexing(store);

                    var query = session.Query<Doc, DocsIndex>()
                        .Where(x => x.Id == "doc-1")
                        .ProjectInto<Doc>(); // without ProjectInto, the query works: from index 'DocsIndex' where Id = $p0
                    var results = await query.ToListAsync();

                    Assert.Single(results);
                }
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }

        public class DocsIndex : AbstractIndexCreationTask<Doc>
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
                // Without reduce part, the query looks like this: from index 'DocsIndex' where id() = $p0 select id() as Id, StrVal
                // and works correctly.
                Reduce = results =>
                    from result in results
                    group result by result.Id
                    into g
                    let doc = g.First()
                    select new
                    {
                        doc.Id,
                        doc.StrVal,
                    };
            }
        }
    }
}
