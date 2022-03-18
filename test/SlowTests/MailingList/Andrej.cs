using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Andrej : RavenTestBase
    {
        public Andrej(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TestBoostedSearchCountTest()
        {
            using (var store = GetDocumentStore())
            {
                await new DocsIndex().ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc { Id = "doc-1", StrVal = "abc" });
                    await session.StoreAsync(new Doc { Id = "doc-2", StrVal = "doc" });
                    await session.StoreAsync(new Doc { Id = "doc-3", StrVal = "doc1" });
                    await session.SaveChangesAsync();

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<Doc, DocsIndex>()
                        .Search(x => x.StrVal, "doc*", boost: 2); // when boost is removed, works ok

                    // fails with System.ArgumentException: nDocs must be > 0
                    //at Lucene.Net.Search.IndexSearcher.Search(Weight weight, Filter filter, Int32 nDocs, IState state)
                    var count = await query.CountAsync();
                    Assert.Equal(2, count);
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
            }
        }
    }
}
