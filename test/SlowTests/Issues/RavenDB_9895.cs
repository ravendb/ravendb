using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9895:RavenTestBase
    {
        public RavenDB_9895(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TestIntersectTest()
        {
            using (var store = GetDocumentStore())
            {
                await new DocsIndex().ExecuteAsync(store);

                if (await ShouldInitData(store))
                {
                    await InitializeData(store);
                }
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenAsyncSession())
                {                    

                    var query = session.Query<Doc, DocsIndex>()
                        .Where(x => x.StrVal1 == "Doc0")
                        .Intersect()
                        .Where(x => x.StrVal2 == "Doc1");

                    var result = await query.ToListAsync();
                    Assert.Equal(5, result.Count);
                }
            }
        }

        private async Task<bool> ShouldInitData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Doc>("doc/1");
                return doc == null;
            }
        }

        private async Task InitializeData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 1; i <= 10; i++)
                {
                    await session.StoreAsync(new Doc { Id = "doc/" + i, StrVal1 = "Doc" + (i / 10), StrVal2 = "Doc" + (i / 5) });
                }
                await session.SaveChangesAsync();
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public string StrVal1 { get; set; }
            public string StrVal2 { get; set; }
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
                        doc.StrVal1,
                        doc.StrVal2,
                    };

                // Querying works correctly without reduce part.
                Reduce = results =>
                    from result in results
                    group result by result.Id
                    into g
                    let doc = g.FirstOrDefault(x => !string.IsNullOrEmpty(x.StrVal1))
                    select new
                    {
                        Id = g.Key,
                        StrVal1 = doc.StrVal1,
                        StrVal2 = doc.StrVal2,
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
