using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14619 : RavenTestBase
    {
        public RavenDB_14619(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task NestedBoostTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc { Id = "doc-1", StrVal = "test", StrValSecondary = "test", Type = "1", NumVal = 1 });
                    await session.StoreAsync(new Doc { Id = "doc-2", StrVal = "test", StrValSecondary = "", Type = "1", NumVal = 2 });
                    await session.StoreAsync(new Doc { Id = "doc-3", StrVal = "test", StrValSecondary = "test", Type = "1", NumVal = null });
                    await session.StoreAsync(new Doc { Id = "doc-4", StrVal = "test", StrValSecondary = "", Type = "1", NumVal = 0 });
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    // Missing the outer boost in lucene query
                    var textFilterTypeFilterNumValsNullLastQuery = @"from index 'DocsIndex'
where
(true or boost(NumVal != null, 100))
and
boost(
  (
    boost(search(StrVal, ""test""), 100)
    or
    boost(search(StrValSecondary, ""test""), 10)
  )
  and
  (
    boost(Type == ""1"", 0)
  )
, 0)
order by score(), NumVal as double desc";

                    var textFilterTypeFilterNumValsNullLastResults = await session.Advanced.AsyncRawQuery<Doc>(textFilterTypeFilterNumValsNullLastQuery).ToListAsync();

                    Assert.Equal(new[] { "doc-2", "doc-1", "doc-4", "doc-3" }, textFilterTypeFilterNumValsNullLastResults.Select(x => x.Id).ToArray());
                }
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
            public string StrValSecondary { get; set; }
            public string Type { get; set; }
            public double? NumVal { get; set; }
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
                        doc.StrValSecondary,
                        doc.Type,
                        doc.NumVal,
                    };

                Indexes.Add(x => x.StrVal, FieldIndexing.Search);

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
