using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12334 : RavenTestBase
    {
        [Fact]
        public async Task Map_reduce_results_should_not_contains_implicit_nulls_wich_were_not_indexed()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc { Id = "doc-1", StrVal = "a", });
                    await session.SaveChangesAsync();

                    WaitForIndexing(store);
                    WaitForUserToContinueTheTest(store);

                    var result = await session.Query<BlittableJsonReaderObject, DocsIndex>().FirstAsync();

                    Assert.True(result.TryGet("Id", out object _));
                    Assert.True(result.TryGet("StrVal", out object _));
                    Assert.False(result.TryGet("AuxId", out object _));
                    Assert.False(result.TryGet("NumVal", out object _));
                }
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }

        public class DocView
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
            public string AuxId { get; set; }
            public double? NumVal { get; set; }
        }

        public class DocsIndex : AbstractMultiMapIndexCreationTask<DocView>
        {
            public DocsIndex()
            {
                AddMap<Doc>(docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.StrVal,
                        AuxId = (string)null,
                        NumVal = (double?)null,
                    });

                Reduce = results =>
                    from result in results
                    group result by result.Id
                    into g
                    let doc = g.First(x => x.StrVal != null)
                    let auxDoc = g.First(x => x.AuxId != null)
                    select new
                    {
                        Id = g.Key,
                        StrVal = doc.StrVal ?? null,
                        // those will be DynamicNullObject objects where IsExplicitNull == false so we won't index them
                        auxDoc.AuxId, 
                        auxDoc.NumVal,
                    };
            }
        }
    }
}
