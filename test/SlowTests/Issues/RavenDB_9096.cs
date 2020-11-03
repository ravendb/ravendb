using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9096 : RavenTestBase
    {
        public RavenDB_9096(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void LongMinShouldBeParsedCorrectly()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var djv = new DynamicJsonValue
                {
                    ["Value"] = long.MinValue
                };

                var json = context.ReadObject(djv, "json");

                Assert.True(json.TryGetMember("Value", out var value));
                Assert.Equal(long.MinValue, value);

                var s = json.ToString();

                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    json = context.Sync.ReadForMemory(stream, "json");

                    Assert.True(json.TryGet("Value", out LazyNumberValue lnv));

                    Assert.Equal(long.MinValue, lnv.ToInt64(CultureInfo.InvariantCulture));
                }
            }
        }

        [Fact]
        public async Task TestNullIntTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocIndex().Execute(store);

                if (await ShouldInitData(store))
                {
                    await InitializeData(store);
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    // This assertion works but the following query in studio returns one result:
                    // from index 'DocIndex' where IntVal = -9223372036854775808
                    var filtered = await session.Query<Doc, DocIndex>().Where(x => x.IntVal == long.MinValue).ToListAsync();
                    Assert.Empty(filtered);

                    var results = await session.Query<Doc, DocIndex>().AggregateBy(x => x.ByField(y => y.IntVal)).ExecuteAsync();
                    Assert.Empty(results["IntVal"].Values.Where(x => x.Range == "-9223372036854775808"));
                    Assert.NotEmpty(results["IntVal"].Values.Where(x => x.Range == "1"));
                }
            }
        }

        private static async Task<bool> ShouldInitData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Doc>("doc/1");
                return doc == null;
            }
        }

        private static async Task InitializeData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc { Id = "doc/1", IntVal = 1 });
                await session.StoreAsync(new Doc { Id = "doc/2", IntVal = null });
                await session.SaveChangesAsync();
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public int? IntVal { get; set; }
        }

        private class DocIndex : AbstractIndexCreationTask<Doc>
        {
            public DocIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.IntVal,
                    };


                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
