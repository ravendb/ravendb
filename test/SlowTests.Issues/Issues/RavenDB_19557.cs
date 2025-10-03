using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19557 : RavenTestBase
{
    public RavenDB_19557(ITestOutputHelper output) : base(output)
    {
    }

    private class TestObj
    {
        public string[] Props { get; set; }
        public string Prop { get; set; }
    }

    private class Index : AbstractMultiMapIndexCreationTask<Index.Result>
    {
        public class Result
        {
            public IEnumerable<string> MapProp { get; set; }
        }
        public Index()
        {
            AddMap<TestObj>(testObjs =>
                from obj in testObjs
                select new Result
                {
                    MapProp = obj.Props.Concat(new[] { obj.Prop }).Distinct()
                });

            StoreAllFields(FieldStorage.Yes);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task TestCase(Options options)
    {
        using var store = GetDocumentStore(options);

        await store.ExecuteIndexAsync(new Index());

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj
            {
                Props = new[] { "value1" },
                Prop = "value1"
            });

            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenAsyncSession())
        {
            var query = from r in session.Query<Index.Result, Index>()
                        select new
                        {
                            R = r.MapProp.ToArray()
                        };

            var result = await query.SingleAsync();

            // {"R":["value1","value1"]}
            Assert.Single(result.R); //Fails here
        }
    }
}
