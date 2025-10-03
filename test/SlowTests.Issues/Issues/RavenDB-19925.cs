using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19925 : RavenTestBase
{
        
    public class DocWithArray
    {
        public string Id { get; set; }
        public string[] ArrayStr { get; set; }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanProjectArrayFromDocument(Options options)
    {
        const string id = "testdocument";
        using var store = GetDocumentStore(options);        
        using (var session = store.OpenAsyncSession())
        {
            DocWithArray[] testDocs = new DocWithArray[4];
            for(int x = 0; x < 4; x++) {
                testDocs[x] = new DocWithArray {
                    Id = id + "/" + x.ToString(),
                    ArrayStr = new[] { "hello" }
                };
                await session.StoreAsync(testDocs[x]);
            };

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            IQueryable<string[]> stringsEnumerable = session.Query<DocWithArray>()
                .Select(x => x.ArrayStr);
            var stringArrays =
                await stringsEnumerable  // <---
                    .ToArrayAsync();
            Assert.Equal(4, stringArrays.Length);
            Assert.Equal(new string[]{"hello"}, stringArrays[0]);
        }
    }

    public RavenDB_19925(ITestOutputHelper output) : base(output)
    {
    }
}
