using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19830 : RavenTestBase
{
    public RavenDB_19830(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanProjectWhenFieldAndPropertyNameAreTheSameWithDifferentCasing(Options options)
    {
        const string id = "testdocument/1";
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            var testDoc = new DocWithLambdaProperties
            {
                Id = id,
                Str = "hello"
            };

            await session.StoreAsync(testDoc);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var testDocs =
                await session.Query<DocWithLambdaProperties>()
                    .ToArrayAsync();
            Assert.Equal("hello", testDocs[0].Str);   // <-- SUCCEEDS
        }

        using (var session = store.OpenAsyncSession())
        {
            var testDocs =
                await session.Query<DocWithLambdaProperties>()
                    .ProjectInto<DocWithLambdaProperties>()
                    .ToArrayAsync();
            Assert.Equal("hello", testDocs[0].Str);
        }
    }

    private class DocWithLambdaProperties
    {
        private string str;
        public string Id { get; set; }
        public string Str
        {
            get => str;
            set => str = value;
        }
    }
}
