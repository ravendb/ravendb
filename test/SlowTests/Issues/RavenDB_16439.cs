using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16439 : RavenTestBase
    {
        public RavenDB_16439(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanSuggest()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new DocumentationPage {Title = "Hamlet", TextContent = "aaa"});
                    await session.StoreAsync(new DocumentationPage {Title = "Makbet", TextContent = "bbb"});

                    await session.SaveChangesAsync();

                    var query =
                        session.Advanced.AsyncDocumentQuery<DocumentationPage>()
                            .Search(x => x.Title, "\"").Boost(50)
                            .OrElse()
                            .Search(x => x.TextContent, "\"aaa").Boost(35);

                    var results = await query.ToArrayAsync();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("Hamlet", results[0].Title);
                }
            }
        }

        private class DocumentationPage
        {
            public string TextContent { get; set; }
            public string Title { get; set; }
            public string Key { get; set; }
            public string Id { get; set; }
        }
    }
}
