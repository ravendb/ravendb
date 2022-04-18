using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class HighlightingQuery : RavenTestBase
    {
        public HighlightingQuery(ITestOutputHelper output) : base(output)
        {
        }

        public class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void HighlightingOnAutomap(Options option)
        {
            using var store = GetDocumentStore(option);

            Samples.CreateNorthwindDatabase(store);
            {
                using var session = store.OpenSession();
                var result = session.Query<Company>()
                    .Search(p => p.Name, "alf*")
                    .Highlight(x => x.Name, 128, 1, out Highlightings highlightings)
                    .Single();

                WaitForUserToContinueTheTest(store);

                Assert.Equal("<b style=\"background:yellow\">Alfreds</b> Futterkiste", highlightings.GetFragments(result.Id)[0]);
            }
        }
    }
}
