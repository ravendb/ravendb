using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries
{
    public class HighlightingQuery : RavenTestBase
    {
        public HighlightingQuery(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void HighlightingOnAutomap()
        {
            using var store = GetDocumentStore();

            CreateNorthwindDatabase(store);
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
