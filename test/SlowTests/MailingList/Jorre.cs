using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Jorre : RavenTestBase
    {
        public Jorre(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryOnNegativeDecimal(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Boat>()
                        .Where(x => x.Weight == -1)
                        .ToList();
                }
            }
        }

        private class Boat
        {
            public decimal Weight { get; set; }
        }
    }
}
