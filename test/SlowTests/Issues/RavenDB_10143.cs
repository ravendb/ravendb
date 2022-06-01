using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10143 : RavenTestBase
    {
        public RavenDB_10143(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Dynamic_select_distinct_query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() {Name = "Arek"});

                    session.Store(new User() {Name = "Arek"});

                    session.SaveChanges();

                    var results = session.Query<User>().Select(x => x.Name).Distinct().ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}
