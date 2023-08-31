using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class ProjectioOfMissingProp : RavenTestBase
    {
        public ProjectioOfMissingProp(ITestOutputHelper output) : base(output)
        {
        }

        public class Item
        {
            public List<string> Tags;
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanProjectArrayPropThatIsMissingInDoc(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Item
                    {
                    }, "items/1");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Query<Item>()
                        .Select(x => x.Tags)
                        .ToList();
                }
            }
        }
    }
}
