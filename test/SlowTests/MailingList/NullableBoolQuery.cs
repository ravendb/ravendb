using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class NullableBoolQuery : RavenTestBase
    {
        public NullableBoolQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public bool? Active { get; set; }
        }

        [Fact]
        public void CanQuery_Simple()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Active = true });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Item>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Active == true)
                        .ToList());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQuery_Null(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Active = true });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var collection = session.Query<Item>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Active != null && x.Active.Value)
                        .ToList();
                    Assert.NotEmpty(collection);
                }
            }
        }
    }
}
