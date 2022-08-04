using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class MissingIncludes : RavenTestBase
    {
        public MissingIncludes(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
#pragma warning disable 414
            public string Parent;
#pragma warning restore 414
        }

        [Fact]
        public void WontGenerateRequestOnMissing_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Parent = "items/2-A"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Include<Item>(x => x.Parent).Load("items/1-A");
                    Assert.Null(session.Load<Item>("items/2-A"));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WontGenerateRequestOnMissing_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Parent = "items/2"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<Item>().Include(x => x.Parent).Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Null(session.Load<Item>("items/2"));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
