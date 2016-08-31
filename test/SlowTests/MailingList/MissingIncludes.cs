using System.Linq;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.MailingList
{
    public class MissingIncludes : RavenTestBase
    {
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
                        Parent = "items/2"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Include<Item>(x => x.Parent).Load(1);
                    Assert.Null(session.Load<Item>(2));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void WontGenerateRequestOnMissing_Query()
        {
            using (var store = GetDocumentStore())
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
                    Assert.Null(session.Load<Item>(2));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
