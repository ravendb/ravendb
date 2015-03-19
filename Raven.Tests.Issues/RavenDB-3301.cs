using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3301 : RavenTestBase
    {
        private class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        [Fact]

        public void MaintainChangesToJsonRequestFactory()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                using (var session = store.OpenSession())
                {

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new Item {Name = "item"});
                    }
                    session.SaveChanges();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(2, store.JsonRequestFactory.CurrentCacheSize);
                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 3000;
                    for (int i = 1; i < 20; i++)
                    {
                        session.Load<Item>(i);
                    }
                    Assert.Equal(19, session.Advanced.NumberOfRequests);
                    Assert.Equal(21, store.JsonRequestFactory.CurrentCacheSize);
                }

                store.MaxNumberOfCachedRequests = 3;
                
                Assert.Equal(3, store.MaxNumberOfCachedRequests);
                Assert.Equal(0, store.JsonRequestFactory.CurrentCacheSize);

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i < 20; i++)
                    {
                        session.Load<Item>(i);    
                    }
                    
                    Assert.Equal(19, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, store.JsonRequestFactory.CurrentCacheSize);

                    session.Load<Item>(1);
                    session.Load<Item>(2);
                    session.Load<Item>(3);
                    session.Load<Item>(4);
                    
                    Assert.Equal(19, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, store.JsonRequestFactory.CurrentCacheSize);

                }
                store.MaxNumberOfCachedRequests = 3;
                Assert.Equal(3, store.JsonRequestFactory.CurrentCacheSize);
            }
        }
    }
}
