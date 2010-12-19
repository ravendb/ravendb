using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Bugs.Caching
{
    public class CachingOfDocumentInclude : RemoteClientTest
    {
        [Fact]
        public void Can_cache_document_with_includes()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User {Name = "Ayende"});
                    s.Store(new User { PartnerId = "users/1"});
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x=>x.PartnerId)
                        .Load("users/1");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/1");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                }
            }
        }
    }
}