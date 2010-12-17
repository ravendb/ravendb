using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
    public class LastModifiedRemote : RemoteClientTest
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using(GetNewServer())
            using (var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(session.Advanced.GetMetadataFor(user)["Last-Modified"]);
                }
            }
        }
    }
}