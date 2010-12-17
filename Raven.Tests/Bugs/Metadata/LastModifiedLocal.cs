using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
    public class LastModifiedLocal : LocalClientTest
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using(var store = NewDocumentStore())
            {
                using(var session = store.OpenSession())
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