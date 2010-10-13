using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class CanGetMetadataForTransient : LocalClientTest
    {
        [Fact]
        public void GetMetadataForTransient()
        {
            using(var store = NewDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    var entity = new User{Name = "Ayende"};
                    s.Store(entity);
                    s.Advanced.GetMetadataFor(entity)["admin"] = true;

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entity = new User{Id = "users/1"};
                    Assert.True(s.Advanced.GetMetadataFor(entity).Value<bool>("admin"));
                }
            }
        }
    }
}