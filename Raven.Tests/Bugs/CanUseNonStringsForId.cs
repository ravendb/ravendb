using System;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class CanUseNonStringsForId : LocalClientTest
    {
        [Fact]
        public void CanStoreAndLoadEntityWithIntKey()
        {
            using(var store = NewDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    s.Store(new UserInt32
                    {
                        Id = 3,
                        Name = "Ayende"
                    });
                    s.SaveChanges();
                }

                using(var s = store.OpenSession())
                {
                    var userInt32 = s.Load<UserInt32>("3");
                    Assert.Equal(3, userInt32.Id);
                    Assert.Equal("Ayende", userInt32.Name);
                }
            }
        }

        [Fact]
        public void CanStoreAndLoadEntityWithGuidKey()
        {
            var id = Guid.NewGuid();
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new UserGuid()
                    {
                        Id = id,
                        Name = "Ayende"
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var user = s.Load<UserGuid>(id.ToString());
                    Assert.Equal(id, user.Id);
                    Assert.Equal("Ayende", user.Name);
                }
            }
        }
    }

    public class UserGuid
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    public class UserInt32
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}