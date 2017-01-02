using FastTests;
using Raven.Json.Linq;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5968 : RavenTestBase
    {
        [Fact]
        public void EtagNullShouldOverrideDocument()
        {
            using (var store = GetDocumentStore())
            {

                using (var s = store.OpenSession())
                {
                    s.Store(new User {Name = "name1"}, (long?)null,"users/1");
                    s.SaveChanges();
                   
                }
                using (var s = store.OpenSession())
                {
                    s.Store(new User {Name = "name2"}, (long?)null, "users/1");
                    s.SaveChanges();
                }
            }
        }

        [Fact]
        public void EtagNullShouldOverrideDocumentPut()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null, new RavenJObject { { "Name", "Value1" }}, new RavenJObject());
                store.DatabaseCommands.Put("users/1", null, new RavenJObject { { "Name", "Value2" } }, new RavenJObject());
                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>("users/1");
                    Assert.Equal("Value2",user.Name);
                }

            }
        }
    }
}
