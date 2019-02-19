using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class DeletingDynamics : RavenTestBase
    {
        [Fact]
        public void CanDeleteItemsUsingDynamic()
        {
            using (var store = GetDocumentStore())
            {
                store.Commands().Put("users/1", null, new object());
                store.Commands().Put("users/2", null, new object());

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<dynamic>("users/1");
                    var user2 = session.Load<dynamic>("users/2");

                    session.Delete(user1);
                    session.Delete(user2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<dynamic>("users/1");
                    var user2 = session.Load<dynamic>("users/2");

                    Assert.Null(user1);
                    Assert.Null(user2);
                }
            }
        }
    }
}
