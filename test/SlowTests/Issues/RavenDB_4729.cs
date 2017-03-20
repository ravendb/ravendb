using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4729 : RavenTestBase
    {
        [Fact]
        public void CanSaveModifyEntityThatHasUntrackedProperties()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new
                    {
                        FirstName = "Nick",
                        LastName = (string)null
                    });
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<TestUser>("users/1");
                    user.FirstName = "Bob";
                    session.SaveChanges();
                }
            }
        }

        private class TestUser
        {
            public string FirstName { get; set; }
        }
    }
}
