using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6305 : RavenTestBase
    {
        public RavenDB_6305(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryValueObject()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    FullName = new FullName
                    {
                        FirstName = "Jerry",
                        LastName = "Garcia"
                    }
                });
                session.SaveChanges();

                var myFullNameObject = new FullName
                {
                    FirstName = "Jerry",
                    LastName = "Garcia"
                };

                var query = session.Query<User>()
                                   .Where(u => u.FullName == myFullNameObject)
                                   .ToList();

                Assert.Equal(1, query.Count);
            }
        }


        private class User
        {
            public FullName FullName { get; set; }
        }

        private class FullName
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }
    }
}
