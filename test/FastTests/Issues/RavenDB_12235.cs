using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12235 : RavenTestBase
    {
        [Fact]
        public void ShouldBeAbleToMoveBetweenDocumentQueryToIRavenQueryable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var docQuery = session.Advanced.DocumentQuery<User>();
                    var q = docQuery.ToQueryable();
                    q = q.Where(x => x.Name == "123");
                    q = q.Search(x => x.Age, "123");

                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) select id() as Id, Name, LastName, AddressId, Count, Age", q.ToString());
                }

                using (var asyncSession = store.OpenAsyncSession())
                {
                    var docQuery = asyncSession.Advanced.AsyncDocumentQuery<User>();
                    var q = docQuery.ToQueryable();
                    q = q.Where(x => x.Name == "123");
                    q = q.Search(x => x.Age, "123");

                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) select id() as Id, Name, LastName, AddressId, Count, Age", q.ToString());
                }
            }

        }
    }
}
