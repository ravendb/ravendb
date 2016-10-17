using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5422 : RavenTest
    {
        [Fact]
        public void ShouldBeAbleToQueryLuceneTokens()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new Users_ByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "OR" });
                    session.Store(new User() { Name = "AND" });
                    session.Store(new User() { Name = "NOT" });
                    session.Store(new User() { Name = "TO" });
                    session.Store(new User() { Name = "INTERSECT" });
                    session.Store(new User() { Name = "NULL" });
                    
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User, Users_ByName>().Search(user => user.Name, "OR").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "AND").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "NOT").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "TO").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "INTERSECT").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "NULL").Single();
                }
            }
        }

        public class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users select new { user.Name };
            }
        }

        public class User
        {
            public string Name { get; set; }
        }
    }
}
