using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Queries
{
    public class RavenDB_17041 : RavenTestBase
    {
        public RavenDB_17041(ITestOutputHelper output) : base(output)
        {
        }

        private class UserIndex : AbstractIndexCreationTask<User2>
        {
            public UserIndex()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.FirstName,
                                   u.LastName,
                                   u.Roles
                               };
            }
        }


        internal class RoleData
        {
            public string Name;
            public string Role;
        }

        internal class User2
        {
            public string FirstName;
            public string LastName;
            public List<RoleData> Roles;
        }

        [Fact]
        public async Task Can_Include_With_Alias()
        {
            using (var store = GetDocumentStore())
            {
                var userIndex = new UserIndex();
                await userIndex.ExecuteAsync(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var roles = new List<RoleData>() {new RoleData() {Name = "roles/1", Role = "admin"}, new RoleData() {Name = "role/2", Role = "developer"}};
                    var user = new User2() {FirstName = "Rhini", LastName = "Hiber", Roles = roles};
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    var users = session.Query<User2, UserIndex>()
                        .Include(u => u.Roles.Select(r => r.Role))
                        .Select(u => new {u.FirstName, u.LastName, Roles = u.Roles.Select(r => new {r.Role}).ToList()})
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal(2, users[0].Roles.Count);
                    Assert.Equal("admin", users[0].Roles[0].Role);
                    Assert.Equal("developer", users[0].Roles[1].Role);

                }

            }
        }
    }
}
