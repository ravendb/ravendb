using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Queries
{
    public class RavenDB_17041 : RavenTestBase
    {
        public RavenDB_17041(ITestOutputHelper output) : base(output)
        {
        }

        private class UserIndex : AbstractIndexCreationTask<User>
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

        internal class User
        {
            public string FirstName;
            public string LastName;
            public List<RoleData> Roles;
        }

        [Fact]
        public async Task Can_Include_Secondary_Level_With_Alias()
        {
            using (var store = GetDocumentStore())
            {
                var userIndex = new UserIndex();
                await userIndex.ExecuteAsync(store);
                using (var session = store.OpenSession())
                {
                    var role1 = new RoleData() {Name = "admin", Role = "role/1" };
                    var role2 = new RoleData() {Name = "developer", Role = "role/2"};
                    
                    var roles = new List<RoleData>() { role1, role2 };
                    var user = new User() {FirstName = "Rhini", LastName = "Hiber", Roles = roles};
                    session.Store(role1, role1.Role);
                    session.Store(role2, role2.Role);
                    session.Store(user);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var users = session.Query<User, UserIndex>()
                        .Include(u => u.Roles.Select(r => r.Role))
                        .Select(u => new {u.FirstName, u.LastName, Roles = u.Roles.Select(r => new {r.Role}).ToList()})
                        .ToList();

                    var loaded = session.Load<RoleData>("role/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(loaded.Role, "role/1");

                    loaded = session.Load<RoleData>("role/2");

                    Assert.Equal(loaded.Role, "role/2");
                }

            }
        }
    }
}
