using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class QueryNotAny : RavenTestBase
    {
        public QueryNotAny(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Query_NotAny_WithEnumComparison()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByRoles().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = 1.ToString(), Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.Developer } } });
                    session.Store(new User { Id = 2.ToString(), Roles = new List<Role> { new Role { Type = UserType.Permanent }, new Role { Type = UserType.Developer } } });
                    session.Store(new User { Id = 3.ToString(), Roles = new List<Role> { new Role { Type = UserType.SeniorDeveloper }, new Role { Type = UserType.Manager } } });
                    session.Store(new User { Id = 4.ToString(), Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.SeniorDeveloper } } });
                    session.Store(new User { Id = 5.ToString(), Roles = new List<Role> { new Role { Type = UserType.Permanent }, new Role { Type = UserType.Manager } } });
                    session.Store(new User { Id = 6.ToString(), Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.Developer } } });
                    session.SaveChanges();

                    var nonContractEmployees =
                        session.Query<Users_ByRoles.Result, Users_ByRoles>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.RoleType != UserType.Contract)
                            .As<User>()
                            .ToList();

                    Assert.Equal(3, nonContractEmployees.Count());
                }
            }
        }

        private class Users_ByRoles : AbstractIndexCreationTask<User, Users_ByRoles.Result>
        {
            public class Result
            {
                public UserType RoleType { get; set; }
            }

            public Users_ByRoles()
            {
                Map = users =>
                      from user in users
                      select new
                      {
                          RoleType = user.Roles.Select(x => x.Type)
                      };
            }
        }

        private class User
        {
            public string Id { get; set; }
            public List<Role> Roles { get; set; }
        }

        private class Role
        {
            public UserType Type { get; set; }
        }

        private enum UserType
        {
            Manager,
            Permanent,
            Contract,
            Developer,
            SeniorDeveloper
        }
    }
}
