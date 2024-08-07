using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_21557 : RavenTestBase
{
    public RavenDB_21557(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void Can_project_role_names_from_user_without_roles(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            new UserIndex().Execute(store);

            User userWithoutRoles;
            User userWithRole;

            using (var session = store.OpenSession())
            {
                var role = new Role
                {
                    Id = "roles/1", Name = "Admin"
                };
                session.Store(role);

                userWithoutRoles = new User
                {
                    Id = "users/1", Roles = Array.Empty<UserRole>()
                };
                session.Store(userWithoutRoles);

                userWithRole = new User
                {
                    Id = "users/2", Roles = new UserRole[]{ new() { RoleId = role.Id } }
                };
                session.Store(userWithRole);

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                string[] QueryUserRoleNames(string userId)
                {
                    var data = session.Query<UserIndex.Result, UserIndex>()
                        .Where(x => x.Deleted == false && x.Id == userId)
                        .Select(x => new
                        {
                            user = x,
                            roles = RavenQuery.Load<Role>(x.Roles.Select(r => r.RoleId).Distinct())
                        })
                        .Select(x => new
                        {
                            Roles =x.roles.Select(r => r.Name).ToArray()
                        })
                        .SingleOrDefault();

                    return data.Roles;
                }

                var user1RoleNames = QueryUserRoleNames(userWithoutRoles.Id);
                Assert.Empty(user1RoleNames);

                var user2RoleNames = QueryUserRoleNames(userWithRole.Id);

                Assert.Equal(1, user2RoleNames.Length);
                Assert.Equal("Admin", user2RoleNames[0]);
            }
        }
    }

    private class UserIndex : AbstractIndexCreationTask<User, UserIndex.Result>
    {
        public class Result
        {
            public string Id { get; set; }
            public UserRoleResult[] Roles { get; set; }
            public bool Deleted { get; set; }
        }

        public class UserRoleResult
        {
            public string RoleId { get; set; }
        }

        public UserIndex()
        {
            Map = users => from user in users
                select new Result
                {
                    Id = user.Id,
                    Roles = user.Roles
                        .Select(x => new UserRoleResult
                        {
                            RoleId = x.RoleId
                        })
                        .ToArray(),
                    Deleted = false
                };

            StoreAllFields(FieldStorage.Yes);

            Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxStaticIndexComplexFieldIndexingBehavior)] =
                IndexingConfiguration.CoraxComplexFieldIndexingBehavior.Skip.ToString();
        }
    }

    private class Role
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class User
    {
        public string Id { get; set; }
        public UserRole[] Roles { get; set; }
    }

    private class UserRole
    {
        public string RoleId { get; set; }
    }
}
