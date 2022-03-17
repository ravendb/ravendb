using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13442 : RavenTestBase
    {
        public RavenDB_13442(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_project_with_two_loads_when_2nd_load_argument_defined_in_let_between_the_loads()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var session = store.OpenSession())
                {
                    const string organizationParameter = "organizations/1";

                    var projection =
                        from membership in session.Query<MembershipIndexEntry, MembershipIndex>()
                        let userGroups = RavenQuery.Load<UserGroup>(membership.UserGroups)
                        let organizationId = organizationParameter
                        let organization = RavenQuery.Load<Organization>(organizationId)
                        select new Projection
                        {
                            Organization = organization.Id
                        };

                    Assert.Contains("organization = load(organizationId)", projection.ToString());

                    var results = projection.ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(organizationParameter, results[0].Organization);
                }

            }
        }


        private void Setup(IDocumentStore store)
        {
            store.ExecuteIndex(new MembershipIndex());

            using (var session = store.OpenSession())
            {
                session.Store(new UserGroup
                {
                    Id = "usergroups/1",
                    Organization = "organizations/1",
                    Users = new List<string>
                    {
                        "users/1"
                    }
                });

                session.Store(new User
                {
                    Id = "users/1",
                    Organizations = new List<string>
                    {
                        "organizations/1"
                    }
                });

                session.Store(new Organization
                {
                    Id = "organizations/1"
                });

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        private class User
        {
            public string Id { get; set; }
            public List<string> Organizations { get; set; }
        }

        private class UserGroup
        {
            public string Id { get; set; }
            public string Organization { get; set; }
            public List<string> Users { get; set; }
        }

        private class Organization
        {
            public string Id { get; set; }
        }

        private class Projection
        {
            public string Id { get; set; }
            public string Organization { get; set; }
            public List<string> UserGroups { get; set; }
        }

        private class MembershipIndex : AbstractMultiMapIndexCreationTask<MembershipIndexEntry>
        {
            public MembershipIndex()
            {
                AddMap<User>(users =>
                    from user in users
                    select new MembershipIndexEntry
                    {
                        Id = user.Id,
                        Organizations = user.Organizations,
                        UserGroups = new List<string>()
                    }
                );

                AddMap<UserGroup>(userGroups =>
                    from userGroup in userGroups
                    from user in userGroup.Users
                    select new MembershipIndexEntry
                    {
                        Id = user,
                        Organizations = new List<string>(),
                        UserGroups = new List<string>
                        {
                            userGroup.Id
                        }
                    }
                );

                Reduce = entries =>
                    from entry in entries
                    group entry by entry.Id
                    into grouping
                    select new MembershipIndexEntry
                    {
                        Id = grouping.Key,
                        Organizations = grouping.SelectMany(x => x.Organizations).ToList(),
                        UserGroups = grouping.SelectMany(x => x.UserGroups).ToList()
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class MembershipIndexEntry
        {
            public string Id { get; set; }
            public List<string> Organizations { get; set; }
            public List<string> UserGroups { get; set; }
        }
    }
}

