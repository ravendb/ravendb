using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12314 : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Role
        {
            public string Id { get; set; }

            public List<string> Users { get; set; } = new List<string>();
        }

        private class Index : AbstractMultiMapIndexCreationTask<Index.Entry>
        {
            public Index()
            {
                AddMap<User>(users =>
                    from user in users
                    select new Entry
                    {
                        User = user.Id,
                        Roles = new List<string>()
                    }
                );

                AddMap<Role>(roles =>
                    from role in roles
                    from user in role.Users
                    select new Entry
                    {
                        User = user,
                        Roles = new List<string> { role.Id }
                    }
                );

                Reduce = results =>
                    from result in results
                    group result by result.User into g
                    select new Entry
                    {
                        User = g.Key,
                        Roles = g.SelectMany(x => x.Roles).Distinct().ToList()
                    };

                StoreAllFields(FieldStorage.Yes);
            }

            public class Entry
            {
                public string User { get; set; }
                public List<string> Roles { get; set; }
            }
        }

        private class Projection
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public List<Role> Roles { get; set; }

            public static IRavenQueryable<Projection> TransformFrom(IRavenQueryable<Index.Entry> query)
            {
                return
                    from item in query
                    let user = RavenQuery.Load<User>(item.User)
                    let roles = RavenQuery.Load<Role>(item.Roles)
                    select new Projection
                    {
                        Id = user.Id,
                        Name = user.Name,
                        Roles = roles.ToList()
                    };
            }
        }

        [Fact]
        public void CanLoadEmptyStoredFieldsInProjections()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Index());

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Test User"
                    });

                    session.Store(new Role());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = Projection.TransformFrom(session.Query<Index.Entry, Index>()).FirstOrDefault();
                    Assert.NotNull(result);
                    Assert.Empty(result.Roles);
                }
            }
        }
    }
}
