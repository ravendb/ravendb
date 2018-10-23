using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11900 : RavenTestBase
    {
        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        Name = user.Name,
                        SchoolId = user.School.Id
                    };
            }
        }

        private class UsersIndex2 : AbstractIndexCreationTask<User>
        {
            public UsersIndex2()
            {
                Map = users => from user in users
                    select new
                    {
                        Name = user.Name,
                        School_Id = user.School.Id
                    };

                Store("School_Id", FieldStorage.Yes);
            }
        }

        private class UsersIndex3 : AbstractIndexCreationTask<User>
        {
            public UsersIndex3()
            {
                Map = users => from user in users
                    select new
                    {
                        UserName = user.Name,
                        UserSchoolId = user.School.Id
                    };

                Store("UserSchoolId", FieldStorage.Yes);

            }
        }

        private class User
        {
            public string Name { get; set; }
            public Reference School { get; set; }
        }

        private class Reference
        {
            public string Id { get; set; }
        }

        [Fact]
        public void CanUseFindProjectedPropertyNameForIndexToGetDotNotationInProjections()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindProjectedPropertyNameForIndex = (indexedType, indexedName, path, prop) => path + prop
            }))
            {
                new UsersIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        School = new Reference
                        {
                            Id = "schools/1"
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersIndex>()
                        .Select(u => u.School.Id);

                    Assert.Equal("from index 'UsersIndex' select School.Id", query.ToString());

                    var items = query.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("schools/1", items[0]);
                }

            }
        }

        [Fact]
        public void WhenFindProjectedPropertyNameForIndexIsNullShouldFallbackToFindPropertyNameForIndex()
        {
            using (var store = GetDocumentStore())
            {
                new UsersIndex2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        School = new Reference
                        {
                            Id = "schools/1"
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersIndex2>()
                        .Select(u => u.School.Id);

                    Assert.Equal("from index 'UsersIndex2' select School_Id", query.ToString());

                    var items = query.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("schools/1", items[0]);
                }

            }
        }

        [Fact]
        public void CanUseDifferentConventionsPerIndex()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.FindProjectedPropertyNameForIndex = (indexedType, indexedName, path, prop) =>
                    {
                        switch (indexedName)
                        {
                            case "UsersIndex3":
                            {
                                if (path + prop == "School.Id")
                                    return "UserSchoolId";
                                goto default;
                            }
                            case "UsersIndex":
                                return path + prop;
                            default:
                                return DefaultConvention(path, prop);
                        }
                    };
                    
                    s.Conventions.FindPropertyNameForIndex = (indexedType, indexedName, path, prop) =>
                    {
                        if (indexedName == "UsersIndex3")
                        {
                            return path + prop == "School.Id"
                                ? "UserSchoolId"
                                : "UserName";
                        }

                        return DefaultConvention(path, prop);
                    };
                }
            }))
            {
                new UsersIndex().Execute(store);
                new UsersIndex2().Execute(store);
                new UsersIndex3().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        School = new Reference
                        {
                            Id = "schools/1"
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersIndex3>()
                        .Where(u => u.Name != null)
                        .Select(u => new
                        {
                            u.Name,
                            SchoolId = u.School.Id
                        });
                    Assert.Equal("from index 'UsersIndex3' where UserName != $p0 select Name, UserSchoolId as SchoolId", query.ToString());

                    var items = query.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("schools/1", items[0].SchoolId);

                    query = session.Query<User, UsersIndex2>()
                        .Where(u => u.Name != null)
                        .Select(u => new
                        {
                            u.Name,
                            SchoolId = u.School.Id
                        });
                    Assert.Equal("from index 'UsersIndex2' where Name != $p0 select Name, School_Id as SchoolId", query.ToString());

                    items = query.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("schools/1", items[0].SchoolId);

                    query = session.Query<User, UsersIndex>()
                        .Where(u => u.Name != null)
                        .Select(u => new
                        {
                            u.Name,
                            SchoolId = u.School.Id
                        });

                    Assert.Equal("from index 'UsersIndex' where Name != $p0 select Name, School.Id as SchoolId", query.ToString());

                    items = query.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("schools/1", items[0].SchoolId);
                }

            }
        }

        private static string DefaultConvention(string path, string prop)
        {
            return (path + prop).Replace("[].", "_").Replace(".", "_");
        }

    }
}
