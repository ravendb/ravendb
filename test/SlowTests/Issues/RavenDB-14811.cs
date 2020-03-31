using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14811 : RavenTestBase
    {
        public RavenDB_14811(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Project_Id_Field_In_Class()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "Grisha",
                    Age = 34
                };

                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionIntId
                        {
                            Name = x.Name
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(0, result.Id);
                    Assert.Equal(user.Name, result.Name);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionIntId
                        {
                            Name = x.Id
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(0, result.Id);
                    Assert.Equal(user.Id, result.Name);
                }
            }
        }

        [Fact]
        public void Can_Project_Id_Field()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "Grisha",
                    Age = 34
                };

                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionIntId
                        {
                            Id = 1,
                            Name = x.Name
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(1, result.Id);
                    Assert.Equal(user.Name, result.Name);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionIntId
                        {
                            Id = x.Age,
                            Name = x.Name
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(user.Age, result.Id);
                    Assert.Equal(user.Name, result.Name);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionStringId
                        {
                            Id = x.Id,
                            Name = x.Name
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(user.Id, result.Id);
                    Assert.Equal(user.Name, result.Name);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(x => new UserProjectionStringId
                        {
                            Id = x.Name,
                            Name = x.Name
                        }).FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(user.Name, result.Id);
                    Assert.Equal(user.Name, result.Name);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }

        private class UserProjectionIntId
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private class UserProjectionStringId
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
