using System;
using System.Linq;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class QueriesWithVariables : RavenTestBase
    {
        public QueriesWithVariables(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Query_With_Simple_Constants()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    const string friend = "bob";
                    const int age = 65;
                    var query = from u in session.Query<User>()
                                select new 
                                {
                                    Name = u.Name,
                                    Friend = friend,
                                    Age = age
                                };

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(friend, queryResult[0].Friend);
                    Assert.Equal(age, queryResult[0].Age);

                }
            }
        }

        [Fact]
        public void Query_With_Simple_Constants_IntoClass()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    const string friend = "bob";
                    const int age = 65;
                    var query = from u in session.Query<User>()
                                select new QueryResult
                                {
                                    Name = u.Name,
                                    Friend = friend,
                                    Age = age
                                };

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(friend, queryResult[0].Friend);
                    Assert.Equal(age, queryResult[0].Age);
                }
            }
        }

        [Fact]
        public void Query_With_Variables()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" , FriendId = "users/2" , Birthday = new DateTime(1942, 8, 1)}, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    var friend = session.Load<User>(user1.FriendId).Name;
                    var age = DateTime.Now.Year - user1.Birthday.Year;

                    var query = from u in session.Query<User>()
                                where u.LastName == user1.LastName
                                select new
                                {
                                    Name = u.Name,
                                    Friend = friend,
                                    Age = age
                                };

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(friend, queryResult[0].Friend);
                    Assert.Equal(age, queryResult[0].Age);

                }
            }
        }

        [Fact]
        public void Query_With_Variables_IntoClass()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", FriendId = "users/2", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    var friend = session.Load<User>(user1.FriendId).Name;
                    var age = DateTime.Now.Year - user1.Birthday.Year;

                    var query = from u in session.Query<User>()
                        where u.LastName == user1.LastName
                        select new QueryResult
                        {
                            Name = u.Name,
                            Friend = friend,
                            Age = age
                        };

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(friend, queryResult[0].Friend);
                    Assert.Equal(age, queryResult[0].Age);

                }
            }
        }
    
        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public DateTime Birthday { get; set; }
            public string FriendId { get; set; }
        }

        private class QueryResult
        {
            public string Name { get; set; }
            public string Friend { get; set; }
            public int Age { get; set; }
        }
    }
}

