using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Linq;
using Xunit;

namespace FastTests.Client
{
    public class QueriesWithCustomFunctions : RavenTestBase
    {
        [Fact]
        public void Can_Define_Custom_Functions_Inside_Select()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Name == "Jerry")
                        .Select(u => new { FullName = u.Name + " " + u.LastName, FirstName = u.Name });

                    Assert.Equal("FROM Users as u WHERE Name = $p0 SELECT { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Jerry", queryResult[0].FirstName);
                }
            }
        }

        [Fact]
        public async Task Can_Define_Custom_Functions_Inside_Select_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Name == "Jerry")
                        .Select(u => new { FullName = u.Name + " " + u.LastName, FirstName = u.Name });

                    Assert.Equal("FROM Users as u WHERE Name = $p0 SELECT { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Jerry", queryResult[0].FirstName);

                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Timespan()
        {
            using (var store = GetDocumentStore())
            {                                    
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { u.Name, Age = DateTime.Today - u.Birthday});

                    Assert.Equal("FROM Users as u SELECT { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date().setHours(0,0,0,0)-new Date(Date.parse(u.Birthday))) }",
                                query.ToString());

                    var queryResult = query.ToList();

                    var ts = DateTime.Today - new DateTime(1942, 8, 1);

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(ts, queryResult[0].Age);
                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Timespan_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { u.Name, Age = DateTime.Today - u.Birthday });

                    Assert.Equal("FROM Users as u SELECT { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date().setHours(0,0,0,0)-new Date(Date.parse(u.Birthday))) }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    var ts = DateTime.Today - new DateTime(1942, 8, 1);

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(ts, queryResult[0].Age);
                }
            }
        }

        [Fact]
        public void Custom_Functions_With_DateTime_Properties()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new {
                            DayOfBirth = u.Birthday.Day,
                            MonthOfBirth = u.Birthday.Month,
                            Age = DateTime.Today.Year - u.Birthday.Year
                        });


                    Assert.Equal("FROM Users as u SELECT { DayOfBirth : new Date(Date.parse(u.Birthday)).getDate(), MonthOfBirth : new Date(Date.parse(u.Birthday)).getMonth()+1, Age : new Date().getFullYear()-new Date(Date.parse(u.Birthday)).getFullYear() }"
                        , query.ToString());

                    var queryResult = query.ToList();

                    var birthday = new DateTime(1942, 8, 1);
                    var age = DateTime.Today.Year - birthday.Year;

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(birthday.Day, queryResult[0].DayOfBirth);
                    Assert.Equal(birthday.Month, queryResult[0].MonthOfBirth);
                    Assert.Equal(age, queryResult[0].Age);

                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_DateTime_Properties_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new {
                            DayOfBirth = u.Birthday.Day,
                            MonthOfBirth = u.Birthday.Month,
                            Age = DateTime.Today.Year - u.Birthday.Year
                        });


                    Assert.Equal("FROM Users as u SELECT { DayOfBirth : new Date(Date.parse(u.Birthday)).getDate(), MonthOfBirth : new Date(Date.parse(u.Birthday)).getMonth()+1, Age : new Date().getFullYear()-new Date(Date.parse(u.Birthday)).getFullYear() }"
                        , query.ToString());

                    var queryResult = await query.ToListAsync();

                    var birthday = new DateTime(1942, 8, 1);
                    var age = DateTime.Today.Year - birthday.Year;

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(birthday.Day, queryResult[0].DayOfBirth);
                    Assert.Equal(birthday.Month, queryResult[0].MonthOfBirth);
                    Assert.Equal(age, queryResult[0].Age);

                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Numbers_And_Booleans()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
                        Birthday = new DateTime(1942, 8, 1),
                        IdNumber = 32588734,
                        IsActive = true
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { LuckyNumber = u.IdNumber / u.Birthday.Year, Active = u.IsActive ? "yes" : "no" });

                    Assert.Equal("FROM Users as u SELECT { LuckyNumber : u.IdNumber/new Date(Date.parse(u.Birthday)).getFullYear(), Active : u.IsActive?\"yes\":\"no\" }",
                                query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(32588734 / 1942, queryResult[0].LuckyNumber);
                    Assert.Equal("yes", queryResult[0].Active);
                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Numbers_And_Booleans_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
                        Birthday = new DateTime(1942, 8, 1),
                        IdNumber = 32588734,
                        IsActive = true
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { LuckyNumber = u.IdNumber / u.Birthday.Year, Active = u.IsActive ? "yes" : "no" });

                    Assert.Equal("FROM Users as u SELECT { LuckyNumber : u.IdNumber/new Date(Date.parse(u.Birthday)).getFullYear(), Active : u.IsActive?\"yes\":\"no\" }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(32588734 / 1942, queryResult[0].LuckyNumber);
                    Assert.Equal("yes", queryResult[0].Active);
                }
            }
        }

        [Fact]
        public void Custom_Functions_Inside_Select_Nested()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Roles = new[] { "Musician", "Song Writer" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new {
                            Roles = u.Roles.Select(r => new
                            {
                                RoleName = r + "!"
                            })
                        });

                    Assert.Equal("FROM Users as u SELECT { Roles : u.Roles.map(function(r){return {RoleName:r+\"!\"};}) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    var roles = queryResult[0].Roles.ToList();

                    Assert.Equal(2 , roles.Count);
                    Assert.Equal("Musician!", roles[0].RoleName);
                    Assert.Equal("Song Writer!", roles[1].RoleName);

                }
            }
        }

        [Fact]
        public async Task Custom_Functions_Inside_Select_Nested_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", Roles = new[] { "Musician", "Song Writer" } }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new {
                            Roles = u.Roles.Select(r => new
                            {
                                RoleName = r + "!"
                            })
                        });

                    Assert.Equal("FROM Users as u SELECT { Roles : u.Roles.map(function(r){return {RoleName:r+\"!\"};}) }", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);

                    var roles = queryResult[0].Roles.ToList();

                    Assert.Equal(2, roles.Count);
                    Assert.Equal("Musician!", roles[0].RoleName);
                    Assert.Equal("Song Writer!", roles[1].RoleName);

                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Simple_Let()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let lastName = u.LastName
                                select new
                                {
                                    FullName = u.Name + " " + lastName
                                };

                    Assert.Equal(
@"DECLARE function output(u) {
	var lastName = u.LastName;
	return { FullName : u.Name+"" ""+lastName };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Simple_Let_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                        let lastName = u.LastName
                        select new
                        {
                            FullName = u.Name + " " + lastName
                        };

                    Assert.Equal(
                        @"DECLARE function output(u) {
	var lastName = u.LastName;
	return { FullName : u.Name+"" ""+lastName };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Let()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let format = (Func<User, string>)(p => p.Name + " " + p.LastName)
                                select new
                                {
                                    FullName = format(u)
                                };

                    Assert.Equal(
 @"DECLARE function output(u) {
	var format = function(p){return p.Name+"" ""+p.LastName;};
	return { FullName : format(u) };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Let_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                        let format = (Func<User, string>)(p => p.Name + " " + p.LastName)
                        select new
                        {
                            FullName = format(u)
                        };

                    Assert.Equal(
                        @"DECLARE function output(u) {
	var format = function(p){return p.Name+"" ""+p.LastName;};
	return { FullName : format(u) };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Multiple_Lets()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let space = " "
                                let last = u.LastName
                                let format = (Func<User, string>)(p => p.Name + space + last)
                                select new
                                {
                                    FullName = format(u)
                                };

                    Assert.Equal(
@"DECLARE function output(u) {
	var space = "" "";
	var last = u.LastName;
	var format = function(p){return p.Name+space+last;};
	return { FullName : format(u) };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Multiple_Lets_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                        let space = " "
                        let last = u.LastName
                        let format = (Func<User, string>)(p => p.Name + space + last)
                        select new
                        {
                            FullName = format(u)
                        };

                    Assert.Equal(
                        @"DECLARE function output(u) {
	var space = "" "";
	var last = u.LastName;
	var format = function(p){return p.Name+space+last;};
	return { FullName : format(u) };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Should_Throw_When_Let_Is_Before_Where()
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
                    var query = from u in session.Query<User>()
                        let last = u.LastName
                        where u.Name == "Jerry"
                        select new
                        {
                            LastName = last
                        };

                    Assert.Throws<NotSupportedException>(() => query.ToList());
                }
            }
        }

        [Fact]
        public async Task Should_Throw_When_Let_Is_Before_Where_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                        let last = u.LastName
                        where u.Name == "Jerry"
                        select new
                        {
                            LastName = last
                        };

                    await Assert.ThrowsAsync<NotSupportedException>(async () => await query.ToListAsync());
                }
            }
        }

        [Fact]
        public void Custom_Function_With_Where_and_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 12345 }, "detail/1");
                    session.Store(new Detail { Number = 67890 }, "detail/2");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name != "Bob"
                                let detail = session.Load<Detail>(u.DetailId)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Detail = detail.Number
                                };

                    Assert.Equal("FROM Users as u WHERE Name != $p0 LOAD u.DetailId as detail SELECT { FullName : u.Name+\" \"+u.LastName, Detail : detail.Number }",
                                 query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].Detail);
                }
            }
        }

        [Fact]
        public async Task Custom_Function_With_Where_and_Load_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 12345 }, "detail/1");
                    await session.StoreAsync(new Detail { Number = 67890 }, "detail/2");

                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var asyncSession = store.OpenAsyncSession())
                {
                    var query = from u in asyncSession.Query<User>()
                        where u.Name != "Bob"
                        let detail = RavenQuery.Load<Detail>(u.DetailId)
                        select new
                        {
                            FullName = u.Name + " " + u.LastName,
                            Detail = detail.Number
                        };

                    Assert.Equal(@"FROM Users as u WHERE Name != $p0 LOAD u.DetailId as detail SELECT { FullName : u.Name+"" ""+u.LastName, Detail : detail.Number }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].Detail);
                }
            }
        }

        [Fact]
        public void Custom_Function_With_Multiple_Loads()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 12345 }, "detail/1");
                    session.Store(new Detail { Number = 67890 }, "detail/2");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1", FriendId = "users/2" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2", FriendId = "users/1" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        where u.Name != "Bob"
                        let format = (Func<User, string>)(user => user.Name + " " + user.LastName)
                        let detail = session.Load<Detail>(u.DetailId)
                        let friend = session.Load<User>(u.FriendId)
                        select new
                        {
                            FullName = format(u),
                            Friend = format(friend),
                            Detail = detail.Number
                        };

                    Assert.Equal(
@"DECLARE function output(u, detail, friend) {
	var format = function(user){return user.Name+"" ""+user.LastName;};
	return { FullName : format(u), Friend : format(friend), Detail : detail.Number };
}
FROM Users as u WHERE Name != $p0 LOAD u.DetailId as detail, u.FriendId as friend SELECT output(u, detail, friend)",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[0].Friend);
                    Assert.Equal(12345, queryResult[0].Detail);
                }
            }
        }

        [Fact]
        public async Task Custom_Function_With_Multiple_Loads_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 12345 }, "detail/1");
                    await session.StoreAsync(new Detail { Number = 67890 }, "detail/2");

                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1", FriendId = "users/2" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2", FriendId = "users/1" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name != "Bob"
                                let format = (Func<User, string>)(user => user.Name + " " + user.LastName)
                                let detail = RavenQuery.Load<Detail>(u.DetailId)
                                let friend = RavenQuery.Load<User>(u.FriendId)
                                select new
                                {
                                    FullName = format(u),
                                    Friend = format(friend),
                                    Detail = detail.Number
                                };

                    Assert.Equal(
@"DECLARE function output(u, detail, friend) {
	var format = function(user){return user.Name+"" ""+user.LastName;};
	return { FullName : format(u), Friend : format(friend), Detail : detail.Number };
}
FROM Users as u WHERE Name != $p0 LOAD u.DetailId as detail, u.FriendId as friend SELECT output(u, detail, friend)",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[0].Friend);
                    Assert.Equal(12345, queryResult[0].Detail);
                }
            }
        }

        [Fact]
        public void Custom_Fuctions_With_Let_And_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 12345 }, "detail/1");
                    session.Store(new Detail { Number = 67890 }, "detail/2");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let format = (Func<User, string>)(user => user.Name + " " + u.LastName)
                                let detail = session.Load<Detail>(u.DetailId)
                                select new
                                {
                                    FullName = format(u),
                                    DetailNumber = detail.Number
                                };

                    Assert.Equal(
@"DECLARE function output(u, detail) {
	var format = function(user){return user.Name+"" ""+u.LastName;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
FROM Users as u LOAD u.DetailId as detail SELECT output(u, detail)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].DetailNumber);

                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                    Assert.Equal(67890, queryResult[1].DetailNumber);

                }
            }
        }

        [Fact]
        public async Task Custom_Fuctions_With_Let_And_Load_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 12345 }, "detail/1");
                    await session.StoreAsync(new Detail { Number = 67890 }, "detail/2");

                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                                let format = (Func<User, string>)(user => user.Name + " " + u.LastName)
                                let detail = RavenQuery.Load<Detail>(u.DetailId)
                                select new
                                {
                                    FullName = format(u),
                                    DetailNumber = detail.Number
                                };

                    Assert.Equal(
@"DECLARE function output(u, detail) {
	var format = function(user){return user.Name+"" ""+u.LastName;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
FROM Users as u LOAD u.DetailId as detail SELECT output(u, detail)", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].DetailNumber);

                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                    Assert.Equal(67890, queryResult[1].DetailNumber);

                }
            }
        }

        [Fact]
        public void Custom_Function_With_Where_and_Load_Array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");
                    session.Store(new Detail { Number = 3 }, "details/3");
                    session.Store(new Detail { Number = 4 }, "details/4");


                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new[] { "details/1", "details/2" } }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailIds = new[] { "details/3", "details/4" } }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        where u.Name != "Bob"
                        let details = RavenQuery.Load<Detail>(u.DetailIds)
                        select new
                        {
                            FullName = u.Name + " " + u.LastName,
                            Details = details
                        };

                    Assert.Equal(@"FROM Users as u WHERE Name != $p0 LOAD u.DetailIds as details[] SELECT { FullName : u.Name+"" ""+u.LastName, Details : details }",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);

                    var detailList = queryResult[0].Details.ToList();

                    Assert.Equal(2, detailList.Count);
                    Assert.Equal(1, detailList[0].Number);
                    Assert.Equal(2, detailList[1].Number);
                }
            }
        }

        [Fact]
        public async Task Custom_Function_With_Where_and_Load_Array_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 1 }, "details/1");
                    await session.StoreAsync(new Detail { Number = 2 }, "details/2");
                    await session.StoreAsync(new Detail { Number = 3 }, "details/3");
                    await session.StoreAsync(new Detail { Number = 4 }, "details/4");


                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new[] { "details/1", "details/2" } }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailIds = new[] { "details/3", "details/4" } }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name != "Bob"
                                let details = RavenQuery.Load<Detail>(u.DetailIds)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Details = details
                                };

                    Assert.Equal(@"FROM Users as u WHERE Name != $p0 LOAD u.DetailIds as details[] SELECT { FullName : u.Name+"" ""+u.LastName, Details : details }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);

                    var detailList = queryResult[0].Details.ToList();

                    Assert.Equal(2, detailList.Count);
                    Assert.Equal(1, detailList[0].Number);
                    Assert.Equal(2, detailList[1].Number);
                }
            }
        }

        [Fact]
        public void Custom_Function_With_Where_and_Load_List()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");
                    session.Store(new Detail { Number = 3 }, "details/3");
                    session.Store(new Detail { Number = 4 }, "details/4");


                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new List<string>{ "details/1", "details/2" } }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailIds = new List<string> { "details/3", "details/4" } }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name != "Bob"
                                let details = RavenQuery.Load<Detail>(u.DetailIds)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Details = details
                                };

                    Assert.Equal(@"FROM Users as u WHERE Name != $p0 LOAD u.DetailIds as details[] SELECT { FullName : u.Name+"" ""+u.LastName, Details : details }",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);

                    var detailList = queryResult[0].Details.ToList();

                    Assert.Equal(2, detailList.Count);
                    Assert.Equal(1, detailList[0].Number);
                    Assert.Equal(2, detailList[1].Number);
                }
            }
        }

        [Fact]
        public async Task Custom_Function_With_Where_and_Load_List_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 1 }, "details/1");
                    await session.StoreAsync(new Detail { Number = 2 }, "details/2");
                    await session.StoreAsync(new Detail { Number = 3 }, "details/3");
                    await session.StoreAsync(new Detail { Number = 4 }, "details/4");


                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new[] { "details/1", "details/2" } }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailIds = new[] { "details/3", "details/4" } }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name != "Bob"
                                let details = RavenQuery.Load<Detail>(u.DetailIds)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Details = details
                                };

                    Assert.Equal(@"FROM Users as u WHERE Name != $p0 LOAD u.DetailIds as details[] SELECT { FullName : u.Name+"" ""+u.LastName, Details : details }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);

                    var detailList = queryResult[0].Details.ToList();

                    Assert.Equal(2, detailList.Count);
                    Assert.Equal(1, detailList[0].Number);
                    Assert.Equal(2, detailList[1].Number);
                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Multiple_Where_And_Let()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 12345 }, "detail/1");
                    session.Store(new Detail { Number = 67890 }, "detail/2");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name == "Jerry"
                                where u.IsActive == false
                                orderby u.LastName descending
                                let last = u.LastName
                                let format = (Func<User, string>)(user => user.Name + " " + last)
                                let detail = session.Load<Detail>(u.DetailId)
                                select new
                                {
                                    FullName = format(u),
                                    DetailNumber = detail.Number
                                };

                    Assert.Equal(
@"DECLARE function output(u, detail) {
	var last = u.LastName;
	var format = function(user){return user.Name+"" ""+last;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
FROM Users as u WHERE (Name = $p0) AND (IsActive = $p1) ORDER BY LastName DESC LOAD u.DetailId as detail SELECT output(u, detail)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].DetailNumber);

                }
            }
        }

        [Fact]
        public async Task Custom_Functions_With_Multiple_Where_And_Let_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Detail { Number = 12345 }, "detail/1");
                    await session.StoreAsync(new Detail { Number = 67890 }, "detail/2");

                    await session.StoreAsync(new User { Name = "Jerry", LastName = "Garcia", DetailId = "detail/1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Bob", LastName = "Weir", DetailId = "detail/2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from u in session.Query<User>()
                                where u.Name == "Jerry"
                                where u.IsActive == false
                                orderby u.LastName descending
                                let last = u.LastName
                                let format = (Func<User, string>)(user => user.Name + " " + last)
                                let detail = RavenQuery.Load<Detail>(u.DetailId)
                                select new
                                {
                                    FullName = format(u),
                                    DetailNumber = detail.Number
                                };

                    Assert.Equal(
@"DECLARE function output(u, detail) {
	var last = u.LastName;
	var format = function(user){return user.Name+"" ""+last;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
FROM Users as u WHERE (Name = $p0) AND (IsActive = $p1) ORDER BY LastName DESC LOAD u.DetailId as detail SELECT output(u, detail)", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal(12345, queryResult[0].DetailNumber);

                }
            }
        }

        [Fact]
        public void Custom_Functions_Math_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" , IdNumber = 7}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    Pow = Math.Pow(u.IdNumber, u.IdNumber),
                                    Max = Math.Max(u.IdNumber + 1, u.IdNumber)
                                };

                    Assert.Equal("FROM Users as u SELECT { Pow : Math.pow(u.IdNumber, u.IdNumber), Max : Math.max((u.IdNumber+1), u.IdNumber) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    Assert.Equal(8, queryResult[0].Max);
                    Assert.Equal(823543, queryResult[0].Pow);
                }
            }
        }

        [Fact]
        public void Can_Project_Into_Class()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new QueryResult
                                {
                                    FullName = user.Name + " " + user.LastName
                                };

                    Assert.Equal("FROM Users as user SELECT { FullName : user.Name+\" \"+user.LastName }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Can_Project_Into_Class_With_Let()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let first = user.Name
                                let last = user.LastName
                                let format = (Func<string>)(() => first + " " + last)
                                select new QueryResult
                                {
                                    FullName = format()
                                };

                    Assert.Equal(
@"DECLARE function output(user) {
	var first = user.Name;
	var last = user.LastName;
	var format = function(){return first+"" ""+last;};
	return { FullName : format() };
}
FROM Users as user SELECT output(user)", query.ToString());


                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Custom_Functions_With_DateTime_Object()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        let date = new DateTime(1960, 1, 1)
                        select new
                        {
                            Bday = u.Birthday,
                            Date = date
                        };

                    Assert.Equal(
                        @"DECLARE function output(u) {
	var date = new Date(1960, 0, 1);
	return { Bday : new Date(Date.parse(u.Birthday)), Date : date };
}
FROM Users as u SELECT output(u)", query.ToString());


                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(new DateTime(1942, 8, 1), queryResult[0].Bday);
                    Assert.Equal(new DateTime(1960, 1, 1), queryResult[0].Date);


                }
            }
        }

        [Fact]
        public void Custom_Functions_With_Escape_Hatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Birthday = new DateTime(1942, 8, 1) }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                        select new
                        {
                            Date = RavenQuery.Raw<DateTime>("new Date(Date.parse(user.Birthday))"),
                            Name = RavenQuery.Raw<string>("user.Name.substr(0,3)"),
                        };

                    Assert.Equal("FROM Users as user SELECT { Date : new Date(Date.parse(user.Birthday)), Name : user.Name.substr(0,3) }",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(new DateTime(1942, 8, 1), queryResult[0].Date);
                    Assert.Equal("Jer", queryResult[0].Name);

                }
            }
        }

        [Fact]
        public void Custom_Functions_Escape_Hatch_With_Path()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia"}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Name == "Jerry")
                        .Select(a => new
                        {
                            Name = RavenQuery.Raw<string>(a.Name, "substr(0,3)")
                        });

                    Assert.Equal("FROM Users as a WHERE Name = $p0 SELECT { Name : a.Name.substr(0,3) }",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jer", queryResult[0].Name);

                }
            }
        }

        [Fact]
        public void Custom_Function_With_Complex_Loads()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");
                    session.Store(new Detail { Number = 3 }, "details/3");
                    session.Store(new Detail { Number = 4 }, "details/4");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", FriendId = "users/2", DetailIds = new List<string> { "details/1", "details/2" }}, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1", DetailIds = new List<string> { "details/3", "details/4" }}, "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let friend = session.Load<User>(u.FriendId).Name
                                let details = RavenQuery.Load<Detail>(u.DetailIds).Select(x=> x.Number)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Friend = friend,
                                    Details = details
                                };

                    Assert.Equal(
@"DECLARE function output(u, _doc_0, _docs_1) {
	var friend = _doc_0.Name;
	var details = _docs_1.map(function(x){return x.Number;});
	return { FullName : u.Name+"" ""+u.LastName, Friend : friend, Details : details };
}
FROM Users as u LOAD u.FriendId as _doc_0, u.DetailIds as _docs_1[] SELECT output(u, _doc_0, _docs_1)", query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob", queryResult[0].Friend);

                    var detailList = queryResult[0].Details.ToList();
                    Assert.Equal(2, detailList.Count);

                    Assert.Equal(1, detailList[0]);
                    Assert.Equal(2, detailList[1]);

                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                    Assert.Equal("Jerry", queryResult[1].Friend);

                    detailList = queryResult[1].Details.ToList();
                    Assert.Equal(2, detailList.Count);

                    Assert.Equal(3, detailList[0]);
                    Assert.Equal(4, detailList[1]);
                }
            }
        }

        [Fact]
        public void Should_Throw_With_Proper_Message_When_Using_Wrong_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new List<string> { "details/1", "details/2" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let details = session.Load<Detail>(u.DetailIds).Values.Select(x => x.Number)
                                select new
                                {
                                    FullName = u.Name + " " + u.LastName,
                                    Details = details
                                };

                    var exception = Assert.Throws<NotSupportedException>(() => query.ToList());
                    Assert.Equal("Using IDocumentSession.Load(IEnumerable<string> ids) inside a query is not supported. " +
                                 "You should use RavenQuery.Load(IEnumerable<string> ids) instead", exception.InnerException?.Message);

                }
            }
        }

        [Fact]
        public void Custom_Functions_With_ToList_And_ToArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Roles = new []{"Grateful", "Dead"}}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    RolesList = u.Roles.Select(a=> new
                                    {
                                        Id = a
                                    }).ToList(),

                                    RolesArray = u.Roles.Select(a => new
                                    {
                                        Id = a
                                    }).ToArray()
                                };

                    Assert.Equal("FROM Users as u SELECT { RolesList : u.Roles.map(function(a){return {Id:a};}), " +
                                 "RolesArray : u.Roles.map(function(a){return {Id:a};}) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    Assert.Equal(2, queryResult[0].RolesList.Count);
                    Assert.Equal("Grateful", queryResult[0].RolesList[0].Id);
                    Assert.Equal("Dead", queryResult[0].RolesList[1].Id);

                    Assert.Equal(2, queryResult[0].RolesArray.Length);
                    Assert.Equal("Grateful", queryResult[0].RolesArray[0].Id);
                    Assert.Equal("Dead", queryResult[0].RolesArray[1].Id);

                }
            }
        }

        [Fact]
        public void Custom_Functions_Null_Coalescing_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Phil", LastName = "" }, "users/2");
                    session.Store(new User { Name = "Pigpen" }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        select new
                        {
                            FirstName = u.Name,
                            LastName = u.LastName ?? "Has no last name"
                        };

                    Assert.Equal("FROM Users as u SELECT { FirstName : u.Name, " +
                                 "LastName : u.LastName !== null && u.LastName !== undefined ? u.LastName : \"Has no last name\" }"
                        , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].FirstName);
                    Assert.Equal("Garcia", queryResult[0].LastName);

                    Assert.Equal("Phil", queryResult[1].FirstName);
                    Assert.Equal("", queryResult[1].LastName);

                    Assert.Equal("Pigpen", queryResult[2].FirstName);
                    Assert.Equal("Has no last name", queryResult[2].LastName);

                }
            }
        }

        [Fact]
        public void Custom_Functions_Nested_Conditional_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.Store(new User { Name = "Phil", LastName = "Lesh" }, "users/3");
                    session.Store(new User { Name = "Bill", LastName = "Kreutzmann" }, "users/4");
                    session.Store(new User { Name = "Jon", LastName = "Doe" }, "users/5");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    u.Name,
                                    Role = u.Name == "Jerry" || u.Name == "Bob" ? "Guitar" :
                                        (u.Name == "Phil" ? "Bass" : (u.Name == "Bill" ? "Drums" : "Unknown"))
                                };

                    Assert.Equal("FROM Users as u SELECT { Name : u.Name, Role : u.Name===\"Jerry\"||u.Name===\"Bob\" ? \"Guitar\" : " +
                                 "(u.Name===\"Phil\" ? \"Bass\" : (u.Name===\"Bill\"?\"Drums\":\"Unknown\")) }"
                    , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(5, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal("Guitar", queryResult[0].Role);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal("Guitar", queryResult[1].Role);

                    Assert.Equal("Phil", queryResult[2].Name);
                    Assert.Equal("Bass", queryResult[2].Role);

                    Assert.Equal("Bill", queryResult[3].Name);
                    Assert.Equal("Drums", queryResult[3].Role);

                    Assert.Equal("Jon", queryResult[4].Name);
                    Assert.Equal("Unknown", queryResult[4].Role);

                }
            }
        }

        [Fact]
        public void Custom_Functions_String_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", IdNumber = 19420801, Roles = new []{"The", "Grateful", "Dead"}}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    //padStart(), padEnd() are not supporeted in jint
                                    //https://github.com/sebastienros/jint/issues/429

                                    //PadLeft = u.Name.PadLeft(10, 'z'),
                                    //PadRight = u.Name.PadRight(10, 'z'),

                                    Substr = u.Name.Substring(0, 3),
                                    Join = string.Join(", ", u.Name, u.LastName, u.IdNumber),
                                    ArrayJoin = string.Join("-", u.Roles)
                                };

                    Assert.Equal("FROM Users as u SELECT { Substr : u.Name.substr(0, 3), " +
                                 "Join : [u.Name,u.LastName,u.IdNumber].join(\", \"), " +
                                 "ArrayJoin : u.Roles.join(\"-\") }"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    Assert.Equal("Jer", queryResult[0].Substr);
                    Assert.Equal("Jerry, Garcia, 19420801", queryResult[0].Join);
                    Assert.Equal("The-Grateful-Dead", queryResult[0].ArrayJoin);

                }
            }
        }

        public class ProjectionParameters : RavenTestBase
        {
            public class Document
            {
                public string Id { get; set; }
                public string TargetId { get; set; }
                public decimal TargetValue { get; set; }
                public bool Deleted { get; set; }
                public IEnumerable<Document> SubDocuments { get; set; }
            }

            public class Result
            {
                public string TargetId { get; set; }
                public decimal TargetValue { get; set; }
            }

            Document doc1;
            Document doc2;
            Document doc3;

            private void SetUp(IDocumentStore store)
            {
                using (var session = store.OpenSession())
                {
                    doc1 = new Document
                    {
                        Deleted = false,
                        SubDocuments = new List<Document>
                    {
                        new Document
                        {
                            TargetId = "id1"
                        },
                        new Document
                        {
                            TargetId = "id2"
                        }
                    }
                    };
                    doc2 = new Document
                    {
                        Deleted = false,
                        SubDocuments = new List<Document>
                    {
                        new Document
                        {
                            TargetId = "id4"
                        },
                        new Document
                        {
                            TargetId = "id5"
                        }
                    }
                    };
                    doc3 = new Document
                    {
                        Deleted = true
                    };

                    session.Store(doc1);
                    session.Store(doc2);
                    session.Store(doc3);
                    session.SaveChanges();
                }
            }

            [Fact]
            public void CanProjectWithArrayParameters()
            {
                using (var store = GetDocumentStore())
                {
                    SetUp(store);

                    using (var session = store.OpenSession())
                    {
                        var ids = new[] { doc1.Id, doc2.Id, doc3.Id };
                        string[] targetIds = { "id2" };

                        var projection =
                            from d in session.Query<Document>().Where(x => x.Id.In(ids))
                            where d.Deleted == false
                            select new
                            {
                                Id = d.Id,
                                Deleted = d.Deleted,
                                Values = d.SubDocuments
                                        .Where(x => targetIds.Length == 0 || targetIds.Contains(x.TargetId))

                                          .Select(x => new Result
                                          {
                                                  TargetId = x.TargetId,
                                                  TargetValue = x.TargetValue
                                          })
                            };

                        Assert.Equal("FROM Documents as d WHERE (id() IN ($p0)) AND (Deleted = $p1) " +
                                     "SELECT { Id : d.Id, Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return ([\"id2\"]).length===0||[\"id2\"].indexOf(x.TargetId)>=0;}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
                                     , projection.ToString());

                        var result = projection.ToList();

                        Assert.Equal(2, result.Count);

                        Assert.Equal(doc1.Id, result[0].Id);

                        var values = result[0].Values.ToList();
                        Assert.Equal(1, values.Count);
                        Assert.Equal("id2", values[0].TargetId);

                        Assert.Equal(doc2.Id, result[1].Id);

                        values = result[1].Values.ToList();
                        Assert.Equal(0, values.Count);

                    }
                }
            }

            [Fact]
            public void CanProjectWithListParameters()
            {
                using (var store = GetDocumentStore())
                {
                    SetUp(store);

                    using (var session = store.OpenSession())
                    {
                        var ids = new[] {doc1.Id, doc2.Id, doc3.Id};
                        var targetIds = new List<string>
                        {
                            "id2"
                        };

                        var projection =
                            from d in session.Query<Document>().Where(x => x.Id.In(ids))
                            where d.Deleted == false
                            select new
                            {
                                Id = d.Id,
                                Deleted = d.Deleted,
                                Values = d.SubDocuments
                                    .Where(x => targetIds.Count == 0 || targetIds.Contains(x.TargetId))
                                    .Select(x => new Result
                                    {
                                        TargetId = x.TargetId,
                                        TargetValue = x.TargetValue
                                    })
                            };

                        Assert.Equal("FROM Documents as d WHERE (id() IN ($p0)) AND (Deleted = $p1) " +
                                     "SELECT { Id : d.Id, Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return [\"id2\"].length===0||[\"id2\"].indexOf(x.TargetId)>=0;}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
                            , projection.ToString());

                        var result = projection.ToList();
                        Assert.Equal(2, result.Count);

                        Assert.Equal(doc1.Id, result[0].Id);

                        var values = result[0].Values.ToList();
                        Assert.Equal(1, values.Count);
                        Assert.Equal("id2", values[0].TargetId);

                        Assert.Equal(doc2.Id, result[1].Id);

                        values = result[1].Values.ToList();
                        Assert.Equal(0, values.Count);
                    }
                }
            }

            [Fact]
            public void CanProjectWithStringParameter()
            {
                using (var store = GetDocumentStore())
                {
                    SetUp(store);

                    using (var session = store.OpenSession())
                    {
                        var ids = new[] { doc1.Id, doc2.Id, doc3.Id };
                        var targetId = "id2";

                        var projection =
                            from d in session.Query<Document>().Where(x => x.Id.In(ids))
                            where d.Deleted == false
                            select new
                            {
                                Id = d.Id,
                                Deleted = d.Deleted,
                                Values = d.SubDocuments
                                    .Where(x => targetId == null || x.TargetId == targetId)
                                    .Select(x => new Result
                                    {
                                        TargetId = x.TargetId,
                                        TargetValue = x.TargetValue
                                    })
                            };

                        Assert.Equal("FROM Documents as d WHERE (id() IN ($p0)) AND (Deleted = $p1) " +
                                     "SELECT { Id : d.Id, Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return \"id2\"===null||x.TargetId===\"id2\";}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
                            , projection.ToString());

                        var result = projection.ToList();
                        Assert.Equal(2, result.Count);

                        Assert.Equal(doc1.Id, result[0].Id);

                        var values = result[0].Values.ToList();
                        Assert.Equal(1, values.Count);
                        Assert.Equal("id2", values[0].TargetId);

                        Assert.Equal(doc2.Id, result[1].Id);

                        values = result[1].Values.ToList();
                        Assert.Equal(0, values.Count);
                    }
                }
            }
        }
    
        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public DateTime Birthday { get; set; }
            public int IdNumber { get; set; }
            public bool IsActive { get; set; }
            public string[] Roles { get; set; }
            public string DetailId { get; set; }
            public string FriendId { get; set; }
            public IEnumerable<string> DetailIds { get; set; }
        }
        private class Detail
        {
            public int Number { get; set; }
        }
        public class QueryResult
        {
            public string FullName { get; set; }
        }
    }
}

