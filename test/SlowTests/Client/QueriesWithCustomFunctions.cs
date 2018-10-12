using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client
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

                    Assert.Equal("from Users as u where u.Name = $p0 select { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

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

                    Assert.Equal("from Users as u where u.Name = $p0 select { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

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

                    Assert.Equal("from Users as u select { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date(new Date().setHours(0,0,0,0))-new Date(Date.parse(u.Birthday))) }",
                                query.ToString());

                    var queryResult = query.ToList();

                    var ts = DateTime.UtcNow.Date - new DateTime(1942, 8, 1);

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

                    Assert.Equal("from Users as u select { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date(new Date().setHours(0,0,0,0))-new Date(Date.parse(u.Birthday))) }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    var ts = DateTime.UtcNow.Date - new DateTime(1942, 8, 1);

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


                    Assert.Equal("from Users as u select { DayOfBirth : new Date(Date.parse(u.Birthday)).getDate(), MonthOfBirth : new Date(Date.parse(u.Birthday)).getMonth()+1, Age : new Date().getFullYear()-new Date(Date.parse(u.Birthday)).getFullYear() }"
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


                    Assert.Equal("from Users as u select { DayOfBirth : new Date(Date.parse(u.Birthday)).getDate(), MonthOfBirth : new Date(Date.parse(u.Birthday)).getMonth()+1, Age : new Date().getFullYear()-new Date(Date.parse(u.Birthday)).getFullYear() }"
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

                    Assert.Equal("from Users as u select { LuckyNumber : u.IdNumber/new Date(Date.parse(u.Birthday)).getFullYear(), Active : u.IsActive?\"yes\":\"no\" }",
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

                    Assert.Equal("from Users as u select { LuckyNumber : u.IdNumber/new Date(Date.parse(u.Birthday)).getFullYear(), Active : u.IsActive?\"yes\":\"no\" }",
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

                    Assert.Equal("from Users as u select { Roles : u.Roles.map(function(r){return {RoleName:r+\"!\"};}) }", query.ToString());

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

                    Assert.Equal("from Users as u select { Roles : u.Roles.map(function(r){return {RoleName:r+\"!\"};}) }", query.ToString());

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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var lastName = u.LastName;
	return { FullName : u.Name+"" ""+lastName };
}
from Users as u select output(u)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
                        @"declare function output(u) {
	var lastName = u.LastName;
	return { FullName : u.Name+"" ""+lastName };
}
from Users as u select output(u)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
 @"declare function output(u) {
	var format = function(p){return p.Name+"" ""+p.LastName;};
	return { FullName : format(u) };
}
from Users as u select output(u)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
                        @"declare function output(u) {
	var format = function(p){return p.Name+"" ""+p.LastName;};
	return { FullName : format(u) };
}
from Users as u select output(u)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var space = "" "";
	var last = u.LastName;
	var format = function(p){return p.Name+space+last;};
	return { FullName : format(u) };
}
from Users as u select output(u)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
                        @"declare function output(u) {
	var space = "" "";
	var last = u.LastName;
	var format = function(p){return p.Name+space+last;};
	return { FullName : format(u) };
}
from Users as u select output(u)", query.ToString());


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

                    Assert.Equal("from Users as u where u.Name != $p0 load u.DetailId as detail select { FullName : u.Name+\" \"+u.LastName, Detail : detail.Number }",
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

                    Assert.Equal(@"from Users as u where u.Name != $p0 load u.DetailId as detail select { FullName : u.Name+"" ""+u.LastName, Detail : detail.Number }",
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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail, friend) {
	var format = function(user){return user.Name+"" ""+user.LastName;};
	return { FullName : format(u), Friend : format(friend), Detail : detail.Number };
}
from Users as u where u.Name != $p0 load u.DetailId as detail, u.FriendId as friend select output(u, detail, friend)",
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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail, friend) {
	var format = function(user){return user.Name+"" ""+user.LastName;};
	return { FullName : format(u), Friend : format(friend), Detail : detail.Number };
}
from Users as u where u.Name != $p0 load u.DetailId as detail, u.FriendId as friend select output(u, detail, friend)",
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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail) {
	var format = function(user){return user.Name+"" ""+u.LastName;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
from Users as u load u.DetailId as detail select output(u, detail)", query.ToString());

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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail) {
	var format = function(user){return user.Name+"" ""+u.LastName;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
from Users as u load u.DetailId as detail select output(u, detail)", query.ToString());

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

                    Assert.Equal(@"from Users as u where u.Name != $p0 load u.DetailIds as details[] select { FullName : u.Name+"" ""+u.LastName, Details : details }",
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

                    Assert.Equal(@"from Users as u where u.Name != $p0 load u.DetailIds as details[] select { FullName : u.Name+"" ""+u.LastName, Details : details }",
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

                    Assert.Equal(@"from Users as u where u.Name != $p0 load u.DetailIds as details[] select { FullName : u.Name+"" ""+u.LastName, Details : details }",
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

                    Assert.Equal(@"from Users as u where u.Name != $p0 load u.DetailIds as details[] select { FullName : u.Name+"" ""+u.LastName, Details : details }",
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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail) {
	var last = u.LastName;
	var format = function(user){return user.Name+"" ""+last;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
from Users as u where (u.Name = $p0) and (u.IsActive = $p1) order by LastName desc load u.DetailId as detail select output(u, detail)", query.ToString());

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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, detail) {
	var last = u.LastName;
	var format = function(user){return user.Name+"" ""+last;};
	return { FullName : format(u), DetailNumber : detail.Number };
}
from Users as u where (u.Name = $p0) and (u.IsActive = $p1) order by LastName desc load u.DetailId as detail select output(u, detail)", query.ToString());

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

                    Assert.Equal("from Users as u select { Pow : Math.pow(u.IdNumber, u.IdNumber), Max : Math.max((u.IdNumber+1), u.IdNumber) }", query.ToString());

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

                    Assert.Equal("from Users as user select { FullName : user.Name+\" \"+user.LastName }", query.ToString());

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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(user) {
	var first = user.Name;
	var last = user.LastName;
	var format = function(){return first+"" ""+last;};
	return { FullName : format() };
}
from Users as user select output(user)", query.ToString());


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

                    RavenTestHelper.AssertEqualRespectingNewLines(
                        @"declare function output(u) {
	var date = new Date(1960, 0, 1);
	return { Bday : new Date(Date.parse(u.Birthday)), Date : date };
}
from Users as u select output(u)", query.ToString());


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

                    Assert.Equal("from Users as user select { Date : new Date(Date.parse(user.Birthday)), Name : user.Name.substr(0,3) }",
                        query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(new DateTime(1942, 8, 1), queryResult[0].Date);
                    Assert.Equal("Jer", queryResult[0].Name);

                }
            }
        }
        
        [Fact]
        public void Custom_Functions_With_Escape_Hatch_Inside_Let()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Birthday = new DateTime(1942, 8, 1)
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let days = RavenQuery.Raw<int>("Math.ceil((Date.now() - Date.parse(u.Birthday)) / (1000*60*60*24))")
                                select new
                                {
                                    Days = days
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var days = Math.ceil((Date.now() - Date.parse(u.Birthday)) / (1000*60*60*24));
	return { Days : days };
}
from Users as u select output(u)", query.ToString());
                    
                    var queryResult = query.ToList();
                    
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(Math.Ceiling((DateTime.UtcNow - new DateTime(1942, 8, 1)).TotalDays), queryResult[0].Days);
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

                    Assert.Equal("from Users as a where a.Name = $p0 select { Name : a.Name.substr(0,3) }",
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

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, _doc_0, _docs_1) {
	var friend = _doc_0.Name;
	var details = _docs_1.map(function(x){return x.Number;});
	return { FullName : u.Name+"" ""+u.LastName, Friend : friend, Details : details };
}
from Users as u load u.FriendId as _doc_0, u.DetailIds as _docs_1[] select output(u, _doc_0, _docs_1)", query.ToString());

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
                                 "You should use RavenQuery.Load(IEnumerable<string> ids) instead", exception.Message);

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

                    Assert.Equal("from Users as u select { RolesList : u.Roles.map(function(a){return {Id:a};}), " +
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

                    Assert.Equal("from Users as u select { FirstName : u.Name, " +
                                 "LastName : (u.LastName!=null?u.LastName:\"Has no last name\") }"
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
        public void Custom_Functions_ValueTypeParse_Support()
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
                                select new
                                {
                                    IntParse = int.Parse("1234") + int.Parse("1234"),
                                    DoubleParse = double.Parse("1234"),
                                    DecimalParse = decimal.Parse("12.34"),
                                    BoolParse = bool.Parse("true"),
                                    CharParse = char.Parse("s"),
                                    ByteParse = byte.Parse("127"),
                                    LongParse = long.Parse("1234"),
                                    SByteParse = sbyte.Parse("127"),
                                    ShortParse = short.Parse("1234"),
                                    UintParse = uint.Parse("1234"),
                                    UlongParse = ulong.Parse("1234"),
                                    UshortParse = ushort.Parse("1234")
                                };

                    Assert.Equal("from Users as u select { " +
                        "IntParse : parseInt(\"1234\")+parseInt(\"1234\"), " +
                        "DoubleParse : parseFloat(\"1234\"), " +
                        "DecimalParse : parseFloat(\"12.34\"), " +
                        "BoolParse : \"true\" == (\"true\"), " +
                        "CharParse : (\"s\"), " +
                        "ByteParse : parseInt(\"127\"), " +
                        "LongParse : parseInt(\"1234\"), " +
                        "SByteParse : parseInt(\"127\"), " +
                        "ShortParse : parseInt(\"1234\"), " +
                        "UintParse : parseInt(\"1234\"), " +
                        "UlongParse : parseInt(\"1234\"), " +
                        "UshortParse : parseInt(\"1234\") }", query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);


                    Assert.Equal(int.Parse("1234") + int.Parse("1234"), queryResult[0].IntParse);
                    Assert.Equal(1234, queryResult[0].DoubleParse);
                    Assert.Equal(12.34M, queryResult[0].DecimalParse);
                    Assert.Equal(true, queryResult[0].BoolParse);
                    Assert.Equal('s', queryResult[0].CharParse);
                    Assert.Equal(127, queryResult[0].ByteParse);
                    Assert.Equal(1234, queryResult[0].LongParse);
                    Assert.Equal(127, queryResult[0].SByteParse);
                    Assert.Equal(1234, queryResult[0].ShortParse);
                    Assert.Equal((uint)1234, queryResult[0].UintParse);
                    Assert.Equal((ulong)1234, queryResult[0].UlongParse);
                    Assert.Equal((ushort)1234, queryResult[0].UshortParse);

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

                    Assert.Equal("from Users as u select { Name : u.Name, Role : u.Name===\"Jerry\"||u.Name===\"Bob\" ? \"Guitar\" : " +
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
                    session.Store(new User { Name = "Bob", LastName = "Weir", Roles = new[] { "o" } }, "users/2");
                    session.Store(new User { Name = "  John   ", LastName = "Doe" }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    PadLeft = u.Name.PadLeft(10, 'z'),
                                    PadRight = u.Name.PadRight(10, 'z'),
                                    StartsWith = u.Name.StartsWith("J"),
                                    EndsWith = u.Name.EndsWith("b"),
                                    Substr = u.Name.Substring(0, 2),
                                    Join = string.Join(", ", u.Name, u.LastName, u.IdNumber),
                                    ArrayJoin = string.Join("-", u.Roles),
                                    Trim = u.Name.Trim(),
                                    ToUpper = u.Name.ToUpper(),
                                    ToLower = u.Name.ToLower(),
                                    Contains = u.Name.Contains("e"),
                                    Format = "Name: "+u.Name+", LastName : "+u.LastName,
                                    Split = u.Name.Split('r', StringSplitOptions.None),
                                    SplitLimit = u.Name.Split(new char[] { 'r' }, 3),
                                    SplitArray = u.Name.Split(new char[] { 'r', 'e' }),
                                    SplitArgument =  u.Name.Split(u.Roles, StringSplitOptions.None),
                                    SplitStringArray = u.Name.Split(new string[] { "er", "rr" }, StringSplitOptions.None),

                                    Replace = u.Name.Replace('r', 'd'),
                                    ReplaceString = u.Name.Replace("Jerry", "Charly"),
                                    ReplaceArguments = u.Name.Replace(u.Name, u.LastName),
                                    ReplaceArgumentsComplex = u.Name.Replace(u.Name + "a", u.LastName + "a")
                                };
                    Assert.Equal("from Users as u select { " +
                        "PadLeft : u.Name.padStart(10, \"z\"), " +
                        "PadRight : u.Name.padEnd(10, \"z\"), " +
                        "StartsWith : u.Name.startsWith(\"J\"), " +
                        "EndsWith : u.Name.endsWith(\"b\"), " +
                        "Substr : u.Name.substr(0, 2), " +
                        "Join : [u.Name,u.LastName,u.IdNumber].join(\", \"), " +
                        "ArrayJoin : u.Roles.join(\"-\"), " +
                        "Trim : u.Name.trim(), " +
                        "ToUpper : u.Name.toUpperCase(), " +
                        "ToLower : u.Name.toLowerCase(), " +
                        "Contains : u.Name.indexOf(\"e\") !== -1, " +
                        "Format : \"Name: \"+u.Name+\", LastName : \"+u.LastName, " +
                        "Split : u.Name.split(new RegExp(\"r\", \"g\")), " +
                        "SplitLimit : u.Name.split(new RegExp(\"r\", \"g\")), " +
                        "SplitArray : u.Name.split(new RegExp(\"r\"+\"|\"+\"e\", \"g\")), " +
                        "SplitArgument : u.Name.split(new RegExp(u.Roles, \"g\")), " +
                        "SplitStringArray : u.Name.split(new RegExp(\"er\"+\"|\"+\"rr\", \"g\")), " +
                        "Replace : u.Name.replace(new RegExp(\"r\", \"g\"), \"d\"), " +
                        "ReplaceString : u.Name.replace(new RegExp(\"Jerry\", \"g\"), \"Charly\"), " +
                        "ReplaceArguments : u.Name.replace(new RegExp(u.Name, \"g\"), u.LastName), " +
                        "ReplaceArgumentsComplex : u.Name.replace(new RegExp((u.Name+\"a\"), \"g\"), (u.LastName+\"a\")) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry".PadLeft(10, 'z'), queryResult[0].PadLeft);
                    Assert.Equal("Jerry".PadRight(10, 'z'), queryResult[0].PadRight);
                    Assert.True(queryResult[0].StartsWith);
                    Assert.False(queryResult[0].EndsWith);
                    Assert.Equal("Je", queryResult[0].Substr);
                    Assert.Equal("Jerry, Garcia, 19420801", queryResult[0].Join);
                    Assert.Equal("The-Grateful-Dead", queryResult[0].ArrayJoin);
                    Assert.Equal("Jerry".ToUpper(), queryResult[0].ToUpper);
                    Assert.Equal("Jerry".ToLower(), queryResult[0].ToLower);
                    Assert.Equal("Jerry".Contains("e"), queryResult[0].Contains);
                    Assert.Equal("Name: Jerry, LastName : Garcia", queryResult[0].Format);
                    Assert.Equal("Jerry".Split('r', StringSplitOptions.None), queryResult[0].Split);
                    Assert.Equal("Jerry".Split(new char[] { 'r' }, 3), queryResult[0].SplitLimit);
                    Assert.Equal("Jerry".Split(new char[] { 'r', 'e' }), queryResult[0].SplitArray);
                    Assert.Equal("Jerry".Split(new string[] { "er", "rr" }, StringSplitOptions.None), queryResult[0].SplitStringArray);
                    Assert.Equal("Jerry".Replace('r', 'd'), queryResult[0].Replace);
                    Assert.Equal("Jerry".Replace("Jerry", "Charly"), queryResult[0].ReplaceString);
                    Assert.Equal("Jerry".Replace("Jerry", "Garcia"), queryResult[0].ReplaceArguments);
                    Assert.Equal("Jerry".Replace("Jerrya", "Charlya"), queryResult[0].ReplaceArgumentsComplex);

                    Assert.Equal("Bob".PadLeft(10, 'z'), queryResult[1].PadLeft);
                    Assert.Equal("Bob".PadRight(10, 'z'), queryResult[1].PadRight);
                    Assert.Equal("Bob".Split(new char[] { 'o' }), queryResult[1].SplitArgument);
                    Assert.False(queryResult[1].StartsWith);
                    Assert.True(queryResult[1].EndsWith);
                    Assert.Equal("Bo", queryResult[1].Substr);
                    Assert.Equal("Bob, Weir, 0", queryResult[1].Join);
                    Assert.Equal("Name: Bob, LastName : Weir", queryResult[1].Format);


                    Assert.Equal("  John   ".Trim(), queryResult[2].Trim);
                    Assert.Null(queryResult[2].ArrayJoin);
                }
            }
        }

        [Fact]
        public void Custom_Function_ToDictionary_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserGroup()
                    {
                        Name = "Administrators",
                        Users = new List<User>()
                        {
                            new User() { Name = "Bob", LastName = "Santa Claus" },
                            new User() { Name = "Jack", LastName = "Ripper" },
                            new User() { Name = "John", LastName = "Doe" },
                        }
                    });
                    session.Store(new UserGroup()
                    {
                        Name = "Editors",
                        Users = new List<User>()
                        {
                            new User() { Name = "Tom", LastName = "Smith" },
                            new User() { Name = "Ed", LastName = "Lay" },
                            new User() { Name = "Russell", LastName = "Leetch" },
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<UserGroup>()
                                select new
                                {
                                    Name = u.Name,
                                    UsersByName = u.Users.ToDictionary(a => a.Name),
                                    UsersByNameLastName = u.Users.ToDictionary(a => a.Name, a => a.LastName)
                                };

                    Assert.Equal("from UserGroups as u select { " +
                        "Name : u.Name, " +
                        "UsersByName : u.Users.reduce(function(_obj, _cur) {_obj[(function(a){return a.Name;})(_cur)] = _cur;return _obj;}, {}), " +
                        "UsersByNameLastName : u.Users.reduce(function(_obj, _cur) {_obj[(function(a){return a.Name;})(_cur)] = (function(a){return a.LastName;})(_cur);return _obj;}, {}) }", query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Administrators", queryResult[0].Name);
                    Assert.Equal("Doe", queryResult[0].UsersByName["John"].LastName);
                    Assert.Equal("Ripper", queryResult[0].UsersByNameLastName["Jack"]);
                    Assert.Equal(3, queryResult[0].UsersByName.Count);

                    Assert.Equal("Editors", queryResult[1].Name);
                    Assert.Equal("Smith", queryResult[1].UsersByName["Tom"].LastName);
                    Assert.Equal("Leetch", queryResult[1].UsersByNameLastName["Russell"]);
                    Assert.Equal(3, queryResult[1].UsersByNameLastName.Count);
                }
            }
        }

        [Fact]
        public void Custom_Function_First_And_FirstOrDefault_Support()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");
                    session.Store(new Detail { Number = 3 }, "details/3");
                    session.Store(new Detail { Number = 4 }, "details/4");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new List<string> { "details/1", "details/2" }}, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailIds = new List<string> { "details/3", "details/4" } }, "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let details = RavenQuery.Load<Detail>(u.DetailIds)
                                select new 
                                {
                                    Name = u.Name,
                                    First = details.First(x => x.Number > 1).Number,
                                    FirstOrDefault = details.FirstOrDefault(),
                                    FirstOrDefaultWithPredicate = details.FirstOrDefault(x => x.Number < 3)
                                };

                    Assert.Equal("from Users as u load u.DetailIds as details[] " +
                                 "select { Name : u.Name, " +
                                          "First : details.find(function(x){return x.Number>1;}).Number, " +
                                          "FirstOrDefault : details[0], " +
                                          "FirstOrDefaultWithPredicate : details.find(function(x){return x.Number<3;}) }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(2, queryResult[0].First);
                    Assert.Equal(1, queryResult[0].FirstOrDefault?.Number);
                    Assert.Equal(1, queryResult[0].FirstOrDefaultWithPredicate.Number);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal(3, queryResult[1].First);
                    Assert.Equal(3, queryResult[1].FirstOrDefault?.Number);
                    Assert.Null(queryResult[1].FirstOrDefaultWithPredicate);
                }
            }
        }

        [Fact]
        public void Custom_Function_With_Nested_Query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 1 }, "details/1");
                    session.Store(new Detail { Number = 2 }, "details/2");
                    session.Store(new Detail { Number = 3 }, "details/3");
                    session.Store(new Detail { Number = 4 }, "details/4");

                    session.Store(new User { Name = "Jerry", LastName = "Garcia", DetailIds = new List<string> { "details/1", "details/2" } }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", DetailIds = new List<string> { "details/3", "details/4" } }, "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    Name = u.Name,
                                    DetailNumbers = from detailId in u.DetailIds
                                                    let detail = RavenQuery.Load<Detail>(detailId)
                                                    select new
                                                    {
                                                        Number = detail.Number
                                                    }
                                };

                    Assert.Equal("from Users as u select { Name : u.Name, " +
                                 "DetailNumbers : u.DetailIds.map(function(detailId){return {detailId:detailId,detail:load(detailId)};})" +
                                                            ".map(function(h__TransparentIdentifier0){return {Number:h__TransparentIdentifier0.detail.Number};}) }"
                                ,query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);

                    var details = queryResult[0].DetailNumbers.ToList();

                    Assert.Equal(2, details.Count);
                    Assert.Equal(1, details[0].Number);
                    Assert.Equal(2, details[1].Number);

                    Assert.Equal("Bob", queryResult[1].Name);
                    details = queryResult[1].DetailNumbers.ToList();

                    Assert.Equal(2, details.Count);
                    Assert.Equal(3, details[0].Number);
                    Assert.Equal(4, details[1].Number);

                }
            }
        }

        [Fact]
        public void Query_On_Index_With_Load()
        {
            using (var store = GetDocumentStore())
            {
                var definition = new IndexDefinitionBuilder<User>("UsersByNameAndFriendId")
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            doc.Name, doc.FriendId
                        }
                }.ToIndexDefinition(store.Conventions);
                store.Maintenance.Send(new PutIndexesOperation(definition));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", FriendId = "users/2" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1"}, "users/2");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1" }, "users/3");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>("UsersByNameAndFriendId")
                                where u.Name != "Pigpen"
                                let friend = RavenQuery.Load<User>(u.FriendId)
                                select new
                                {
                                    Name = u.Name,
                                    Friend = friend.Name
                                };

                    Assert.Equal("from index \'UsersByNameAndFriendId\' as u where u.Name != $p0 " +
                                 "load u.FriendId as friend select { Name : u.Name, Friend : friend.Name }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal("Bob", queryResult[0].Friend);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal("Jerry", queryResult[1].Friend);

                }
            }
        }

        [Fact]
        public void Query_On_Index_With_Load_Into_Class()
        {
            using (var store = GetDocumentStore())
            {
                var definition = new IndexDefinitionBuilder<User>("UsersByNameAndFriendId")
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Name,
                                      doc.FriendId
                                  }
                }.ToIndexDefinition(store.Conventions);
                store.Maintenance.Send(new PutIndexesOperation(definition));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", FriendId = "users/2" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1" }, "users/2");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1" }, "users/3");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>("UsersByNameAndFriendId")
                                where u.Name != "Pigpen"
                                let friend = RavenQuery.Load<User>(u.FriendId)
                                select new IndexQueryResult
                                {
                                    Name = u.Name,
                                    Friend = friend.Name
                                };

                    Assert.Equal("from index \'UsersByNameAndFriendId\' as u where u.Name != $p0 " +
                                 "load u.FriendId as friend select { Name : u.Name, Friend : friend.Name }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal("Bob", queryResult[0].Friend);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal("Jerry", queryResult[1].Friend);

                }
            }
        }

        [Fact]
        public void Custom_Function_With_GetMetadataFor()
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
                                select new 
                                {
                                    Name = u.Name,
                                    Metadata = session.Advanced.GetMetadataFor(u),
                                };

                    Assert.Equal("from Users as u select { Name : u.Name, Metadata : getMetadata(u) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.Equal(metadata.Count, queryResult[0].Metadata.Count);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Id], queryResult[0].Metadata[Constants.Documents.Metadata.Id]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Collection], queryResult[0].Metadata[Constants.Documents.Metadata.Collection] );
                    Assert.Equal(metadata[Constants.Documents.Metadata.ChangeVector], queryResult[0].Metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.RavenClrType], queryResult[0].Metadata[Constants.Documents.Metadata.RavenClrType]);

                    DateTime.TryParse(metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind , out var lastModified);
                    DateTime.TryParse(queryResult[0].Metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastModifiedFromQueryResult);

                    Assert.Equal(lastModified, lastModifiedFromQueryResult);
                }
            }
        }

        [Fact]
        public async Task Custom_Function_With_GetMetadataFor_Async()
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
                                select new
                                {
                                    Name = u.Name,
                                    Metadata = session.Advanced.GetMetadataFor(u),
                                };

                    Assert.Equal("from Users as u select { Name : u.Name, Metadata : getMetadata(u) }", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);

                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.Equal(metadata.Count, queryResult[0].Metadata.Count);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Id], queryResult[0].Metadata[Constants.Documents.Metadata.Id]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Collection], queryResult[0].Metadata[Constants.Documents.Metadata.Collection]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.ChangeVector], queryResult[0].Metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.RavenClrType], queryResult[0].Metadata[Constants.Documents.Metadata.RavenClrType]);

                    DateTime.TryParse(metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastModified);
                    DateTime.TryParse(queryResult[0].Metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastModifiedFromQueryResult);

                    Assert.Equal(lastModified, lastModifiedFromQueryResult);
                }
            }
        }

        [Fact]
        public void Can_Load_Static_Value()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia"}, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.Store(new Detail { Number = 15 }, "details/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.LastName == "Garcia"
                                let detail = session.Load<Detail>("details/1")
                                select new
                                {
                                    Name = u.Name,
                                    Detail = detail
                                };

                    Assert.Equal("from Users as u where u.LastName = $p0 " +
                                 "load $p1 as detail select { Name : u.Name, Detail : detail }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(15, queryResult[0].Detail.Number);

                    var rawQuery = session.Advanced.RawQuery<RawQueryResult>("from Users as u where u.LastName = \"Garcia\" " +
                                                                             "load \"details/1\" as detail " +
                                                                             "select { Name : u.Name, Detail : detail}").ToList();
                                    
                    Assert.Equal(1, rawQuery.Count);
                    Assert.Equal("Jerry", rawQuery[0].Name);
                    Assert.Equal(15, rawQuery[0].Detail.Number);
                }
            }
        }

        [Fact]
        public void Custom_Function_With_RavenQueryMetadata()
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
                                select new
                                {
                                    Name = u.Name,
                                    Metadata = RavenQuery.Metadata(u),
                                };

                    Assert.Equal("from Users as u select { Name : u.Name, Metadata : getMetadata(u) }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.Equal(metadata.Count, queryResult[0].Metadata.Count);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Id], queryResult[0].Metadata[Constants.Documents.Metadata.Id]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.Collection], queryResult[0].Metadata[Constants.Documents.Metadata.Collection]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.ChangeVector], queryResult[0].Metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.Equal(metadata[Constants.Documents.Metadata.RavenClrType], queryResult[0].Metadata[Constants.Documents.Metadata.RavenClrType]);

                    DateTime.TryParse(metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastModified);
                    DateTime.TryParse(queryResult[0].Metadata[Constants.Documents.Metadata.LastModified].ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastModifiedFromQueryResult);

                    Assert.Equal(lastModified, lastModifiedFromQueryResult);
                }
            }
        }

        [Fact]
        public async Task QueryCompareExchangeValue(){
        
            using (var store = GetDocumentStore())
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("users/1", "Karmel", 0));
                var result = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("users/1"));
                Assert.Equal("Karmel", result.Value);
                            
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    u.Name,
                                    UniqueUser = RavenQuery.CmpXchg<string>("users/1")
                                };

                    Assert.Equal("from Users as u select { Name : u.Name, UniqueUser : cmpxchg(\"users/1\") }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Karmel", queryResult[0].UniqueUser);
                }
            }
        }
        
        [Fact]
        public async Task QueryCompareExchangeInnerValue(){
        
            using (var store = GetDocumentStore())
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User
                {
                    Name = "Karmel",
                    LastName = "Indych"
                }, 0));
                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("users/1"));
                Assert.Equal("Karmel", res.Value.Name);
                            
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        select new
                        {
                            u.Name,
                            UniqueUser = RavenQuery.CmpXchg<User>("users/1").Name,
                        };

                    Assert.Equal("from Users as u select { Name : u.Name, UniqueUser : cmpxchg(\"users/1\").Name }", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Karmel", queryResult[0].UniqueUser);
                }
            }
        }

        [Fact]
        public async Task QueryCompareExchangeWhere(){
        
            using (var store = GetDocumentStore())
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Tom","Jerry", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Hera","Zeus", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Gaya","Uranus", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Jerry@gmail.com","users/2", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Zeus@gmail.com","users/1", 0));
              
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry"}, "users/2");
                    session.Store(new User { Name = "Zeus", LastName = "Jerry"}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        where u.Name == RavenQuery.CmpXchg<string>("Hera") && u.LastName == RavenQuery.CmpXchg<string>("Tom")
                        select u;
                    var q = session.Advanced
                        .DocumentQuery<User>()
                        .WhereEquals("Name", CmpXchg.Value("Hera"))
                        .WhereEquals("LastName", CmpXchg.Value("Tom"));

                    Assert.Equal("from Users where Name = cmpxchg($p0) and LastName = cmpxchg($p1)", query.ToString());
                    Assert.Equal(q.ToString(), query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Zeus", queryResult[0].Name);
                    
                    query = from u in session.Query<User>()
                        where u.Name != RavenQuery.CmpXchg<string>("Hera")
                        select u;
                    Assert.Equal("from Users where Name != cmpxchg($p0)", query.ToString());
                    queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);

                    var rql = "from Users where Name = cmpxchg(\"Hera\")";
                    queryResult = session.Advanced.RawQuery<User>(rql).ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Zeus", queryResult[0].Name);

                    rql = "from Users where id() = cmpxchg(\"Zeus@gmail.com\")";
                    queryResult = session.Advanced.RawQuery<User>(rql).ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Zeus", queryResult[0].Name);
                }
            }
        }

        [Fact(Skip = "RavenDB-9850")]
        public async Task QueryCompareExchangeWhereWithProperty()
        {

            using (var store = GetDocumentStore())
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Tom", "Jerry", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Hera", "Zeus", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Jerry@gmail.com", "users/2", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("Zeus@gmail.com", "users/1", 0));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<Linked>("ActiveUser", new Linked
                {
                    Name = "Uranus",
                    Next = new Linked
                    {
                        Name = "Cronus",
                        Next = new Linked
                        {
                            Name = "Zeus"
                        }
                    },
                    Users = new List<User>
                    {
                        new User
                        {
                            Name = "foo/bar",
                            IsActive = true
                        }
                    }
                }, 0));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", IsActive = true }, "users/2");
                    session.Store(new User { Name = "Zeus", IsActive = false }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        where u.Name == RavenQuery.CmpXchg<Linked>("ActiveUser").Next.Next.Name
                        select u;
                    var q = session.Advanced.DocumentQuery<User>().WhereEquals("Name", CmpXchg.Value("ActiveUser"));

                    Assert.Equal("from Users where Name = cmpxchg($p0).Next.Next.Name", query.ToString());
                    Assert.Equal(q.ToString(), query.ToString());

                    var queryResult = query.ToList();
//                    Assert.Equal(1, queryResult.Count);
//                    Assert.Equal("Zeus", queryResult[0].Name);

                    query = from u in session.Query<User>()
                        where u.IsActive == RavenQuery.CmpXchg<Linked>("ActiveUser").Users[0].IsActive
                        select u;
                    Assert.Equal("from Users where IsActive = cmpxchg($p0).Users[0].IsActive", query.ToString());
                    queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Zeus", queryResult[0].Name);
                }
            }
        }

        [Fact]
        public void Should_Add_An_Alias_To_Where_Tokens()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry", LastName = "Garcia"
                    }, "employees/1");

                    session.Store(new Employee
                    {
                        FirstName = "Bob", LastName = "Weir"
                    }, "employees/2");

                    session.Store(new Order
                    {
                        Employee = "employees/1",
                        OrderedAt = new DateTime(1942, 8, 1)
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/2",
                        OrderedAt = new DateTime(1947, 10, 16)
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var complexLinqQuery =  from o in session.Query<Order>()
                                            where o.OrderedAt.Year <= 1945
                                            let employee = session.Load<Employee>(o.Employee)
                                            select new
                                            {
                                                Id = o.Id,
                                                Status = "Ordered at " + o.OrderedAt + ", by " + employee.FirstName + " " + employee.LastName
                                            };

                    Assert.Equal("from Orders as o where o.OrderedAt.Year <= $p0 " +
                                 "load o.Employee as employee " +
                                 "select { Id : id(o), Status : \"Ordered at \"+new Date(Date.parse(o.OrderedAt))+\", by \"+employee.FirstName+\" \"+employee.LastName }"
                                 ,complexLinqQuery.ToString());

                    var queryResult = complexLinqQuery.ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Ordered at Sat Aug 01 1942 00:00:00 GMT+00:00, by Jerry Garcia", queryResult[0].Status);

                }
            }
        }

        [Fact]
        public void Custom_Function_With_Sum()
        {
            using (var store = GetDocumentStore())
            {
                var o1 = new Order
                {
                    Lines = new List<OrderLine>
                    {
                        new OrderLine
                        {
                            PricePerUnit = (decimal)1.0,
                            Quantity = 3
                        },
                        new OrderLine
                        {
                            PricePerUnit = (decimal)1.5,
                            Quantity = 3
                        }
                    }
                };
                var o2 = new Order
                {
                    Lines = new List<OrderLine>
                    {
                        new OrderLine
                        {
                            PricePerUnit = (decimal)1.0,
                            Quantity = 5
                        },
                    }
                };
                var o3 = new Order
                {
                    Lines = new List<OrderLine>
                    {
                        new OrderLine
                        {
                            PricePerUnit = (decimal)3.0,
                            Quantity = 6,
                            Discount = (decimal)3.5
                        },
                        new OrderLine
                        {
                            PricePerUnit = (decimal)8.0,
                            Quantity = 3,
                            Discount = (decimal)3.5
                        },
                        new OrderLine
                        {
                            PricePerUnit = (decimal)1.8,
                            Quantity = 2
                        }
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(o1);
                    session.Store(o2);
                    session.Store(o3);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var complexLinqQuery =
                        from o in session.Query<Order>()
                        let TotalSpentOnOrder =
                            (Func<Order, decimal>)(order =>
                                order.Lines.Sum(l => l.PricePerUnit * l.Quantity - l.Discount))
                        select new
                        {
                            Id = o.Id,
                            TotalMoneySpent = TotalSpentOnOrder(o)
                        };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o) {
	var TotalSpentOnOrder = function(order){return order.Lines.map(function(l){return l.PricePerUnit*l.Quantity-l.Discount;}).reduce(function(a, b) { return a + b; }, 0);};
	return { Id : id(o), TotalMoneySpent : TotalSpentOnOrder(o) };
}
from Orders as o select output(o)", complexLinqQuery.ToString());

                    var queryResult = complexLinqQuery.ToList();
                    Assert.Equal(3, queryResult.Count);

                    var totalSpentOnOrder =
                        (Func<Order, decimal>)(order =>
                            order.Lines.Sum(l => l.PricePerUnit * l.Quantity - l.Discount));

                    Assert.Equal("orders/1-A", queryResult[0].Id);
                    Assert.Equal(totalSpentOnOrder(o1), queryResult[0].TotalMoneySpent);

                    Assert.Equal("orders/2-A", queryResult[1].Id);
                    Assert.Equal(totalSpentOnOrder(o2), queryResult[1].TotalMoneySpent);

                    Assert.Equal("orders/3-A", queryResult[2].Id);
                    Assert.Equal(totalSpentOnOrder(o3), queryResult[2].TotalMoneySpent);

                }
            }
        }

        [Fact]
        public void Can_project_id_property_to_any_name()
        {
            //http://issues.hibernatingrhinos.com/issue/RavenDB-9260

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        LastName = "Garcia"
                    }, "employees/1");

                    session.Store(new Order
                    {
                        Employee = "employees/1"
                    }, "orders/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let employee = session.Load<Employee>(o.Employee)
                                let employeeId = employee.Id
                                select new
                                {
                                    OrderId = o.Id,
                                    EmployeeId1 = employeeId,
                                    EmployeeId2 = employee.Id
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, employee) {
	var employeeId = id(employee);
	return { OrderId : id(o), EmployeeId1 : employeeId, EmployeeId2 : id(employee) };
}
from Orders as o load o.Employee as employee select output(o, employee)" , query.ToString());
                                 

                    var queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);

                    Assert.Equal("orders/1", queryResult[0].OrderId);
                    Assert.Equal("employees/1", queryResult[0].EmployeeId1);
                    Assert.Equal("employees/1", queryResult[0].EmployeeId2);

                }
            }
        }

        [Fact]
        public void Should_quote_alias_if_its_a_reserved_word()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 25,
                                Discount = (decimal)0.1,
                                Quantity = 4
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from order in session.Query<Order>()
                                select new
                                {
                                    Total = order.Lines.Sum(l => l.PricePerUnit * l.Quantity * (1 - l.Discount))
                                };


                    Assert.Equal("from Orders as 'order' " +
                                 "select { Total : order.Lines.map(function(l){return l.PricePerUnit*l.Quantity*(1-l.Discount);}).reduce(function(a, b) { return a + b; }, 0) }"
                                 , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(90, queryResult[0].Total);
                }
            }
        }

        [Fact]
        public void Custom_Function_With_ToString()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        Birthday = new DateTime(1942, 8, 1)
                    });
                    session.Store(new User
                    {
                        Name = "Oren",
                        Birthday = new DateTime(1234, 5, 6, 7, 8, 9),
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                                       .Where(x => x.Birthday.ToString().StartsWith("1234"))
                                       .Select(u => new
                                       {
                                           u.Name,
                                           Birthday = u.Birthday.ToString()
                                       });

                    Assert.Equal("from Users as u where startsWith(u.Birthday, $p0) " +
                                 "select { Name : u.Name, Birthday : new Date(Date.parse(u.Birthday)).toString() }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Oren", queryResult[0].Name);
                    Assert.Equal("Sat May 06 1234 07:08:09 GMT+00:00", queryResult[0].Birthday);

                }
            }
        }

        [Fact]
        public void Custom_Functions_Linq_Methods_Support()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "Jerry",
                    Roles = new[] {"1", "1", "2", "2", "2", "3", "4"},
                    Details = new List<Detail>
                    {
                        new Detail { Number = 19},
                        new Detail { Number = -25},
                        new Detail { Number = 27},
                        new Detail { Number = 6},
                    }
                };

                var roles = new[] { "a", "b", "c" };

                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new
                        {
                            LastOrDefault = u.Roles.LastOrDefault(),
                            LastOrDefaultWithPredicate = u.Roles.LastOrDefault(x => x != "4"),
                            Take = u.Roles.Take(2),
                            Skip = u.Roles.Skip(2),
                            Max = u.Roles.Max(),
                            MaxWithSelector = u.Details.Max(d => d.Number),
                            Min = u.Roles.Min(),
                            MinWithSelector = u.Details.Min(d => d.Number),
                            Reverse = u.Roles.Reverse(),
                            IndexOf = u.Roles.ToList().IndexOf("3"),
                            Concat = u.Roles.Concat(roles),
                            Distinct = u.Roles.Distinct(),
                            ElementAt = u.Details.Select(x=>x.Number).ElementAt(2)

                        });

                    Assert.Equal("from Users as u select { " +
                                 "LastOrDefault : u.Roles.slice(-1)[0], " +
                                 "LastOrDefaultWithPredicate : u.Roles.slice().reverse().find(function(x){return x!==\"4\";}), " +
                                 "Take : u.Roles.slice(0, 2), " +
                                 "Skip : u.Roles.slice(2, u.Roles.length), " +
                                 "Max : u.Roles.reduce(function(a, b) { return Raven_Max(a, b);}), " +
                                 "MaxWithSelector : u.Details.map(function(d){return d.Number;}).reduce(function(a, b) { return Raven_Max(a, b);}, 0), " +
                                 "Min : u.Roles.reduce(function(a, b) { return Raven_Min(a, b);}), " +
                                 "MinWithSelector : u.Details.map(function(d){return d.Number;}).reduce(function(a, b) { return Raven_Min(a, b);}, 0), " +
                                 "Reverse : u.Roles.slice().reverse(), " +
                                 "IndexOf : u.Roles.indexOf(\"3\"), " +
                                 "Concat : u.Roles.concat($p0), " +
                                 "Distinct : u.Roles.filter((value, index) => u.Roles.indexOf(value) == index), " +
                                 "ElementAt : u.Details.map(function(x){return x.Number;})[2] }"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(user.Roles.LastOrDefault(), queryResult[0].LastOrDefault);
                    Assert.Equal(user.Roles.LastOrDefault(x => x != "4"), queryResult[0].LastOrDefaultWithPredicate);
                    Assert.Equal(user.Roles.Take(2), queryResult[0].Take);
                    Assert.Equal(user.Roles.Skip(2), queryResult[0].Skip);
                    Assert.Equal(user.Roles.Max(), queryResult[0].Max);
                    Assert.Equal(user.Details.Max(d => d.Number), queryResult[0].MaxWithSelector);
                    Assert.Equal(user.Roles.Min(), queryResult[0].Min);
                    Assert.Equal(user.Details.Min(d => d.Number), queryResult[0].MinWithSelector);
                    Assert.Equal(user.Roles.ToList().IndexOf("3"), queryResult[0].IndexOf);
                    Assert.Equal(user.Roles.Concat(roles), queryResult[0].Concat);
                    Assert.Equal(user.Roles.Distinct(), queryResult[0].Distinct);
                    Assert.Equal(user.Details.Select(x => x.Number).ElementAt(2), queryResult[0].ElementAt);

                }
            }
        }

        [Fact]
        public void Can_Load_With_Argument_That_Has_Computation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
                        DetailShortId = "1-A"
                    }, "users/1");
                    session.Store(new Detail
                    {
                        Number = 15
                    }, "details/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.LastName == "Garcia"
                                let detail = session.Load<Detail>("details/" + u.DetailShortId)
                                select new
                                {
                                    Name = u.Name,
                                    Detail = detail
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var detail = load((""details/""+u.DetailShortId));
	return { Name : u.Name, Detail : detail };
}
from Users as u where u.LastName = $p0 select output(u)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(15, queryResult[0].Detail.Number);

                }
            }
        }
        
        [Fact]
        public void Can_Project_With_JsonPropertyAttribute()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var ids = new[] {"ids/1"};

                var projection =
                    from s in session.Query<Document>().Where(x => x.Id.In(ids))
                    select new
                    {
                        Id = s.Id,
                        Results = s.Results.Select(x => new
                            {
                                ResultValue = x.ResultValue
                            })
                            .ToArray()
                    };

                projection.ToList();
            }
        }

        [Fact]
        public void Can_Project_With_Json_Property_Rename()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Number = 5
                });

                session.SaveChanges();

                var projection =
                    from s in session.Query<Document>()
                    select new
                    {
                        Result = s.Number * 2
                    };

                Assert.Equal("from Documents as s select { Result : s.Foo*2 }", projection.ToString());

                var result = projection.ToList();

                Assert.Equal(10, result[0].Result);
            }
        }
        
        [Fact]
        public void Can_Project_Where_Id_StartsWith()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1");
                session.Store(new User(), "users/2");
                session.Store(new User(), "bunny/1");
                session.Store(new User(), "bunny/2");
                session.Store(new User(), "bunny/3");

                session.SaveChanges();

                var query = session.Query<User>().Where(u => u.Id.StartsWith("bunny")).ToList();
                
                Assert.Equal(3, query.Count);
            }
        }
        
        [Fact]
        public void Can_Use_DefaultIfEmpty() 
        {
            using (var store = GetDocumentStore ())
            {
                var lists = new Lists
                {
                    Strings = new List<string>(),
                    Bools = new List<bool>(),
                    Chars = new List<char>(),
                    Ints = new List<int>(),
                    Longs = new List<long>(),
                    Decimals = new List<decimal>(),
                    Doubles = new List<double>(),
                    Users = new List<User>()
                };
                
                using (var session = store.OpenSession())
                {
                    session.Store(lists);           
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var query = from l in session.Query<Lists>()
                                select new 
                                {
                                      Strings = l.Strings.DefaultIfEmpty(),
                                      Bools = l.Bools.DefaultIfEmpty(),
                                      Chars = l.Chars.DefaultIfEmpty(),
                                      Ints = l.Ints.DefaultIfEmpty(),
                                      Longs = l.Longs.DefaultIfEmpty(),                                     
                                      Decimals = l.Decimals.DefaultIfEmpty(),
                                      Doubles = l.Doubles.DefaultIfEmpty(),
                                      Users = l.Users.DefaultIfEmpty()                                  
                                };                   
                    
                    var result = query.ToList();
                    
                    Assert.Equal(lists.Strings.DefaultIfEmpty() , result[0].Strings);
                    Assert.Equal(lists.Bools.DefaultIfEmpty() , result[0].Bools);
                    Assert.Equal(lists.Chars.DefaultIfEmpty() , result[0].Chars);
                    Assert.Equal(lists.Ints.DefaultIfEmpty() , result[0].Ints);
                    Assert.Equal(lists.Longs.DefaultIfEmpty() , result[0].Longs);
                    Assert.Equal(lists.Decimals.DefaultIfEmpty() , result[0].Decimals);
                    Assert.Equal(lists.Doubles.DefaultIfEmpty() , result[0].Doubles);
                    Assert.Equal(lists.Users.DefaultIfEmpty() , result[0].Users);
                }
            }
        }
               					
        [Fact]
        public void Custom_Functions_With_SelectMany()
        {
            using (var store = GetDocumentStore())
            {
                var nestedNode = new Node
                {
                    Name = "Parent",
                    Children = Enumerable.Range(0, 10).Select(x => new Node()
                    {
                        Name = "Child" + x,
                        Children = Enumerable.Range(0, 5).Select(y => new Node()
                        {
                            Name = "Grandchild" + (x * 5 + y),
                            Children = null
                        }).ToList()
                    }).ToList()
                };

                var simpleNode = new Node
                {
                    Name = "ChildlessParent",
                    Children = null
                };
                
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Node>().Select(node => new
                    {
                        Grandchildren = node.Children.SelectMany(x => x.Children).ToList()
                    });

                    Assert.Equal("from Nodes as node select " +
                                 "{ Grandchildren : node.Children.reduce(function(a, b) { return a.concat((function(x){return x.Children;})(b)); }, []) }"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    
                    Assert.Equal(50, queryResult[0].Grandchildren.Count);
                    Assert.Null(queryResult[1].Grandchildren);
                    
                    for (var i = 0; i < 50; i++)
                    {
                        Assert.Equal("Grandchild" + i, queryResult[0].Grandchildren[i].Name);
                    }
                }
            }
        }
                       
        [Fact]
        public async Task Can_SelectMany_From_Dictionary() 
        {
            using (var store = GetDocumentStore ()) 
            {
                using (var session = store.OpenAsyncSession ()) 
                {
                    var testable = new TestableDTO 
                    {
                        Data = new Dictionary<string, IList<string>> 
                        {
                            { "a", new List<string> { "a1", "a2", "a3" } },
                            { "b", new List<string> { "b1", "b2", "b3" } }
                        }
                    };
                                                   
                    await session.StoreAsync(testable);                   
                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession ()) 
                {
                    var query = from item in session.Query<TestableDTO>()
                                select new 
                                {
                                    Id = item.Id,
                                    data = item.Data,
                                    values = item.Data.SelectMany(c => c.Value)
                                                      .Select(n => n)
                                                      .DefaultIfEmpty()
                                                      .ToList()
                                };
                    
                    Assert.Equal("from TestableDTOs as item select { " +
                                 "Id : id(item), " +
                                 "data : item.Data, " +
                                 "values : (function(arr){return arr.length > 0 ? arr : [null]})" +
                                             "(Object.getOwnPropertyNames(item.Data)" +
                                                    ".map(function(k){return item.Data[k]})" +
                                                    ".reduce(function(a, b) { return a.concat(b);},[])" +
                                                    ".map((function(n){return n;}))) }"
                                , query.ToString());
                    
                    var first = await query.FirstAsync();
                    
                    Assert.Collection ( first.values,
                        i => Assert.Equal ( "a1", i ),
                        i => Assert.Equal ( "a2", i ),
                        i => Assert.Equal ( "a3", i ),
                        i => Assert.Equal ( "b1", i ),
                        i => Assert.Equal ( "b2", i ),
                        i => Assert.Equal ( "b3", i ) );
                }
            }
        }
        
        [Fact]
        public void Custom_Fuctions_With_Nested_Loads_Simple()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Detail { Number = 12345 }, "detail/1");
                    session.Store(new Detail { Number = 67890 }, "detail/2");

                    session.Store(new User { Name = "Jerry", DetailId = "detail/1", FriendId = "users/2"}, "users/1");
                    session.Store(new User { Name = "Bob", DetailId = "detail/2", FriendId = "users/1"}, "users/2");
                                                                             
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let detail = session.Load<Detail>(user.DetailId)
                                let friend = session.Load<User>(user.FriendId)
                                let friendsDetail = session.Load<Detail>(friend.DetailId)
                                select new
                                {
                                    user.Name,
                                    Mine = detail.Number,
                                    Friends = friendsDetail.Number
                                };

                    Assert.Equal("from Users as user " +
                                 "load user.DetailId as detail, user.FriendId as friend, friend.DetailId as friendsDetail " +
                                 "select { Name : user.Name, " +
                                          "Mine : detail.Number, " +
                                          "Friends : friendsDetail.Number }"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    
                    Assert.Equal("Jerry" ,queryResult[0].Name);
                    Assert.Equal(12345 ,queryResult[0].Mine);
                    Assert.Equal(67890 ,queryResult[0].Friends);
                    
                    Assert.Equal("Bob" ,queryResult[1].Name);
                    Assert.Equal(67890 ,queryResult[1].Mine);
                    Assert.Equal(12345 ,queryResult[1].Friends);
                }
            }
        }
        
        [Fact]
        public void Custom_Fuctions_With_Nested_Loads_Complex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {                    
                    session.Store(new Company
                    {
                        Name = "GD",
                        EmployeesIds = new List<string> {"employees/1","employees/2", "employees/3"}
                    }, "companies/1");
                    
                    session.Store(new Employee
                    {
                        FirstName = "Bob",
                        LastName = "Weir",
                        ReportsTo = "employees/2"
                    }, "employees/1");
                    
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        LastName = "Garcia"
                    }, "employees/2");
                    
                    session.Store(new Order
                    {
                        OrderedAt = new DateTime(1942,8,1),
                        Company = "companies/1",
                    }, "orders/1");
                                                         
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let company = session.Load<Company>(o.Company)
                                let employee = RavenQuery.Load<Employee>(company.EmployeesIds).FirstOrDefault()
                                let manager = session.Load<Employee>(employee.ReportsTo)
                                select new
                                {
                                    Company = company.Name,
                                    Employee = employee.FirstName + " " + employee.LastName,
                                    Manager = manager.FirstName + " " + manager.LastName
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, company, _docs_1) {
	var employee = _docs_1[0];
	var manager = load(employee.ReportsTo);
	return { Company : company.Name, Employee : employee.FirstName+"" ""+employee.LastName, Manager : manager.FirstName+"" ""+manager.LastName };
}
from Orders as o load o.Company as company, company.EmployeesIds as _docs_1[] select output(o, company, _docs_1)" ,query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    
                    Assert.Equal("GD" ,queryResult[0].Company);
                    Assert.Equal("Bob Weir" ,queryResult[0].Employee);
                    Assert.Equal("Jerry Garcia" ,queryResult[0].Manager);
                }
            }
        }
        
        [Fact]
        public void Can_Load_SingleDocument_When_Declare()
        {
            //RavenDB-9637
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Configuration() { Language = "nl" }, "configuration/global");
                    session.Store(new Group()
                    {
                        Name = new Dictionary<string, string>(){
                            { "en", "Administrator" },
                            { "nl", "Administratie" }
                        }
                    }, "groups/1");
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Groups = new List<string>() { "groups/1" } }, "users/1");
                    session.Store(new User { Name = "John", LastName = "Doe", Groups = null}, "users/2");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                        let configuration = RavenQuery.Load<Configuration>("configuration/global")
                        let test = 1    // This will create a (declare) function
                        let groups = RavenQuery.Load<Group>(user.Groups)
                        select new
                        {                           
                            Language = configuration.Language
                        };


                    var queryResult = query.ToList();

                    Assert.Equal("nl", queryResult[0].Language);
                }
            }
        }        
                
        [Fact]
        public void Can_Load_Old_Document_With_Undefined_Member()
        {
            //RavenDB-9638

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Configuration() { Language = "nl" }, "configuration/global");
                    session.Store(new Group()
                    {
                        Name = new Dictionary<string, string>(){
                            { "en", "Administrator" },
                            { "nl", "Administratie" }
                        }
                    }, "groups/1");
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Groups = new List<string>() { "groups/1" } }, "users/1");
                    session.Store(new User { Name = "John", LastName = "Doe", Groups = null}, "users/2");
                    session.Store(new OldUser.User { Name = "Bob", LastName = "Weir" }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryResult = (from user in session.Query<User>()
                                       let groups = RavenQuery.Load<Group>(user.Groups)
                                       select new
                                       {
                                           Language = groups.Select(a => a.Name)                                          
                                       }).ToList();
                                        
                    Assert.NotEmpty(queryResult[0].Language);
                    Assert.Empty(queryResult[1].Language);
                    Assert.Empty(queryResult[2].Language);
                }
            }
        }
        
        [Fact]
        public void Can_Do_Null_Comparison_On_Undefined_Member()
        {                      
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia", Groups = new List<string>() { "groups/1" } }, "users/1");
                    session.Store(new User { Name = "John", LastName = "Doe", Groups = null}, "users/2");
                    session.Store(new OldUser.User { Name = "Bob", LastName = "Weir" }, "users/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    HasGroups = user.Groups != null
                                };
                    
                    Assert.Equal("from Users as user select " +
                                 "{ HasGroups : user.Groups!=null }", query.ToString());
                    
                    var queryResult = query.ToList();
                    
                    Assert.True(queryResult[0].HasGroups);
                    Assert.False(queryResult[1].HasGroups);
                    Assert.False(queryResult[2].HasGroups);
                }
            }
        }
        
        [Fact]
        public void IsNullOrEmptySupport()
        {                      
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" });
                    session.Store(new User { Name = "Bob", LastName = "" });
                    session.Store(new User { Name = "Phil"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Name = string.IsNullOrEmpty(user.LastName) ? user.Name : user.LastName,
                                };
                    
                    Assert.Equal("from Users as user " +
                                 "select { Name : (user.LastName == null || user.LastName === \"\")?user.Name:user.LastName }", query.ToString());
                    
                    var queryResult = query.ToList();
                    
                    Assert.Equal(3, queryResult.Count);
                    
                    Assert.Equal("Garcia", queryResult[0].Name);
                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal("Phil", queryResult[2].Name);

                }
            }
        }
        
        [Fact]
        public void IsNullOrWhitespaceSupport()
        {                      
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" });
                    session.Store(new User { Name = "Bob", LastName = " " });
                    session.Store(new User { Name = "Phil"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Name = string.IsNullOrWhiteSpace(user.LastName) ? user.Name : user.LastName,
                                };
                    
                    Assert.Equal("from Users as user " +
                                 "select { Name : (!user.LastName || !user.LastName.trim())?user.Name:user.LastName }", query.ToString());
                    
                    var queryResult = query.ToList();
                    
                    Assert.Equal(3, queryResult.Count);
                    
                    Assert.Equal("Garcia", queryResult[0].Name);
                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal("Phil", queryResult[2].Name);
                }
            }
        }
        
        [Fact]
        public void CanProjectWithEnumerableCount()
        {               
            //http://issues.hibernatingrhinos.com/issue/RDBC-99
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", Roles = new []{ "1", "2" }});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {                   
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    user.Name,
                                    NumberOfRoles = user.Roles.Count()
                                };
                    
                    Assert.Equal("from Users as user " +
                                 "select { Name : user.Name, NumberOfRoles : user.Roles.length }", 
                                query.ToString());
                    
                    var queryResult = query.ToList();
                    
                    Assert.Equal(1, queryResult.Count);                    
                    Assert.Equal(2, queryResult[0].NumberOfRoles);
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

                        Assert.Equal("from Documents as d where (id() in ($p0)) and (d.Deleted = $p1) " +
                                     "select { Id : id(d), Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return $p2.length===0||$p3.indexOf(x.TargetId)>=0;}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
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

                        Assert.Equal("from Documents as d where (id() in ($p0)) and (d.Deleted = $p1) " +
                                     "select { Id : id(d), Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return $p2.length===0||$p3.indexOf(x.TargetId)>=0;}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
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

                        Assert.Equal("from Documents as d where (id() in ($p0)) and (d.Deleted = $p1) " +
                                     "select { Id : id(d), Deleted : d.Deleted, " +
                                     "Values : d.SubDocuments.filter(function(x){return $p2==null||x.TargetId===$p3;}).map(function(x){return {TargetId:x.TargetId,TargetValue:x.TargetValue};}) }"
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

        private class Linked
        {
            public List<User> Users;
            public string Name;
            public Linked Next;
        }

        private class UserGroup
        {
            public List<User> Users { get; set; }
            public string Name { get; set; }
        }
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string LastName { get; set; }
            public DateTime Birthday { get; set; }
            public int IdNumber { get; set; }
            public bool IsActive { get; set; }
            public string[] Roles { get; set; }
            public string DetailId { get; set; }
            public string FriendId { get; set; }
            public IEnumerable<string> DetailIds { get; set; }
            public List<Detail> Details { get; set; }
            public string DetailShortId { get; set; }
            public List<string> Groups { get; set; }

        }
        private class Detail
        {
            public int Number { get; set; }
        }
        private class QueryResult
        {
            public string FullName { get; set; }
        }
        private class RawQueryResult
        {
            public string Name { get; set; }
            public Detail Detail { get; set; }
        }
        private class IndexQueryResult
        {
            public string Name { get; set; }
            public string Friend { get; set; }
        }

        private class Document
        {
            public string Id { get; set; }
            public Result[] Results { get; set; }

            [JsonProperty(PropertyName = "Foo")]
            public int Number { get; set; }
        }

        private class Result
        {
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public decimal? ResultValue { get; set; }                
        }
        
        private class Lists
        {
            public List<string> Strings { get; set; }
            public List<bool> Bools { get; set; }           
            public List<char> Chars { get; set; }
            public List<int> Ints { get; set; }
            public List<long> Longs { get; set; }
            public List<decimal> Decimals { get; set; }
            public List<double> Doubles { get; set; }
            public List<User> Users { get; set; }           
        }
                
        private class Node
        {
            public string Name { get; set; }
            public List<Node> Children = new List<Node>();
        }
        
        private class TestableDTO 
        {
            public string Id { get; set; }
            public IDictionary<string, IList<string>> Data { get; set; } 
        }
        
        private class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string ReportsTo { get; set; }
        }
        
        private class Configuration
        {
            public string Language { get; set; }
        }

        private class Group
        {
            // Multilanguage name
            public Dictionary<string, string> Name { get; set; }
        }
        
        private class OldUser
        {
            public class User
            {
                public string Name { get; set; }
                public string LastName { get; set; }
            }
        }
        
    }
}

