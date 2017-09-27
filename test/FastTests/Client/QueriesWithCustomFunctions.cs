using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
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
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { FullName = u.Name + " " + u.LastName, FirstName = u.Name});

                    Assert.Equal("FROM Users as u SELECT { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

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
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new { FullName = u.Name + " " + u.LastName, FirstName = u.Name });

                    Assert.Equal("FROM Users as u SELECT { FullName : u.Name+\" \"+u.LastName, FirstName : u.Name }", query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Jerry", queryResult[0].FirstName);

                }
            }
        }

        [Fact]
        public void Can_Define_Custom_Functions_With_Timespan()
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

                    Assert.Equal("FROM Users as u SELECT { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date().setHours(0,0,0,0)-Date.parse(u.Birthday)) }",
                                query.ToString());

                    var queryResult = query.ToList();

                    var ts = DateTime.Today - new DateTime(1942, 8, 1);

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(ts, queryResult[0].Age);
                }
            }
        }

        [Fact]
        public async Task Can_Define_Custom_Functions_With_Timespan_Async()
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

                    Assert.Equal("FROM Users as u SELECT { Name : u.Name, Age : convertJsTimeToTimeSpanString(new Date().setHours(0,0,0,0)-Date.parse(u.Birthday)) }",
                        query.ToString());

                    var queryResult = await query.ToListAsync();

                    var ts = DateTime.Today - new DateTime(1942, 8, 1);

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(ts, queryResult[0].Age);
                }
            }
        }

        [Fact]
        public void Can_Define_Custom_Functions_With_DateTime_Properties()
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
        public async Task Can_Define_Custom_Functions_With_DateTime_Properties_Async()
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
        public void Can_Define_Custom_Functions_With_Numbers_And_Booleans()
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
        public async Task Can_Define_Custom_Functions_With_Numbers_And_Booleans_Async()
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

        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public DateTime Birthday { get; set; }
            public int IdNumber { get; set; }
            public bool IsActive { get; set; }
        }
    }
}

