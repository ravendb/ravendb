using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_14541 : RavenTestBase
{
    public RavenDB_14541(ITestOutputHelper output) : base(output)
    {
    }

    private class Address
    {
        public string CountryState { get; set; }
        public string City { get; set; }
        public string StateId;
    }

    private class State
    {
        public string Name { get; set; }
    }

    private class QueryResult
    {
        public string Comapny { get; set; }
    }

    private class User
    {
        public string UserName { get; set; }
        public string? StateId;
        public string? CityId;
    }

    private class City
    {
        public string Name { get; set; }
    }

    [Fact]
    public void ShouldThrowNotSupportedExceptionHaveIncludesInsteadOf_()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Company = "1", Employee = "employees/1" }, "orders/1-A");
                session.Store(new Order { Company = "2", Employee = "employees/2" }, "orders/2-A");

                session.Store(new Employee { FirstName = "a" }, "employees/1");
                session.Store(new Employee { FirstName = "b" }, "employees/2");

                session.SaveChanges();
            }

            Assert.Throws<NotSupportedException>(() =>
            {
                using (var session = store.OpenSession())
                {
                    var query3 = from u in session.Query<Order>()
                        let includes = RavenQuery.Include<Order>(u.Employee)
                        select new QueryResult { Comapny = u.Company };

                    var results = query3.ToList();
                }
            });
        }
    }

    [Fact]
    public void ShouldThrowNotSupportedExceptionHaveIncorrectObjectAsArg()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            Assert.Throws<NotSupportedException>(() =>
            {
                using (var session = store.OpenSession())
                {
                    var query3 = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(a.StateId)
                        select new { Name = a.City };

                    var res = query3.ToList();
                }
            });
        }
    }

    [Fact]
    public void SessionQuerySelectAddressFromIncludeDoc_UsingRavenQueryStringWithStateObject()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query3 = from a in session.Query<Address>()
                    let _ = RavenQuery.Include<State>("a.StateId")
                    select new { Name = a.City };
                var res2 = query3.ToList();

                var doc1 = session.Load<State>("states/1");
                var doc2 = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeDoc_UsingRavenQueryWithoutStateObject()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query3 = from a in session.Query<Address>()
                    let _ = RavenQuery.Include("a.StateId")
                    select new { Name = a.City };
                var res2 = query3.ToList();

                var doc1 = session.Load<State>("states/1");
                var doc2 = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQuerySelectAdressFromIncludeDoc_UsingRavenQueryWithComplexLambdaExpression()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var address = session.Include<Address>(x => x.CountryState.Split('#', StringSplitOptions.None)[0]).ToString();
            }

            using (var session = store.OpenSession())
            {
                var query3 = from a in session.Query<Address>()
                    let _ = RavenQuery.Include<Address>(x => x.CountryState.Split('#', StringSplitOptions.None)[0])
                    select new { Name = a.City };
                var res1 = query3.ToString();
                var res2 = query3.ToList();
                Assert.Equal(2, res2.Count);
                Assert.Equal("new-york", res2[0].Name);
                Assert.Equal("haifa", res2[1].Name);

                var doc1 = session.Load<State>("states/1");
                var doc2 = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQuerySelectAdressFromIncludeDoc_UsingRavenQueryWithSimpleLambdaExpression()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query3 = from a in session.Query<Address>()
                    let _ = RavenQuery.Include<Address>(x => x.StateId)
                    select new { Name = a.City };
                var res1 = query3.ToString();
                var res2 = query3.ToList();

                Assert.Equal(2, res2.Count);
                Assert.Equal("new-york", res2[0].Name);
                Assert.Equal("haifa", res2[1].Name);

                var doc1 = session.Load<State>("states/1");
                var doc2 = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeTimeSeriesUsingRavenQuery()
    {
        var baseline = DateTime.Today;

        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "nur" }, "user/nur");
                session.Store(new User { UserName = "arik" }, "user/arik");

                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(2), 5, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(3), 12, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(4), 15, "views/nur");

                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(9), 3, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(2), 19, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(3), 80, "views/arik");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var start = baseline;
                var end = baseline.AddHours(5);

                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    select new { Name = a.UserName };

                var AsString = query.ToString();
                var result = query.ToList();

                var nur = session.TimeSeriesFor("user/nur", "CurViews")
                    .Get(start, end);

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var arik = session.TimeSeriesFor("user/arik", "CurViews")
                    .Get(start, end);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeTimeSeriesUsingRavenQuery_useingTwoLets()
    {
        var baseline = DateTime.Today;

        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "nur" }, "user/nur");
                session.Store(new User { UserName = "arik" }, "user/arik");

                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(3), 5, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(4), 12, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(5), 15, "views/nur");

                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(3), 99, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(4), 420, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(5), 66, "views/nur");

                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(3), 3, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(4), 19, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(5), 80, "views/arik");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var start = baseline;
                var end = baseline.AddHours(5);

                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    let name = a.UserName
                    let __ = RavenQuery.IncludeTimeSeries(a, "CurViews2")
                    select new { Name = name };


                var AsString = query.ToString();
                var result = query.ToList();

                var nur = session.TimeSeriesFor("user/nur", "CurViews")
                    .Get(start, end);
                var nur2 = session.TimeSeriesFor("user/nur", "CurViews2")
                    .Get(start, end);
                var arik = session.TimeSeriesFor("user/arik", "CurViews")
                    .Get(start, end);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeTimeSeriesUsingRavenQuery_ShouldThrow()
    {
        var baseline = DateTime.Today;

        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "nur" }, "user/nur");
                session.Store(new User { UserName = "arik" }, "user/arik");

                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(2), 5, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(3), 12, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(4), 15, "views/nur");

                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(2), 100, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(3), 150, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews2").Append(baseline.AddHours(4), 225, "views/nur");

                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(9), 3, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(2), 19, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(3), 80, "views/arik");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var start = baseline;
                var end = baseline.AddHours(5);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    var query = from a in session.Query<User>()
                        let thrower = RavenQuery.IncludeTimeSeries(a, "CurViews2")
                        select new { Name = a.UserName };

                    var AsString = query.ToString();
                    var result = query.ToList();
                });
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeCountersUsingRavenQuery_basicCase()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "Tucker" }, "user/tucker");
                session.Store(new User { UserName = "Kevin" }, "user/kevin");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeCounter(a, "shoes")
                    select new { Name = a.UserName };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerShoes = session.CountersFor("user/tucker");
                var tuckerCounters = tuckerShoes.Get("shoes");

                var kevinShoes = session.CountersFor("user/kevin");
                var kevinshoes = kevinShoes.Get("shoes");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeCountersUsingRavenQuery_stringArrayCase()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "Tucker" }, "user/tucker");
                session.Store(new User { UserName = "Kevin" }, "user/kevin");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            string[] retrive = new[] { "shirts", "shoes" };
            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeCounters(a, retrive)
                    select new { Name = a.UserName };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerShoes = session.CountersFor("user/tucker");
                var tuckerCounters = tuckerShoes.Get("shoes");

                var kevinShoes = session.CountersFor("user/kevin");
                var kevinshoes = kevinShoes.Get("shoes");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryIncludeAllCountersUsingRavenQuery_basicCase()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "Tucker" }, "user/tucker");
                session.Store(new User { UserName = "Kevin" }, "user/kevin");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeAllCounters(a)
                    select new { Name = a.UserName };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerShoes = session.CountersFor("user/tucker");
                var tuckerCounters = tuckerShoes.Get("shoes");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryAllIncludeVariantsInRavenQuery()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            var baseline = DateTime.Today;

            var start = baseline;
            var end = baseline.AddHours(5);

            using (var session = store.OpenSession())
            {
                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.Store(new User { UserName = "Tucker", StateId = "states/1" }, "user/tucker");
                session.Store(new User { UserName = "Kevin", StateId = "states/2" }, "user/kevin");


                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(2), 55, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(3), 12, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(4), 15, "views/Tucker");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeAllCounters(a)
                    let __ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    let ___ = RavenQuery.Include<State>("a.StateId")
                    select new { Name = a.UserName };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerCounterGetter = session.CountersFor("user/tucker");

                var tuckerShoes = tuckerCounterGetter.Get("shoes");

                var tuckerTimeSeries = session.TimeSeriesFor("user/tucker", "CurViews");

                var kevinState = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryAllIncludevariantsInRavenQuery_ShouldThrow()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            var baseline = DateTime.Today;

            var start = baseline;
            var end = baseline.AddHours(5);

            using (var session = store.OpenSession())
            {
                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.Store(new User { UserName = "Tucker", StateId = "states/1" }, "user/tucker");
                session.Store(new User { UserName = "Kevin", StateId = "states/2" }, "user/kevin");


                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(2), 55, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(3), 12, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(4), 15, "views/Tucker");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    var query = from a in session.Query<User>()
                        let _ = RavenQuery.IncludeAllCounters(a)
                        let __ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                        let a_ = RavenQuery.Include<State>("a.StateId")
                        select new { Name = a.UserName };

                    query.ToString();
                });
            }
        }
    }

    [Fact]
    public void SessionQueryAllIncludeVariantsInRavenQuery_withOtherLet()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            var baseline = DateTime.Today;

            var start = baseline;
            var end = baseline.AddHours(5);

            using (var session = store.OpenSession())
            {
                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.Store(new User { UserName = "Tucker", StateId = "states/1" }, "user/tucker");
                session.Store(new User { UserName = "Kevin", StateId = "states/2" }, "user/kevin");


                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(2), 55, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(3), 12, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(4), 15, "views/Tucker");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeAllCounters(a)
                    let __ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    let name = a.UserName
                    let ___ = RavenQuery.Include<State>("a.StateId")
                    select new { Name = name };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerCounterGetter = session.CountersFor("user/tucker");
                //var tuckerAllCounters = tuckerCounterGetter.GetAll();
                var tuckerShoes = tuckerCounterGetter.Get("shoes");
                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var tuckerTimeSeries = session.TimeSeriesFor("user/tucker", "CurViews");

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var kevinState = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryAllIncludeVariantsInRavenQuery_withOtherLetFirst()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            var baseline = DateTime.Today;

            var start = baseline;
            var end = baseline.AddHours(5);

            using (var session = store.OpenSession())
            {
                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.Store(new User { UserName = "Tucker", StateId = "states/1" }, "user/tucker");
                session.Store(new User { UserName = "Kevin", StateId = "states/2" }, "user/kevin");


                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(2), 55, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(3), 12, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(4), 15, "views/Tucker");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let name = a.UserName
                    let _ = RavenQuery.IncludeAllCounters(a)
                    let __ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    let ___ = RavenQuery.Include<State>("a.StateId")
                    select new { Name = name };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerCounterGetter = session.CountersFor("user/tucker");
                var tuckerShoes = tuckerCounterGetter.Get("shoes");
                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var tuckerTimeSeries = session.TimeSeriesFor("user/tucker", "CurViews");

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var kevinState = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Fact]
    public void SessionQueryAllIncludeVariantsInRavenQuery_withLoaderLet()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            var baseline = DateTime.Today;

            var start = baseline;
            var end = baseline.AddHours(5);

            using (var session = store.OpenSession())
            {
                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.Store(new State { Name = "Hadera" }, "city/1");
                session.Store(new State { Name = "warsaw" }, "city/2");

                session.Store(new User { UserName = "Tucker", StateId = "states/1", CityId = "city/1" }, "user/tucker");
                session.Store(new User { UserName = "Kevin", StateId = "states/2", CityId = "city/2" }, "user/kevin");


                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(2), 55, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(3), 12, "views/Tucker");
                session.TimeSeriesFor("user/Tucker", "CurViews").Append(baseline.AddHours(4), 15, "views/Tucker");

                session.CountersFor("user/tucker").Increment("shoes", 100);
                session.CountersFor("user/tucker").Increment("shirts", 20);
                session.CountersFor("user/tucker").Increment("pants", 3);
                session.CountersFor("user/kevin").Increment("shoes", 5);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeAllCounters(a)
                    let __ = RavenQuery.IncludeTimeSeries(a, "CurViews")
                    let ___ = RavenQuery.Include<State>("a.StateId")
                    let city = RavenQuery.Load<City>(a.CityId)
                    select new { Name = a.UserName, HomeTown = city };

                var AsString = query.ToString();
                var result = query.ToList();

                var tuckerCounterGetter = session.CountersFor("user/tucker");
                var tuckerShoes = tuckerCounterGetter.Get("shoes");

                var tuckerTimeSeries = session.TimeSeriesFor("user/tucker", "CurViews");

                var kevinState = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }
}
