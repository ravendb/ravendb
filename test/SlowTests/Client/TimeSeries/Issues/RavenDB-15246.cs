using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15246 : RavenTestBase
    {
        public RavenDB_15246(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestClientCacheWithPageSize()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1-A");
                    var tsf = session.TimeSeriesFor("users/1-A", "Heartrate");
                    for (int i = 0; i <= 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var ts = session.TimeSeriesFor(user, "Heartrate");

                    var res = ts.Get(pageSize: 0);
                    Assert.Null(res);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 10);
                    Assert.Equal(10, res.Length);

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 7);
                    Assert.Equal(7, res.Length);

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 20);
                    Assert.Equal(20, res.Length);

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 25);
                    Assert.Equal(20, res.Length);

                    Assert.Equal(5, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void TestClientCacheWithStart()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1-A");
                    var tsf = session.TimeSeriesFor("users/1-A", "Heartrate");
                    for (int i = 0; i < 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var ts = session.TimeSeriesFor(user, "Heartrate");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var res = ts.Get(start: 20);
                    Assert.Null(res);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    res = ts.Get(start: 10);
                    Assert.Equal(10, res.Length);
                    Assert.Equal(res[0].Timestamp, baseline.AddMinutes(10), RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    res = ts.Get(start: 0);
                    Assert.Equal(20, res.Length);
                    Assert.Equal(res[0].Timestamp, baseline, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(res[10].Timestamp, baseline.AddMinutes(10), RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    res = ts.Get(start: 10);
                    Assert.Equal(10, res.Length);
                    Assert.Equal(res[0].Timestamp, baseline.AddMinutes(10), RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    res = ts.Get(start: 20);
                    Assert.Null(res);

                }
            }
        }

        [Fact]
        public async Task GetResultsWithRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    var tsf = session.TimeSeriesFor("orders/1-A", "Heartrate");
                    for (int i = 0; i < 8; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 64d }, "watches/apple");
                    }

                    session.SaveChanges();


                    var db = await GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            "orders/1-A",
                            new[] { "Heartrate", "Heartrate", "Heartrate" },
                            new[]
                            {
                                baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                            },
                            new[]
                            {
                                baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                            },

                            0, 10);

                        var range = tsr["Heartrate"];
                        Assert.Equal(3, range.Count);

                        Assert.Equal(4, range[0].Entries.Length);
                        Assert.Equal(4, range[1].Entries.Length);
                        Assert.Equal(0, range[2].Entries.Length);

                    }


                    tsf = session.TimeSeriesFor("orders/1-A", "Heartrate");
                    for (int i = 8; i < 11; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 1000d }, "watches/apple");
                    }

                    session.SaveChanges();

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            "orders/1-A",
                            new[] { "Heartrate", "Heartrate", "Heartrate" },
                            new[]
                            {
                                baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                            },
                            new[]
                            {
                                baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                            },

                            0, 10);

                        var range = tsr["Heartrate"];
                        Assert.Equal(3, range.Count);

                        Assert.Equal(4, range[0].Entries.Length);
                        Assert.Equal(4, range[1].Entries.Length);
                        Assert.Equal(2, range[2].Entries.Length);
                    }
                }
            }

        }

        [Fact]
        public async Task ResultsWithRangeAndPageSize()
        {
            using (var store = GetDocumentStore())
            {
                var tag = "raven";
                var id = "users/1";
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, tag);
                    for (int i = 0; i <= 15; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                StringValues fromRangeList = new[]
                    {
                        baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                    };
                StringValues toRangeList = new[]
                    {
                        baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                    };

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                        ctx,
                        id,
                        new[] { tag, tag, tag },
                        fromRangeList,
                        toRangeList,
                        0, 0);

                    var range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(0, range[0].Entries.Length);
                    Assert.Equal(0, range[1].Entries.Length);
                    Assert.Equal(0, range[2].Entries.Length);

                    tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                        ctx,
                        id,
                        new[] {tag, tag, tag},
                        fromRangeList,
                        toRangeList,
                        0, 30);

                    range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(4, range[0].Entries.Length);
                    Assert.Equal(4, range[1].Entries.Length);
                    Assert.Equal(4, range[2].Entries.Length);

                    tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                        ctx,
                        id,
                        new[] { tag, tag, tag },
                        fromRangeList,
                        toRangeList,
                        0, 6);

                    range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(4, range[0].Entries.Length);
                    Assert.Equal(2, range[1].Entries.Length);
                    Assert.Equal(0, range[2].Entries.Length);
                }
            }
        }

        [Fact]
        public async Task ResultsWithRangeAndStart()
        {
            using (var store = GetDocumentStore())
            {
                var tag = "raven";
                var id = "users/1";
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, tag);
                    for (int i = 0; i <= 15; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                StringValues fromRangeList = new[]
                    {
                        baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                    };
                StringValues toRangeList = new[]
                    {
                        baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                        baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                    };

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                        ctx,
                        id,
                        new[] { tag, tag, tag },
                        fromRangeList,
                        toRangeList,
                        0, 20);

                    var range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(4, range[0].Entries.Length);
                    Assert.Equal(4, range[1].Entries.Length);
                    Assert.Equal(4, range[2].Entries.Length);

                    tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            id,
                            new[] { tag, tag, tag },
                            fromRangeList,
                            toRangeList,
                            3, 20);

                    range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(1, range[0].Entries.Length);
                    Assert.Equal(4, range[1].Entries.Length);
                    Assert.Equal(4, range[2].Entries.Length);

                    tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                        ctx,
                        id,
                        new[] { tag, tag, tag },
                        fromRangeList,
                        toRangeList,
                        9, 20);

                    range = tsr[tag];

                    Assert.Equal(3, range.Count);

                    Assert.Equal(0, range[0].Entries.Length);
                    Assert.Equal(0, range[1].Entries.Length);
                    Assert.Equal(3, range[2].Entries.Length);
                }

            }
        }
    }
}
