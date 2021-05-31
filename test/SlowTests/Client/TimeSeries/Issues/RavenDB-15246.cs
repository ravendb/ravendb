using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

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
                var baseline = RavenTestHelper.UtcToday;
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
                    Assert.Empty(res);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 10);
                    Assert.Equal(10, res.Length);

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 7);
                    Assert.Equal(7, res.Length);

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 20);
                    Assert.Equal(20, res.Length);

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    res = ts.Get(pageSize: 25);
                    Assert.Equal(21, res.Length);

                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void TestRanges()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "users/1-A";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, "raven");
                    for (int i = 0; i <= 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    for (int i = 12; i <= 13; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    for (int i = 16; i <= 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                var rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(1), To = baseline.AddMinutes(7)
                    }
                };
                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
                rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(8), To = baseline.AddMinutes(11)
                    }
                };

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
                rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(8), To = baseline.AddMinutes(17)
                    }
                };

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
                rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(14), To = baseline.AddMinutes(15)
                    }
                };

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
                rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(23), To = baseline.AddMinutes(25)
                    }
                };

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
                rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(20), To = baseline.AddMinutes(26)
                    }
                };

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["raven"].Count);
                }
            }
        }

        [Fact]
        public void TestClientCacheWithStart()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
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
                    Assert.Empty(res);
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
                    Assert.Empty(res);
                }
            }
        }

        [Fact]
        public void GetResultsWithRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "users/1-A";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, "raven");
                    for (int i = 0; i < 8; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 64d }, "watches/apple");
                    }

                    session.SaveChanges();

                    var rangesList = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "raven", From = baseline.AddMinutes(0), To = baseline.AddMinutes(3)
                        },
                        new TimeSeriesRange
                        {
                            Name = "raven", From = baseline.AddMinutes(4), To = baseline.AddMinutes(7)
                        },
                        new TimeSeriesRange
                        {
                            Name = "raven", From = baseline.AddMinutes(8), To = baseline.AddMinutes(11)
                        }
                    };

                    var re = store.GetRequestExecutor();

                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var tsCommand = new GetMultipleTimeSeriesOperation.
                            GetMultipleTimeSeriesCommand(id, rangesList, 0, 10);
                        re.Execute(tsCommand, context);
                        var res = tsCommand.Result;

                        Assert.Equal(1, res.Values.Count);
                        Assert.Equal(3, res.Values["raven"].Count);

                        Assert.Equal(4, res.Values["raven"][0].Entries.Length);
                        Assert.Equal(4, res.Values["raven"][1].Entries.Length);
                        Assert.Equal(0, res.Values["raven"][2].Entries.Length);
                    }

                    tsf = session.TimeSeriesFor(id, "raven");
                    for (int i = 8; i < 11; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 1000d }, "watches/apple");
                    }

                    session.SaveChanges();

                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var tsCommand = new GetMultipleTimeSeriesOperation.
                            GetMultipleTimeSeriesCommand(id, rangesList, 0, 10);
                        re.Execute(tsCommand, context);
                        var res = tsCommand.Result;

                        Assert.Equal(1, res.Values.Count);
                        Assert.Equal(3, res.Values["raven"].Count);

                        Assert.Equal(4, res.Values["raven"][0].Entries.Length);
                        Assert.Equal(4, res.Values["raven"][1].Entries.Length);
                        Assert.Equal(2, res.Values["raven"][2].Entries.Length);
                    }
                }
            }
        }

        [Fact]
        public void ResultsWithRangeAndPageSize()
        {
            using (var store = GetDocumentStore())
            {
                var tag = "raven";
                var id = "users/1";
                var baseline = RavenTestHelper.UtcToday;
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

                var rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(0), To = baseline.AddMinutes(3)
                    },
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(4), To = baseline.AddMinutes(7)
                    },
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(8), To = baseline.AddMinutes(11)
                    }
                };

                var re = store.GetRequestExecutor();

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, 0);
                    re.Execute(tsCommand, context);

                    var res = tsCommand.Result;

                    Assert.Empty(res.Values);

                    tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, 30);
                    re.Execute(tsCommand, context);

                    res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(3, res.Values["raven"].Count);

                    Assert.Equal(4, res.Values["raven"][0].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][1].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][2].Entries.Length);

                    tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, 6);
                    re.Execute(tsCommand, context);

                    res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(2, res.Values["raven"].Count);

                    Assert.Equal(4, res.Values["raven"][0].Entries.Length);
                    Assert.Equal(2, res.Values["raven"][1].Entries.Length);
                }
            }
        }

        [Fact]
        public void ResultsWithRangeAndStart()
        {
            using (var store = GetDocumentStore())
            {
                var tag = "raven";
                var id = "users/1";
                var baseline = RavenTestHelper.UtcToday;
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

                var rangesList = new List<TimeSeriesRange>
                {
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(0), To = baseline.AddMinutes(3)
                    },
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(4), To = baseline.AddMinutes(7)
                    },
                    new TimeSeriesRange
                    {
                        Name = "raven", From = baseline.AddMinutes(8), To = baseline.AddMinutes(11)
                    }
                };

                var re = store.GetRequestExecutor();

                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 0, 20);
                    re.Execute(tsCommand, context);

                    var res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(3, res.Values["raven"].Count);

                    Assert.Equal(4, res.Values["raven"][0].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][1].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][2].Entries.Length);

                    tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 3, 20);
                    re.Execute(tsCommand, context);

                    res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(3, res.Values["raven"].Count);

                    Assert.Equal(1, res.Values["raven"][0].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][1].Entries.Length);
                    Assert.Equal(4, res.Values["raven"][2].Entries.Length);

                    tsCommand = new GetMultipleTimeSeriesOperation.
                        GetMultipleTimeSeriesCommand(id, rangesList, 9, 20);
                    re.Execute(tsCommand, context);

                    res = tsCommand.Result;

                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(3, res.Values["raven"].Count);

                    Assert.Equal(0, res.Values["raven"][0].Entries.Length);
                    Assert.Equal(0, res.Values["raven"][1].Entries.Length);
                    Assert.Equal(3, res.Values["raven"][2].Entries.Length);
                }
            }
        }
    }
}
