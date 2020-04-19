using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
{
    public class TimeSeriesStatsUpdate : RavenTestBase
    {
        public TimeSeriesStatsUpdate(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RavenDB_14877()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"users/1-A");
                    session.SaveChanges();
                }

                var op = new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = DateTime.Now,
                    Tag = "as",
                    Values = new double[] {73}
                };
                var op2 = new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = DateTime.Now,
                    Tag = "as",
                    Values = new double[] { 78 }
                };
                var op3 = new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = new DateTime(2019, 4, 23) + TimeSpan.FromMinutes(5),
                    Tag = "as",
                    Values = new double[] { 798 }
                };

                var a = new TimeSeriesOperation 
                { 
                    Name = "test",
                    Appends = new List<TimeSeriesOperation.AppendOperation> {op3, op2,op}
                };

                store.Operations.Send(new TimeSeriesBatchOperation("users/1-A", a));

                var opDelete = new TimeSeriesOperation.RemoveOperation
                {
                    From = DateTime.Now - TimeSpan.FromDays(2),
                    To = DateTime.Now + TimeSpan.FromDays(2)
                };

                var ab = new TimeSeriesOperation
                {
                    Name = "test",
                    Removals = new List<TimeSeriesOperation.RemoveOperation> {opDelete }
                };

                store.Operations.Send(new TimeSeriesBatchOperation("users/1-A", ab));

                var abc = new TimeSeriesOperation
                {
                    Name = "test",
                    Removals = new List<TimeSeriesOperation.RemoveOperation>
                    {
                        new TimeSeriesOperation.RemoveOperation
                        {
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue

                        }
                    }
                };

                store.Operations.Send(new TimeSeriesBatchOperation("users/1-A", abc));
                var ts = store.Operations.Send(new GetTimeSeriesOperation("users/1-A", "test", DateTime.MinValue, DateTime.MaxValue));
                Assert.Equal(0, ts.Values["test"][0].Entries.Length);
            }
        }

        [Fact]
        public void UpdateStatsAfterEndDeletion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions {NoCaching = true}))
                {
                    var user = new User();
                    session.Store(user, "users/1-A");
                    var ts = session.TimeSeriesFor(user, "HR");

                    ts.Append(DateTime.Now, 73);
                    var oldTime = new DateTime(2019, 4, 23) + TimeSpan.FromMinutes(5);
                    ts.Append(oldTime, 1);

                    session.SaveChanges();

                    ts.Remove(DateTime.Now - TimeSpan.FromDays(2), DateTime.Now + TimeSpan.FromDays(2));
                    session.SaveChanges();

                    var after1delete = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, after1delete.Count);
                    Assert.Equal(oldTime, after1delete[0].Timestamp);

                    ts.Remove(DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var ts = session.TimeSeriesFor("users/1-A", "HR");
                    var after2delete = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(0, after2delete.Count);
                }
            }
        }

        [Fact]
        public void UpdateStatsAfterStartDeletion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var user = new User();
                    session.Store(user,"users/1-A");
                    var ts = session.TimeSeriesFor(user, "HR");

                    var now = DateTime.Now;
                    ts.Append(now, 73);
                    var oldTime = new DateTime(2019, 4, 23) + TimeSpan.FromMinutes(5);
                    ts.Append(oldTime, 1);
                    
                    session.SaveChanges();

                    ts.Remove(oldTime);
                    session.SaveChanges();

                    var after1delete = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, after1delete.Count);
                    Assert.Equal(now.EnsureMilliseconds(), after1delete[0].Timestamp);

                    ts.Remove(DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var ts = session.TimeSeriesFor("users/1-A", "HR");
                    var after2delete = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(0, after2delete.Count);
                }
            }
        }

        [Fact]
        public void UpdateStatsAfterEndReplacement()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var user = new User();
                    session.Store(user,"users/1-A");
                    var ts = session.TimeSeriesFor(user, "HR");

                    var now = DateTime.Now;
                    ts.Append(now, 73);
                    var oldTime = new DateTime(2019, 4, 23) + TimeSpan.FromMinutes(5);
                    ts.Append(oldTime, 1);
                    
                    session.SaveChanges();

                    ts.Remove(now);
                    now = DateTime.Now;
                    ts.Append(now, 76);
                    session.SaveChanges();

                    var values = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(2, values.Count);
                    Assert.Equal(now.EnsureMilliseconds(), values[1].Timestamp);
                    Assert.Equal(76, values[1].Value);
                }
            }
        }

        [Fact]
        public void UpdateStatsAfterStartReplacement()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var user = new User();
                    session.Store(user,"users/1-A");
                    var ts = session.TimeSeriesFor(user, "HR");

                    var now = DateTime.Now;
                    ts.Append(now, 73);
                    var oldTime = new DateTime(2019, 4, 23) + TimeSpan.FromMinutes(5);
                    ts.Append(oldTime, 1);
                    
                    session.SaveChanges();

                    var first = oldTime - TimeSpan.FromMinutes(1);
                    ts.Append(first, 76);
                    session.SaveChanges();

                    var values = ts.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, values.Count);
                    Assert.Equal(first.EnsureMilliseconds(), values[0].Timestamp);
                }
            }
        }
    }
}
