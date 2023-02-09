using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions.Documents;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14386 : RavenTestBase
    {
        public RavenDB_14386(ITestOutputHelper output) : base(output)
        {

        }

        [Fact]
        public void TestTimeSeriesCopy()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1-A";
                var id2 = "users/2-A";
                var tag = "Heartrate";
                var tag2 = "Raven";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, tag);
                    for (int i = 0; i <= 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] {(double)i}, "watches/apple");
                    }

                    tsf = session.TimeSeriesFor(id, tag2);
                    for (int i = 0; i <= 50; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] {(double)i}, "watches/apple");
                    }

                    session.Store(new User(), id2);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user2 = session.Load<User>("users/2-A");
                    var user = session.Load<User>("users/1-A");

                    session.SaveChanges();

                    var ts = session.TimeSeriesFor(user2, tag);
                    var res = ts.Get();
                    Assert.Null(res);
                    ts = session.TimeSeriesFor(user2, tag2);
                    res = ts.Get();
                    Assert.Null(res);

                    foreach (var singleResult in session.Advanced.GetTimeSeriesFor(user))
                    {
                        session.Advanced.Defer(new CopyTimeSeriesCommandData(id,
                            singleResult,
                            id2,
                            singleResult));
                    }

                    session.SaveChanges();

                    ts = session.TimeSeriesFor(user2, tag);
                    res = ts.Get();
                    Assert.Equal(21, res.Length);
                    ts = session.TimeSeriesFor(user2, tag2);
                    res = ts.Get();
                    Assert.Equal(51, res.Length);

                }
            }
        }

        [Fact]
        public void TestTimeSeriesCopyFail()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1-A";
                var id2 = "users/2-A";
                var tag = "Heartrate";
                var tag2 = "Raven";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, tag);
                    for (int i = 0; i <= 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }

                    tsf = session.TimeSeriesFor(id, tag2);
                    for (int i = 0; i <= 50; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    foreach (var singleResult in session.Advanced.GetTimeSeriesFor(user))
                    {
                        session.Advanced.Defer(new CopyTimeSeriesCommandData(id,
                            singleResult,
                            id2,
                            singleResult));
                    }

                    Assert.Throws<DocumentDoesNotExistException>(() =>session.SaveChanges());
                }
            }
        }

        // RavenDB-17679
        [Fact]
        public async Task TimeSeriesCopyShouldNotThrowForRollupTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy();

                var p1 = new TimeSeriesPolicy("BySecond", TimeSpan.FromSeconds(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy> { p1 }
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var id = "users/1-A";
                var id2 = "users/2-A";
                var tag = "Heartrate";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    var tsf = session.TimeSeriesFor(id, "Heartrate");
                    for (int i = 0; i <= 20; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }

                    session.Store(new User(), id2);
                    session.SaveChanges();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var user2 = session.Load<User>(id2);
                    Assert.NotNull(user2);

                    foreach (var singleResult in session.Advanced.GetTimeSeriesFor(user))
                    {
                        session.Advanced.Defer(new CopyTimeSeriesCommandData(id,
                            singleResult,
                            id2,
                            singleResult));
                    }

                    session.SaveChanges();
                }
                
                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var ts = session.TimeSeriesFor(id2, tag);
                        var res = ts.Get();
                        if (res == null)
                            return false;

                        ts = session.TimeSeriesFor(id2, p1.GetTimeSeriesName(tag));
                        res = ts.Get();
                        return res != null;
                    }
                }, true));
            }
        }
    }
}
