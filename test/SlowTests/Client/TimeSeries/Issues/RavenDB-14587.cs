using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14587 : ReplicationTestBase
    {
        public RavenDB_14587(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PreventAppendingNaN_1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var baseline = new DateTime(1980, 1, 1);

                    session.TimeSeriesFor("users/1", "speed").Append(baseline, new []{ double.NaN });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public void PreventAppendingNaN_2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var baseline = new DateTime(1980, 1, 1);

                    session.TimeSeriesFor("users/1", "speed").Append(baseline, new[] { 105d, double.NaN });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public void PreventAppendingNaN_3()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var baseline = new DateTime(1980, 1, 1);

                    var tsf = session.TimeSeriesFor("users/1", "speed");
                    tsf.Append(baseline, new[] { 105d });
                    tsf.Append(baseline.AddMinutes(1), new[] { 97d });
                    tsf.Append(baseline.AddMinutes(2), new[] { double.NaN });
                    tsf.Append(baseline.AddMinutes(3), new[] { 112d });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public void PreventAppendingNaN_4()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var baseline = new DateTime(1980, 1, 1);

                    var tsf = session.TimeSeriesFor("users/1", "speed");
                    tsf.Append(baseline, new[] { 105d });
                    tsf.Append(baseline.AddMinutes(1), new[] { 97d });
                    tsf.Append(baseline.AddMinutes(2), new[] { 102d, double.NaN });
                    tsf.Append(baseline.AddMinutes(3), new[] { 112d, 113 });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public void PreventAppendingNaN_5()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                var baseline = new DateTime(1980, 1, 1);

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1", "speed");
                    tsf.Append(baseline, new[] { 105d });
                    tsf.Append(baseline.AddMinutes(1), new[] { 97d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1", "speed");

                    tsf.Append(baseline.AddMinutes(2), new[] { 102d, 104 });
                    tsf.Append(baseline.AddMinutes(3), new[] { 112d, double.NaN });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public void PreventAppendingNaN_6()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                var baseline = new DateTime(1980, 1, 1);

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1", "speed");
                    tsf.Append(baseline, new[] { 90d , 91 });
                    tsf.Append(baseline.AddHours(1), new[] { 104d, 104});

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1", "speed");
                    tsf.Append(baseline.AddMinutes(10), new[] { 98d, 97 });
                    tsf.Append(baseline.AddMinutes(20), new[] { 102d, 100 });
                    tsf.Append(baseline.AddMinutes(30), new[] { 88d, double.NaN });
                    tsf.Append(baseline.AddMinutes(40), new[] { 92d, 93 });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

        [Fact]
        public async Task CanAppendDifferentNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                var name = "heartrate";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "karmel"}, id);

                    var tsf = session.TimeSeriesFor(id, name);
                    var baseline = RavenTestHelper.UtcToday;
                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Append(baseline.AddDays(i), new[] {1d, 2d, 3d});
                    }
                    for (int i = 100; i < 200; i++)
                    {
                        tsf.Append(baseline.AddDays(i), new[] {1d, 2d});
                    }
                    await session.SaveChangesAsync();
                }
            }
        }
    }
}
