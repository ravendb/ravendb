using System;
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

                    session.TimeSeriesFor("users/1").Append("speed", baseline, tag: null, new []{ double.NaN });

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

                    session.TimeSeriesFor("users/1").Append("speed", baseline, tag: null, new[] { 105d, double.NaN });

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

                    var tsf = session.TimeSeriesFor("users/1");
                    tsf.Append("speed", baseline, tag: null, new[] { 105d });
                    tsf.Append("speed", baseline.AddMinutes(1), tag: null, new[] { 97d });
                    tsf.Append("speed", baseline.AddMinutes(2), tag: null, new[] { double.NaN });
                    tsf.Append("speed", baseline.AddMinutes(3), tag: null, new[] { 112d });

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

                    var tsf = session.TimeSeriesFor("users/1");
                    tsf.Append("speed", baseline, tag: null, new[] { 105d });
                    tsf.Append("speed", baseline.AddMinutes(1), tag: null, new[] { 97d });
                    tsf.Append("speed", baseline.AddMinutes(2), tag: null, new[] { 102d, double.NaN });
                    tsf.Append("speed", baseline.AddMinutes(3), tag: null, new[] { 112d, 113 });

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
                    var tsf = session.TimeSeriesFor("users/1");
                    tsf.Append("speed", baseline, tag: null, new[] { 105d });
                    tsf.Append("speed", baseline.AddMinutes(1), tag: null, new[] { 97d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1");

                    tsf.Append("speed", baseline.AddMinutes(2), tag: null, new[] { 102d, 104 });
                    tsf.Append("speed", baseline.AddMinutes(3), tag: null, new[] { 112d, double.NaN });

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
                    var tsf = session.TimeSeriesFor("users/1");
                    tsf.Append("speed", baseline, tag: null, new[] { 90d , 91 });
                    tsf.Append("speed", baseline.AddHours(1), tag: null, new[] { 104d, 104});

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/1");
                    tsf.Append("speed", baseline.AddMinutes(10), tag: null, new[] { 98d, 97 });
                    tsf.Append("speed", baseline.AddMinutes(20), tag: null, new[] { 102d, 100 });
                    tsf.Append("speed", baseline.AddMinutes(30), tag: null, new[] { 88d, double.NaN });
                    tsf.Append("speed", baseline.AddMinutes(40), tag: null, new[] { 92d, 93 });

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains("TimeSeries entries cannot have 'double.NaN' as one of their values", ex.Message);
                }
            }
        }

    }
}
