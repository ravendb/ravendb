using System;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14426 : ReplicationTestBase
    {
        public RavenDB_14426(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeleteAndReInsertTimeSeriesEntry()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende");

                    tsf.Append("Heartrate", baseline.AddMinutes(1), "fitbit", new[] { 58d });
                    tsf.Append("Heartrate", baseline.AddMinutes(5), "fitbit", new[] { 68d });
                    tsf.Append("Heartrate", baseline.AddMinutes(10), "fitbit", new[] { 78d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", baseline.AddMinutes(2), baseline.AddMinutes(8));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(2, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");

                    tsf.Append("Heartrate", baseline.AddMinutes(5), "fitbit", new[] { 99d });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    
                    Assert.Equal(3, all.Count);
                    Assert.Equal(all[1].Values, new []{ 99d });
                }

            }
        }


    }
}
