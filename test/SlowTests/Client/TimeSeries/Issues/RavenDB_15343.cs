using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15343 : ReplicationTestBase
    {
        public RavenDB_15343(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexSegmentsWithADocument()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await new TimeSeriesIndex().ExecuteAsync(slave);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = master.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate").Append(baseline.AddDays(-30), new[] { 1d }, "watches/fitbit");
                    session.TimeSeriesFor("users/karmel", "Heartrate").Append(baseline, new[] { 1d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate2").Append(baseline.AddDays(-30), new[] { 1d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate3").Append(baseline, new[] { 2d }, "watches/fitbit");
                    session.SaveChanges();
                }

                var masterDb = await GetDocumentDatabaseInstanceFor(master);
                using (var controller = new ReplicationController(masterDb))
                {
                    await SetupReplicationAsync(master, slave);
                    controller.ReplicateOnce();

                    WaitForIndexing(slave);
                    RavenTestHelper.AssertNoIndexErrors(slave);
                }
            }
        }

        [Fact]
        public async Task CanIndexSegmentsWithoutTimeSeries()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var masterDb = await GetDocumentDatabaseInstanceFor(master);
                using (var controller = new ReplicationController(masterDb))
                {
                    
                    await SetupReplicationAsync(master, slave);
                    
                    var baseline = DateTime.UtcNow;

                    using (var session = master.OpenSession())
                    {
                        session.Store(new User { Name = "Karmel" }, "users/karmel");
                        session.SaveChanges();
                    }

                    controller.ReplicateOnce();


                    using (var session = master.OpenSession())
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate").Append(baseline.AddDays(-30), new[] { 1d }, "watches/fitbit");
                        session.TimeSeriesFor("users/karmel", "Heartrate").Append(baseline, new[] { 1d }, "watches/fitbit");
                        session.SaveChanges();
                    }

                    using (var session = master.OpenSession())
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate2").Append(baseline.AddDays(-30), new[] { 1d }, "watches/fitbit");
                        session.SaveChanges();
                    }

                    using (var session = master.OpenSession())
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate3").Append(baseline, new[] { 2d }, "watches/fitbit");
                        session.SaveChanges();
                    }

                    controller.ReplicateOnce();

                    await new TimeSeriesIndex().ExecuteAsync(slave);
                    WaitForIndexing(slave);
                    RavenTestHelper.AssertNoIndexErrors(slave);
                }
            }
        }

        private class TimeSeriesIndex : AbstractMultiMapTimeSeriesIndexCreationTask<TimeSeriesIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public double Value { get; set; }

                public DateTime Start { get; set; }

                public DateTime End { get; set; }
            }

            public TimeSeriesIndex()
            {
                AddMap<User>("Heartrate", segments => from ts in segments
                                                                  from entry in ts.Entries
                                                                  select new Result
                                                                  {
                                                                      Name = ts.Name,
                                                                      Value = entry.Value,
                                                                      Start = ts.Start,
                                                                      End = ts.End
                                                                  });
            }
        }
    }
}
