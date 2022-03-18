using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14932 : RavenTestBase
    {
        public RavenDB_14932(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexRollups()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeValue.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeValue.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeValue.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeValue.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var now = DateTime.UtcNow;
                var nowSeconds = now.Second;
                now = now.AddSeconds(-nowSeconds);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddSeconds(-nowSeconds);

                var baseline = now.AddDays(-12);
                var total = ((TimeSpan)TimeValue.FromDays(12)).TotalMinutes;

                await new TimeSeriesIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] { 29d * i, i }, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await TimeSeries.WaitForPolicyRunnerAsync(database);
                await TimeSeries.VerifyPolicyExecutionAsync(store, config.Collections["Users"], 4, policies: new List<TimeSeriesPolicy> { p1 });
                await TimeSeries.VerifyPolicyExecutionAsync(store, config.Collections["Users"], 5, policies: new List<TimeSeriesPolicy> { p2 });
                await TimeSeries.VerifyPolicyExecutionAsync(store, config.Collections["Users"], 2, policies: new List<TimeSeriesPolicy> { p3 });
                await TimeSeries.VerifyPolicyExecutionAsync(store, config.Collections["Users"], 3, policies: new List<TimeSeriesPolicy> { p4 });

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");

                    var count = session
                        .TimeSeriesFor(user, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Count(entry => entry.IsRollup == false);

                    count += session
                        .TimeSeriesFor(user, "Heartrate@By6Hours")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Count();

                    count += session
                        .TimeSeriesFor(user, "Heartrate@By1Day")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Count();

                    count += session
                        .TimeSeriesFor(user, "Heartrate@By30Minutes")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Count();

                    count += session
                        .TimeSeriesFor(user, "Heartrate@By1Hour")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Count();

                    var results = session.Query<TimeSeriesIndex.Result, TimeSeriesIndex>()
                        .ToList();

                    Assert.True(count == results.Count, $"Test time = {now}");
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

                AddMap<User>("Heartrate@By6Hours", segments => from ts in segments
                                                               from entry in ts.Entries
                                                               select new Result
                                                               {
                                                                   Name = ts.Name,
                                                                   Value = entry.Value,
                                                                   Start = ts.Start,
                                                                   End = ts.End
                                                               });

                AddMap<User>("Heartrate@By1Day", segments => from ts in segments
                                                             from entry in ts.Entries
                                                             select new Result
                                                             {
                                                                 Name = ts.Name,
                                                                 Value = entry.Value,
                                                                 Start = ts.Start,
                                                                 End = ts.End
                                                             });

                AddMap<User>("Heartrate@By30Minutes", segments => from ts in segments
                                                                  from entry in ts.Entries
                                                                  select new Result
                                                                  {
                                                                      Name = ts.Name,
                                                                      Value = entry.Value,
                                                                      Start = ts.Start,
                                                                      End = ts.End
                                                                  });

                AddMap<User>("Heartrate@By1Hour", segments => from ts in segments
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
