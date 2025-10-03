using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23091 : RavenTestBase
    {
        public RavenDB_23091(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.TimeSeries)]
        public async Task ShouldRemoveTimeSeriesAfterRetention()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromHours(24));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw
                        }
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
               
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    session.TimeSeriesFor("users/karmel", "Heartrate").Append(now.AddDays(-2), 69d, "watches/fitbit");

                    session.SaveChanges();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate").Append(now, 88d, "watches/fitbit");

                    session.Store(new User { Name = "Karmel2" }, "users/karmel2");
                    session.TimeSeriesFor("users/karmel2", "Heartrate2").Append(now.AddDays(-2), 77d, "watches/fitbit");

                    session.SaveChanges();
                }
               
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel2", "Heartrate2").Get();
                    Assert.Null(ts);
                }
            }
        }
    }
}
