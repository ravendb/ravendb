using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Policies
{
    public class TimeSeriesConfigurationTests : RavenTestBase
    {
        public TimeSeriesConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task CanConfigureTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            DownSamplePolicies = new List<TimeSeriesDownSamplePolicy>
                            {
                                new TimeSeriesDownSamplePolicy(TimeSpan.FromHours(12), DownSampleFrequency.Hour),
                                new TimeSeriesDownSamplePolicy(TimeSpan.FromMinutes(180), DownSampleFrequency.Minute),
                                new TimeSeriesDownSamplePolicy(TimeSpan.FromSeconds(60), DownSampleFrequency.Second),
                                new TimeSeriesDownSamplePolicy(TimeSpan.FromDays(2), DownSampleFrequency.Day),
                            },
                            DeleteAfter = TimeSpan.FromDays(365)
                        }
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];

                Assert.Equal(collection.DeleteAfter, TimeSpan.FromDays(365));
                var policies = collection.DownSamplePolicies;
                Assert.Equal(4, policies.Count);

                Assert.Equal(TimeSpan.FromSeconds(60), policies[0].TimeFromNow);
                Assert.Equal(DownSampleFrequency.Second, policies[0].DownSampleFrequency);

                Assert.Equal(TimeSpan.FromMinutes(180), policies[1].TimeFromNow);
                Assert.Equal(DownSampleFrequency.Minute, policies[1].DownSampleFrequency);

                Assert.Equal(TimeSpan.FromHours(12), policies[2].TimeFromNow);
                Assert.Equal(DownSampleFrequency.Hour, policies[2].DownSampleFrequency);

                Assert.Equal(TimeSpan.FromDays(2), policies[3].TimeFromNow);
                Assert.Equal(DownSampleFrequency.Day, policies[3].DownSampleFrequency);


            }
        }
    }
}
