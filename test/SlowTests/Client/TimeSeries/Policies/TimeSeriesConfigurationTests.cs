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
                            RollupPolicies = new List<RollupPolicy>
                            {
                                new RollupPolicy(TimeSpan.FromHours(12), TimeSpan.FromHours(1)),
                                new RollupPolicy(TimeSpan.FromMinutes(180), TimeSpan.FromMinutes(1)),
                                new RollupPolicy(TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(1)),
                                new RollupPolicy(TimeSpan.FromDays(2), TimeSpan.FromDays(1)),
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                        
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];

                var policies = collection.RollupPolicies;
                Assert.Equal(4, policies.Count);

                Assert.Equal(TimeSpan.FromSeconds(60), policies[0].RetentionTime);
                Assert.Equal(TimeSpan.FromSeconds(1), policies[0].AggregateBy);

                Assert.Equal(TimeSpan.FromMinutes(180), policies[1].RetentionTime);
                Assert.Equal(TimeSpan.FromMinutes(1), policies[1].AggregateBy);

                Assert.Equal(TimeSpan.FromHours(12), policies[2].RetentionTime);
                Assert.Equal(TimeSpan.FromHours(1), policies[2].AggregateBy);

                Assert.Equal(TimeSpan.FromDays(2), policies[3].RetentionTime);
                Assert.Equal(TimeSpan.FromDays(1), policies[3].AggregateBy);
            }
        }
    }
}
