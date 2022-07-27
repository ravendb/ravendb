using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Commands.TimeSeries;
using Sparrow;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class GetTimeSeriesConfigurationTests : RavenTestBase
    {
        public GetTimeSeriesConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetTimeSeriesConfiguration(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("ByHourFor12Hours", TimeValue.FromHours(1), TimeValue.FromHours(48)),
                                new TimeSeriesPolicy("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3)),
                                new TimeSeriesPolicy("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(12)),
                            }
                        }
                    }
                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                await store.Maintenance.ForTesting(() => new GetTimeSeriesConfigurationOperation()).AssertAllAsync((key, timeSeriesConfiguration) =>
                {
                    Assert.NotNull(timeSeriesConfiguration);
                    Assert.Equal(1, timeSeriesConfiguration.Collections.Count);
                    Assert.Equal(3, timeSeriesConfiguration.Collections["Users"].Policies.Count);
                });
            }
        }
    }
}
