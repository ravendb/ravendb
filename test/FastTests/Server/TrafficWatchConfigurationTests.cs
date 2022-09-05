using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.TrafficWatch;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server
{
    public class TrafficWatchConfigurationTests : RavenTestBase
    {
        public TrafficWatchConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CheckDefaultsAndCanSetAndGetTrafficWatchConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var defaultConfiguration = await store.Maintenance.Server.SendAsync(new GetTrafficWatchConfigurationOperation());

                Assert.Equal(defaultConfiguration.TrafficWatchMode, TrafficWatchMode.None);
                Assert.Equal(defaultConfiguration.Databases, new List<string>());
                Assert.Equal(defaultConfiguration.StatusCodes, new List<int>());
                Assert.Equal(defaultConfiguration.MinimumResponseSizeInBytes, Size.Zero);
                Assert.Equal(defaultConfiguration.MinimumRequestSizeInBytes, Size.Zero);
                Assert.Equal(defaultConfiguration.MinimumDurationInMs, 0);
                Assert.Equal(defaultConfiguration.HttpMethods, new List<string>());
                Assert.Equal(defaultConfiguration.ChangeTypes, new List<TrafficWatchChangeType>());

                var configuration1 = new PutTrafficWatchConfigurationOperation.Parameters()
                {
                    TrafficWatchMode = TrafficWatchMode.Off,
                    Databases = new List<string> { "test1", "test2" },
                    StatusCodes = new List<int> { 200, 404, 500 },
                    MinimumResponseSizeInBytes = new Size(11, SizeUnit.Bytes),
                    MinimumRequestSizeInBytes = new Size(22, SizeUnit.Bytes),
                    MinimumDurationInMs = 33,
                    HttpMethods = new List<string> { "POST", "GET" },
                    ChangeTypes = new List<TrafficWatchChangeType>
                    {
                        TrafficWatchChangeType.Queries, TrafficWatchChangeType.Counters, TrafficWatchChangeType.BulkDocs
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutTrafficWatchConfigurationOperation(configuration1));

                var configuration2 = await store.Maintenance.Server.SendAsync(new GetTrafficWatchConfigurationOperation());

                Assert.Equal(configuration1.TrafficWatchMode, configuration2.TrafficWatchMode);
                Assert.Equal(configuration1.Databases, configuration2.Databases);
                Assert.Equal(configuration1.StatusCodes, configuration2.StatusCodes);
                Assert.Equal(configuration1.MinimumResponseSizeInBytes, configuration2.MinimumResponseSizeInBytes);
                Assert.Equal(configuration1.MinimumRequestSizeInBytes, configuration2.MinimumRequestSizeInBytes);
                Assert.Equal(configuration1.MinimumDurationInMs, configuration2.MinimumDurationInMs);
                Assert.Equal(configuration1.HttpMethods, configuration2.HttpMethods);
                Assert.Equal(configuration1.ChangeTypes, configuration2.ChangeTypes);
            }
        }
    }
}
