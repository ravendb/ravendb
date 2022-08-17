using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.Logs;
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

                Assert.Equal(defaultConfiguration.TrafficWatchMode, TrafficWatchMode.Off);
                Assert.Equal(defaultConfiguration.Databases, new HashSet<string>());
                Assert.Equal(defaultConfiguration.StatusCodes, new HashSet<int>());
                Assert.Equal(defaultConfiguration.MinimumResponseSize, Size.Zero);
                Assert.Equal(defaultConfiguration.MinimumRequestSize, Size.Zero);
                Assert.Equal(defaultConfiguration.MinimumDuration, 0);
                Assert.Equal(defaultConfiguration.HttpMethods, new HashSet<string>());
                Assert.Equal(defaultConfiguration.ChangeTypes, new HashSet<TrafficWatchChangeType>());

                var configuration1 = new TrafficWatchConfigurationResult()
                {
                    TrafficWatchMode = TrafficWatchMode.Off,
                    Databases = new HashSet<string> { "test1", "test2" },
                    StatusCodes = new HashSet<int> { 200, 404, 500 },
                    MinimumResponseSize = new Size(11, SizeUnit.Bytes),
                    MinimumRequestSize = new Size(22, SizeUnit.Bytes),
                    MinimumDuration = 33,
                    HttpMethods = new HashSet<string> { "POST", "GET" },
                    ChangeTypes = new HashSet<TrafficWatchChangeType>
                    {
                        TrafficWatchChangeType.Queries, TrafficWatchChangeType.Counters, TrafficWatchChangeType.BulkDocs
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutTrafficWatchConfigurationOperation(configuration1));

                var configuration2 = await store.Maintenance.Server.SendAsync(new GetTrafficWatchConfigurationOperation());

                Assert.Equal(configuration1.TrafficWatchMode, configuration2.TrafficWatchMode);
                Assert.Equal(configuration1.Databases, configuration2.Databases);
                Assert.Equal(configuration1.StatusCodes, configuration2.StatusCodes);
                Assert.Equal(configuration1.MinimumResponseSize, configuration2.MinimumResponseSize);
                Assert.Equal(configuration1.MinimumRequestSize, configuration2.MinimumRequestSize);
                Assert.Equal(configuration1.MinimumDuration, configuration2.MinimumDuration);
                Assert.Equal(configuration1.HttpMethods, configuration2.HttpMethods);
                Assert.Equal(configuration1.ChangeTypes, configuration2.ChangeTypes);
            }
        }
    }
}

