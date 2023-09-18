using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System;
using System.Threading;
using FastTests;
using Raven.Client.Http;


namespace SlowTests.Issues
{
    public class RavenDB_19900 : RavenTestBase
    {
        public RavenDB_19900(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task UpdateTopologyTimeoutBehaviourShouldBeAccordingToConfiguration()
        {
            var generalTimeout = TimeSpan.FromSeconds(3);
            var customCommandTimeout = TimeSpan.FromSeconds(1);
            var delay = 2000;

            using (var store = GetDocumentStore())
            {
                var executor = store.GetRequestExecutor();

                try
                {
                    // delaying response should result in UpdateTopology timeout
                    executor.ForTestingPurposesOnly().DelayRequest = () => { Thread.Sleep(delay); };
                    executor.ForTestingPurposesOnly().SetCommandTimeout = (command) =>
                    {
                        command.Timeout = customCommandTimeout;
                    };
                    var e = await Assert.ThrowsAsync<TimeoutException>(async () => await UpdateTopology());
                    Assert.Contains("failed with timeout after 00:00:01", e.ToString());

                    // increasing the general timeout should let UpdateTopology() to be executed
                    using (store.SetRequestTimeout(generalTimeout))
                    {
                        await UpdateTopology();
                    }

                    async Task UpdateTopology()
                    {
                        var node = executor.GetPreferredNode().Result.Node;
                        var topologyUpdateCommand = new RequestExecutor.UpdateTopologyParameters(node);
                        await executor.UpdateTopologyAsync(topologyUpdateCommand);
                    }
                }
                finally
                {
                    executor.ForTestingPurposes = null;
                }
            }
        }
    }
}

