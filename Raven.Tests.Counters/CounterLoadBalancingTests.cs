using System.Threading.Tasks;
using Raven.Abstractions.Replication;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
    public class CounterLoadBalancingTests : RavenBaseCountersTest
    {
        [Theory]
        [InlineData(6)]
        [InlineData(9)]
        [InlineData(30)]
        public async Task When_replicating_can_do_read_striping(int requestCount)
        {
            using (var serverA = GetNewServer(8077))
            using (var serverB = GetNewServer(8076))
            using (var serverC = GetNewServer(8075))
            {
                using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName, ravenServer: serverA))
                using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName, ravenServer: serverB))
                using (var storeC = NewRemoteCountersStore(DefaultCounterStorageName, ravenServer: serverC))
                {
                    storeA.CountersConvention.FailoverBehavior = FailoverBehavior.ReadFromAllServers;
                    await SetupReplicationAsync(storeA, storeB, storeC);

                    //make sure we get replication nodes info
                    await storeA.ReplicationInformer.UpdateReplicationInformationIfNeededAsync();

                    serverA.Server.ResetNumberOfRequests();
                    serverB.Server.ResetNumberOfRequests();
                    serverC.Server.ResetNumberOfRequests();
                    for (int i = 0; i < requestCount; i++)
                        await storeA.ChangeAsync("group", "counter", 2);

                    Assert.True(serverA.Server.NumberOfRequests > 0);
                    Assert.True(serverB.Server.NumberOfRequests > 0);
                    Assert.True(serverC.Server.NumberOfRequests > 0);
                }
            }
        }
    }
}
