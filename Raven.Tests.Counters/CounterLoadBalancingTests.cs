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
                using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA))
                using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
                using (var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC))
                {
                    using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreA))
                    using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreB))
                    using (var storeC = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreC))
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
}