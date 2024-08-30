using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_22634 : MixedClusterTestBase
    {
        public RavenDB_22634(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Sharding, RavenPlatform.Windows, Skip = "WIP")]
        public async Task MixedCluster_61_and_60_ShouldPreventCreatingDatabaseWithPrefixedSharding()
        {
            var (leader, peers, _) = await CreateMixedCluster([
                "6.0.105",
                "6.0.105"
            ]);

            using var leaderStore = new DocumentStore
            {
                Urls = [leader.WebUrl, peers[0].Url, peers[1].Url]
            }.Initialize();

            var dbRec = new DatabaseRecord("prefixed-db")
            {
                Sharding = new ShardingConfiguration
                {
                    Shards = new Dictionary<int, DatabaseTopology>
                    {
                        {0, new DatabaseTopology()},
                        {1, new DatabaseTopology()},
                        {2, new DatabaseTopology()}
                    },
                    Prefixed = [new PrefixedShardingSetting
                    {
                        Prefix = "orders/",
                        Shards = [2]
                    }]
                }
            };

            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                await leaderStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(dbRec)));

            Assert.Contains("Some nodes in the cluster are running older versions of RavenDB that do not support the Prefixed Sharding feature", ex.Message);
        }
    }
}
