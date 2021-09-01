using System.Collections.Generic;
using FastTests.Sharding;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class ShardedClusterTests : ShardedClusterTestBase
    {
        public ShardedClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateTopology_Nodes_3_ReplicationFactor_1_Shards_3()
        {
            var databaseTopologies = GetDatabaseTopologyForShards(1, new List<string> { "A", "B", "C" }, 3);
            Assert.Equal(1, databaseTopologies[0].Count);
            Assert.True(databaseTopologies[0].RelevantFor("A"));
            Assert.Equal(1, databaseTopologies[1].Count);
            Assert.True(databaseTopologies[1].RelevantFor("B"));
            Assert.Equal(1, databaseTopologies[2].Count);
            Assert.True(databaseTopologies[2].RelevantFor("C"));
        }

        [Fact]
        public void CanCreateTopology_Nodes_3_ReplicationFactor_2_Shards_3()
        {
            var databaseTopologies = GetDatabaseTopologyForShards(2, new List<string> { "A", "B", "C" }, 3);
            Assert.Equal(2, databaseTopologies[0].Count);
            Assert.True(databaseTopologies[0].RelevantFor("A"));
            Assert.True(databaseTopologies[0].RelevantFor("C") || databaseTopologies[0].RelevantFor("B"));
            Assert.Equal(2, databaseTopologies[1].Count);
            Assert.True(databaseTopologies[1].RelevantFor("B"));
            Assert.True(databaseTopologies[1].RelevantFor("C") || databaseTopologies[0].RelevantFor("A"));
            Assert.Equal(2, databaseTopologies[2].Count);
            Assert.True(databaseTopologies[2].RelevantFor("C"));
            Assert.True(databaseTopologies[2].RelevantFor("B") || databaseTopologies[0].RelevantFor("A"));
        }

        [Fact]
        public void CanCreateTopology_Nodes_3_ReplicationFactor_3_Shards_3()
        {
            var databaseTopologies = GetDatabaseTopologyForShards(3, new List<string> { "A", "B", "C" }, 3);
            Assert.Equal(3, databaseTopologies[0].Count);
            Assert.True(databaseTopologies[0].RelevantFor("A"));
            Assert.True(databaseTopologies[0].RelevantFor("B"));
            Assert.True(databaseTopologies[0].RelevantFor("C"));
            Assert.Equal(3, databaseTopologies[1].Count);
            Assert.True(databaseTopologies[1].RelevantFor("B"));
            Assert.True(databaseTopologies[1].RelevantFor("A"));
            Assert.True(databaseTopologies[1].RelevantFor("C"));
            Assert.Equal(3, databaseTopologies[2].Count);
            Assert.True(databaseTopologies[2].RelevantFor("C"));
            Assert.True(databaseTopologies[2].RelevantFor("B"));
            Assert.True(databaseTopologies[2].RelevantFor("A"));
        }

        [Fact]
        public void CanCreateTopology_Nodes_2_ReplicationFactor_2_Shards_3()
        {
            var databaseTopologies = GetDatabaseTopologyForShards(2, new List<string> { "A", "B" }, 3);
            Assert.Equal(2, databaseTopologies[0].Count);
            Assert.True(databaseTopologies[0].RelevantFor("A"));
            Assert.True(databaseTopologies[0].RelevantFor("B"));
            Assert.Equal(2, databaseTopologies[1].Count);
            Assert.True(databaseTopologies[1].RelevantFor("B"));
            Assert.True(databaseTopologies[1].RelevantFor("A"));
            Assert.Equal(2, databaseTopologies[2].Count);
            Assert.True(databaseTopologies[2].RelevantFor("A"));
            Assert.True(databaseTopologies[2].RelevantFor("B"));
        }
    }
}
