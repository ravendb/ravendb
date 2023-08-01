using System.Linq;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff(this);
        }

        internal sealed class TestingStuff
        {
            private readonly ShardedDatabaseContext _databaseContext;

            public TestingStuff(ShardedDatabaseContext databaseContext)
            {
                _databaseContext = databaseContext;
            }

            internal int ModifyShardNumber(int shardNumber) =>
                _databaseContext.ShardsTopology.Keys.ElementAt(0) == shardNumber ? _databaseContext.ShardsTopology.Keys.ElementAt(1) : shardNumber;
        }
    }
}
