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

        internal class TestingStuff
        {
            private readonly ShardedDatabaseContext _databaseContext;

            public TestingStuff(ShardedDatabaseContext databaseContext)
            {
                _databaseContext = databaseContext;
            }

            internal int ModifyShardNumber(int shardNumber) => ++shardNumber % _databaseContext.ShardCount;
        }
    }
}
