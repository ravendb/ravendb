using System;
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

            internal int BulkInsertStreamWriteTimeout;

            /// <summary>
            /// Called with the name of the orchestrator feature that handled a database record change and the raft index of the change.
            /// </summary>
            internal Action<string, long> OnDatabaseRecordChangeHandled;

            /// <summary>
            /// Called with the name of the orchestrator feature that handled a database value change and the raft index of the change.
            /// </summary>
            internal Action<string, long> OnValueChangeHandled;

        }
    }
}
