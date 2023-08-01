using Raven.Server.Documents.Indexes.Sharding.Persistence;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding
{
    public sealed class ShardedIndexStore : IndexStore
    {
        public ShardedIndexStore(ShardedDocumentDatabase database, ServerStore serverStore)
            : base(database, serverStore,
                new ShardedDatabaseIndexLockModeController(database),
                new ShardedDatabaseIndexPriorityController(database),
                new ShardedDatabaseIndexStateController(database),
                new ShardedDatabaseIndexCreateController(database),
                new ShardedDatabaseIndexDeleteController(database),
                new DatabaseIndexHasChangedController(database),
                new ShardedIndexReadOperationFactory())
        {
        }
    }
}
