using Raven.Server.Documents.Indexes.Sharding.Persistence;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding
{
    public class ShardedIndexStore : IndexStore
    {
        public ShardedIndexStore(ShardedDocumentDatabase database, ServerStore serverStore) : base(database, serverStore)
        {
            Create = new ShardedDatabaseIndexCreateController(database);
            IndexReadOperationFactory = new ShardedIndexReadOperationFactory();
        }
    }
}
