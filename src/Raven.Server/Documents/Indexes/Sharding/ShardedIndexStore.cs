using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding
{
    public class ShardedIndexStore : IndexStore
    {
        public ShardedIndexStore(DocumentDatabase database, ServerStore serverStore) : base(database)
        {
            ServerStore = serverStore;
            Create = new ShardedDatabaseIndexCreateController(database);
        }
    }
}
