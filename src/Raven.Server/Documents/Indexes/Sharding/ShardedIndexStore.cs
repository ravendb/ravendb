using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding
{
    public class ShardedIndexStore : IndexStore
    {
        public ShardedIndexStore(ShardedDocumentDatabase shardedDocumentDatabase, ServerStore serverStore) : base(shardedDocumentDatabase)
        {
            ServerStore = serverStore;

            LockMode = shardedDocumentDatabase.DatabaseContext.Indexes.LockMode;
            Priority = shardedDocumentDatabase.DatabaseContext.Indexes.Priority;
            State = shardedDocumentDatabase.DatabaseContext.Indexes.State;
            Delete = shardedDocumentDatabase.DatabaseContext.Indexes.Delete;
            Create = shardedDocumentDatabase.DatabaseContext.Indexes.Create;
            HasChanged = shardedDocumentDatabase.DatabaseContext.Indexes.HasChanged;
        }
    }
}
