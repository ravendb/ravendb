using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Smuggler.Documents
{
    public sealed class ShardedDatabaseSource : DatabaseSource
    {
        private readonly ShardedDocumentDatabase _database;

        public ShardedDatabaseSource(ShardedDocumentDatabase database, long startDocumentEtag, long startRaftIndex, RavenLogger logger) 
            : base(database, startDocumentEtag, startRaftIndex, logger)
        {
            _database = database;
        }

        protected override DatabaseRecord ReadDatabaseRecord()
        {
            using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _database.ServerStore.Cluster.ReadDatabase(context, _database.ShardedDatabaseName);
            }
        }
    }
}
