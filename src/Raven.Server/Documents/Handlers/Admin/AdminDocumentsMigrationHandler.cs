using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminDocumentsMigrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/documentsMigrator/cleanup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ExecuteMoveDocuments()
        {
            ValidateShardDatabaseName();

            await ServerStore.EnsureNotPassiveAsync();

            var database = Database as ShardedDocumentDatabase;
            await database.PeriodicDocumentsMigrator.ExecuteMoveDocumentsAsync();

            var operationId = ServerStore.Operations.GetNextOperationId();
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }

        private void ValidateShardDatabaseName()
        {
            if (ShardHelper.IsShardName(DatabaseName) == false)
                throw new NotSupportedException($"Executing documents migration is only valid for sharded databases. Instead got a non-sharded database {DatabaseName}");
        }
    }
}
