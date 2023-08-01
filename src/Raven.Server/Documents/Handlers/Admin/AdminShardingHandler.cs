using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class AdminShardingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sharding/resharding/cleanup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ExecuteMoveDocuments()
        {
            ValidateShardDatabaseName();

            await ServerStore.EnsureNotPassiveAsync();

            var database = ShardedDocumentDatabase.CastToShardedDocumentDatabase(Database);
            await database.DocumentsMigrator.ExecuteMoveDocumentsAsync();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
        }

        private void ValidateShardDatabaseName()
        {
            if (ShardHelper.IsShardName(DatabaseName) == false)
                throw new NotSupportedException($"Executing documents migration is only valid for sharded databases. Instead got a non-sharded database {DatabaseName}");
        }
    }
}
