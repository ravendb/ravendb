// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteRevisionsFor()
        {
            using (var processor = new AdminRevisionsHandlerProcessorForDeleteRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/revisions/conflicts/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task ConfigConflictedRevisions()
        {
            return DatabaseConfigurations(
                ServerStore.ModifyRevisionsForConflicts,
                "conflicted-revisions-config",
                GetRaftRequestIdFromQuery());
        }

        [RavenAction("/databases/*/admin/revisions/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostRevisionsConfiguration()
        {
            using (var processor = new RevisionsHandlerProcessorForPostRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/revisions/config/enforce", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task EnforceConfigRevisions()
        {
            var token = CreateTimeLimitedOperationToken();
            var operationId = ServerStore.Operations.GetNextOperationId();

            var t = Database.Operations.AddOperation(
                Database,
                $"Enforce revision configuration in database '{Database.Name}'.",
                Operations.Operations.OperationType.EnforceRevisionConfiguration,
                onProgress => Database.DocumentsStorage.RevisionsStorage.EnforceConfiguration(onProgress, token),
                operationId,
                token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }
    }
}
