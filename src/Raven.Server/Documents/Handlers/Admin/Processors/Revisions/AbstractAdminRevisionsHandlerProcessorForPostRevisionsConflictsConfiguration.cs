using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.ModifyRevisionsForConflicts(context, RequestHandler.DatabaseName, configuration, raftRequestId);
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(RevisionsHandler.ConflictedRevisionsConfigTag, Index, configuration);
            return ValueTask.CompletedTask;
        }
    }
}
