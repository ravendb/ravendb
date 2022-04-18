using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<
        DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler,
            requestHandler.ContextPool)
        {
        }

        protected override ValueTask<OperationIdResults> AddOperation(long operationId, OperationCancelToken token)
        {
            var t = RequestHandler.Database.Operations.AddOperation(
                RequestHandler.Database,
                $"Enforce revision configuration in database '{RequestHandler.Database.Name}'.",
                Operations.Operations.OperationType.EnforceRevisionConfiguration,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.EnforceConfiguration(onProgress, token),
                operationId,
                token: token);

            return ValueTask.FromResult(new OperationIdResults()
            {
                Results = new List<OperationIdResult>() { new ()
                {
                    OperationId = operationId, 
                    OperationNodeTag = RequestHandler.ServerStore.NodeTag
                }}
            });
        }

        protected override OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return RequestHandler.CreateTimeLimitedOperationToken();
        }
    }
}
