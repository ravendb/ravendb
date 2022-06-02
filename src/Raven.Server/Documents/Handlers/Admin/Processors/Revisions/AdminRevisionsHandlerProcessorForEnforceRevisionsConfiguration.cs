using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void AddOperation(long operationId, OperationCancelToken addOpToken)
        {
            var operationToken = RequestHandler.CreateTimeLimitedOperationToken();

            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.EnforceRevisionConfiguration,
                $"Enforce revision configuration in database '{RequestHandler.Database.Name}'.",
                detailedDescription: null,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.EnforceConfiguration(onProgress, operationToken),
                token: addOpToken);

            _ = t.ContinueWith(_ =>
            {
                addOpToken.Dispose();
            });
        }
    }
}
