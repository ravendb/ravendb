using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal sealed class AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        protected override void ScheduleEnforceConfigurationOperation(long operationId, EnforceRevisionsConfigurationOperation.Parameters parameters, OperationCancelToken token)
        {
            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.EnforceRevisionConfiguration,
                $"Enforce revision configuration in database '{RequestHandler.Database.Name}'.",
                detailedDescription: null,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(onProgress, parameters.IncludeForceCreated, parameters.Collections, token),
                token: token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }
    }
}
