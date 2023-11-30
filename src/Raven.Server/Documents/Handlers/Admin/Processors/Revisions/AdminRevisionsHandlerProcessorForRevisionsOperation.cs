using System.Threading.Tasks;
using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AdminRevisionsHandlerProcessorForRevisionsOperation<TOperationParameters> : AbstractAdminRevisionsHandlerProcessorForRevisionsOperation<DatabaseRequestHandler, DocumentsOperationContext, TOperationParameters>
        where TOperationParameters : ReveisionsOperationParameters
    {

        public AdminRevisionsHandlerProcessorForRevisionsOperation([NotNull] DatabaseRequestHandler requestHandler, OperationType operationType)
            : base(requestHandler, operationType)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        protected abstract Task<IOperationResult> ExecuteOperation(Action<IOperationProgress> onProgress, TOperationParameters parameters, OperationCancelToken token);

        protected override void ScheduleEnforceConfigurationOperation(long operationId, TOperationParameters parameters, // todo: change to some AbstractAdminRevisionsHandlerProcessorForRevisionsOperation
            OperationCancelToken token)
        {
            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                _operationType,
                Description,
                detailedDescription: null,
                 onProgress => ExecuteOperation(onProgress, parameters, token),
                token: token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }
    }
}
