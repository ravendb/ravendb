using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal abstract class ShardedAdminRevisionsHandlerProcessorForRevisionsOperation<TOperationParameters, TOperationResult> : AbstractAdminRevisionsHandlerProcessorForRevisionsOperation<ShardedDatabaseRequestHandler, TransactionOperationContext, TOperationParameters>
        where TOperationParameters : IRevisionsOperationParameters
        where TOperationResult : OperationResult, new()
    {

        public ShardedAdminRevisionsHandlerProcessorForRevisionsOperation([NotNull] ShardedDatabaseRequestHandler requestHandler, OperationType operationType)
            : base(requestHandler, operationType)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        protected abstract RavenCommand<OperationIdResult> GetCommand(JsonOperationContext context, int shardNumber, TOperationParameters parameters);

        protected override void ScheduleEnforceConfigurationOperation(long operationId, TOperationParameters parameters,
            OperationCancelToken token)
        {
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult, TOperationResult, TOperationResult>(
                operationId,
                _operationType,
                Description,
                detailedDescription: null,
                (context , shardNumber) => GetCommand(context, shardNumber, parameters),
                token: token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }
    }
}
