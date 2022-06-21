using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForRevertRevisions : AbstractRevisionsHandlerProcessorForRevertRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForRevertRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleRevertRevisions(long operationId, RevertRevisionsRequest configuration, OperationCancelToken token)
        {
            var task = RequestHandler.DatabaseContext.Operations
                .AddRemoteOperation<OperationIdResult, RevertResult, RevertResult>(
                    operationId,
                    OperationType.DatabaseRevert,
                    $"Revert database '{RequestHandler.DatabaseName}' to {configuration.Time} UTC.",
                    detailedDescription: null,
                    (c, shardNumber) => new RevertRevisionsOperation.RevertRevisionsCommand(configuration, operationId),
                    token);

            _ = task.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }
        
        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }
    }
}
