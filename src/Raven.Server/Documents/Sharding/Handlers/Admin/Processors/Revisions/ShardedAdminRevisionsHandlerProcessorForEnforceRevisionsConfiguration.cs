using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        protected override void ScheduleEnforceConfigurationOperation(long operationId, OperationCancelToken token)
        {
            var task = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult, EnforceConfigurationResult>(
                operationId,
                OperationType.EnforceRevisionConfiguration,
                $"Enforce revision configuration in database '{RequestHandler.DatabaseName}'.",
                detailedDescription: null,
                (_, shardNumber) => new EnforceRevisionsConfigurationOperation.EnforceRevisionsConfigurationCommand(),
                token: token);

            _ = task.ContinueWith(_ =>
            {
                task.Dispose();
            });
        }
    }
}
