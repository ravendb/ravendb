using JetBrains.Annotations;
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

        protected override void AddOperation(long operationId, OperationCancelToken addOpToken)
        {
            //TODO stav: impl merge in EnforceConfigurationResult
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation(operationId, OperationType.EnforceRevisionConfiguration,
                $"Enforce revision configuration in database '{RequestHandler.DatabaseName}'.",
                null,
                onProgress => new EnforceRevisionsConfigurationOperation.EnforceRevisionsConfigurationCommand(operationId),
                addOpToken);

            _ = t.ContinueWith(_ =>
            {
                addOpToken.Dispose();
            });
        }
    }
}
