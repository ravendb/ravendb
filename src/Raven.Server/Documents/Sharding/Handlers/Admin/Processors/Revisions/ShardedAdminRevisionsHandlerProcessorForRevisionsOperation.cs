using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal abstract class ShardedAdminRevisionsHandlerProcessorForRevisionsOperation<TOperationParameters> : AbstractAdminRevisionsHandlerProcessorForRevisionsOperation<ShardedDatabaseRequestHandler, TransactionOperationContext, TOperationParameters>
        where TOperationParameters : ReveisionsOperationParameters
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

        protected override void ScheduleEnforceConfigurationOperation(long operationId, TOperationParameters parameters,  // todo: change to some AbstractAdminRevisionsHandlerProcessorForRevisionsOperation
            OperationCancelToken token)
        {
            var task = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult, EnforceConfigurationResult, EnforceConfigurationResult>(
                operationId,
                _operationType,//OperationType.EnforceRevisionConfiguration,
                Description,//$"Enforce revision configuration in database '{RequestHandler.Database.Name}'.",
                detailedDescription: null,
                (context , shardNumber) => GetCommand(context, shardNumber, parameters), //(_, shardNumber) => new EnforceRevisionsConfigurationOperation.EnforceRevisionsConfigurationCommand(parameters, DocumentConventions.DefaultForServer),
                token: token);

            _ = task.ContinueWith(_ =>
            {
                task.Dispose();
            });
        }
    }
}
