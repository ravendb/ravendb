using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForDefineHub<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private PullReplicationDefinition _pullReplication;

        protected AbstractPullReplicationHandlerProcessorForDefineHub([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            responseJson[nameof(OngoingTask.TaskId)] = _pullReplication.TaskId == 0 ? index : _pullReplication.TaskId;
        }

        protected override ValueTask AssertCanExecuteAsync(string databaseName)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            return base.AssertCanExecuteAsync(databaseName);
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            _pullReplication = JsonDeserializationClient.PullReplicationDefinition(configuration);

            _pullReplication.Validate(RequestHandler.ServerStore.Server.Certificate?.Certificate != null);
            var updatePullReplication = new UpdatePullReplicationAsHubCommand(databaseName, raftRequestId)
            {
                Definition = _pullReplication
            };
            return RequestHandler.ServerStore.SendToLeaderAsync(updatePullReplication);
        }
    }
}
