using System.IO;
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
    internal abstract class AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private PullReplicationAsSink _pullReplication;

        protected AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, PullReplicationAsSink pullReplication);

        protected override void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            if (_pullReplication == null)
            {
                if (configuration.TryGet(nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink), out BlittableJsonReaderObject pullReplicationBlittable) == false)
                {
                    throw new InvalidDataException($"{nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink)} was not found.");
                }

                _pullReplication = JsonDeserializationClient.PullReplicationAsSink(pullReplicationBlittable);
            }

            FillResponsibleNode(context, responseJson, _pullReplication);

            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _pullReplication.TaskId == 0 ? index : _pullReplication.TaskId;
        }

        protected override ValueTask AssertCanExecuteAsync(string databaseName)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsSink();

            return base.AssertCanExecuteAsync(databaseName);
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.UpdatePullReplicationAsSink(databaseName, configuration, raftRequestId, out _pullReplication);
        }
    }
}
