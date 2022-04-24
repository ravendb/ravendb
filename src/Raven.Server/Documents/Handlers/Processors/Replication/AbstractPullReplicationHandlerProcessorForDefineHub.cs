using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForDefineHub<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        private PullReplicationDefinition _pullReplication;

        protected AbstractPullReplicationHandlerProcessorForDefineHub([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            responseJson[nameof(OngoingTask.TaskId)] = _pullReplication.TaskId == 0 ? index : _pullReplication.TaskId;
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();
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
