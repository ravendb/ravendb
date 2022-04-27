using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        private PullReplicationAsSink _pullReplication;

        protected AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            var databaseName = GetDatabaseName();

            if (_pullReplication == null)
            {
                if (configuration.TryGet(nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink), out BlittableJsonReaderObject pullReplicationBlittable) == false)
                {
                    throw new InvalidDataException($"{nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink)} was not found.");
                }

                _pullReplication = JsonDeserializationClient.PullReplicationAsSink(pullReplicationBlittable);
            }

            using (context.OpenReadTransaction())
            {
                var topology = RequestHandler.ServerStore.Cluster.ReadDatabaseTopology(context, databaseName);
                responseJson[nameof(OngoingTask.ResponsibleNode)] = RequestHandler.ServerStore.WhoseTaskIsIt(topology, _pullReplication, null);
            }

            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _pullReplication.TaskId == 0 ? index : _pullReplication.TaskId;
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            if (ResourceNameValidator.IsValidResourceName(GetDatabaseName(), RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsSink();
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.UpdatePullReplicationAsSink(databaseName, configuration, raftRequestId, out _pullReplication);
        }
    }
}
