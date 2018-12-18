using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers
{
    public class PullReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tasks/hub-pull-replication", "PUT", AuthorizationStatus.Operator)]
        public async Task DefineHub()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplication();
            await DatabaseConfigurations((_, databaseName, blittableJson) =>
                {
                    var pullReplication = JsonDeserializationClient.PullReplicationDefinition(blittableJson);
                    pullReplication.Validate(ServerStore.Server.Certificate?.Certificate != null);
                    var updatePullReplication = new UpdatePullReplicationAsHubCommand(databaseName)
                    {
                        Definition = pullReplication
                    };
                    return ServerStore.SendToLeaderAsync(updatePullReplication);
                }, "update-hub-pull-replication",
                fillJson: (json, _, index) =>
                {
                    json[nameof(OngoingTask.TaskId)] = index;
                }, statusCode: HttpStatusCode.Created);
        }

        [RavenAction("/databases/*/admin/tasks/sink-pull-replication", "POST", AuthorizationStatus.Operator)]
        public async Task UpdatePullReplicationOnSinkNode()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                PullReplicationAsSink pullReplication = null;
                await DatabaseConfigurations((_, databaseName, blittableJson) => ServerStore.UpdatePullReplicationAsSink(databaseName, blittableJson, out pullReplication), "update-sink-pull-replication",
                    fillJson: (json, _, index) =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            var databaseRecord = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                            json[nameof(OngoingTask.ResponsibleNode)] = Database.WhoseTaskIsIt(databaseRecord.Topology, pullReplication, null);
                        }

                        json[nameof(ModifyOngoingTaskResult.TaskId)] = pullReplication.TaskId == 0 ? index : pullReplication.TaskId;
                    }, statusCode: HttpStatusCode.Created);
            }
        }
    }
}
