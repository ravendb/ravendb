using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers
{
    public class PullReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tasks/pull-replication", "PUT", AuthorizationStatus.Operator)]
        public async Task DefinePullReplication()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplication();
            await DatabaseConfigurations((_, databaseName, blittableJson) =>
                {
                    var pullReplication = JsonDeserializationClient.PullReplicationDefinition(blittableJson);
                    pullReplication.Validate(ServerStore.Server.Certificate?.Certificate != null);
                    var updatePullReplication = new UpdatePullReplicationCommand(databaseName)
                    {
                        Definition = pullReplication
                    };
                    return ServerStore.SendToLeaderAsync(updatePullReplication);
                }, "update_pull_replication",
                fillJson: (json, _, index) =>
                {
                    json[nameof(OngoingTask.TaskId)] = index;
                }, statusCode: HttpStatusCode.Created);
        }
    }
}
