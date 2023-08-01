using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public sealed class GetServerWideBackupConfigurationsOperation : IServerOperation<ServerWideBackupConfiguration[]>
    {
        public RavenCommand<ServerWideBackupConfiguration[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideBackupConfigurationsCommand();
        }

        private class GetServerWideBackupConfigurationsCommand : RavenCommand<ServerWideBackupConfiguration[]>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/tasks?type={OngoingTaskType.Backup}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetServerWideBackupConfigurationsResponse(response).Results;
            }
        }
    }
}
