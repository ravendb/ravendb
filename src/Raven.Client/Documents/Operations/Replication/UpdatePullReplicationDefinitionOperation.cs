using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public class UpdatePullReplicationDefinitionOperation : IMaintenanceOperation<ModifyOngoingTaskResult>
    {
        private readonly FeatureTaskDefinition _pullReplicationDefinition;

        public UpdatePullReplicationDefinitionOperation(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException($"'{nameof(name)}' must have value");
            }
            _pullReplicationDefinition = new PullReplicationDefinition(name);
        }

        public UpdatePullReplicationDefinitionOperation(PullReplicationDefinition pullReplicationDefinition)
        {
            if (string.IsNullOrEmpty(pullReplicationDefinition.Name))
            {
                throw new ArgumentException($"'{nameof(pullReplicationDefinition.Name)}' must have value");
            }
            _pullReplicationDefinition = pullReplicationDefinition;
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdatePullReplicationDefinitionCommand( _pullReplicationDefinition);
        }

        private class UpdatePullReplicationDefinitionCommand : RavenCommand<ModifyOngoingTaskResult>
        {
            private readonly FeatureTaskDefinition _pullReplicationDefinition;

            public UpdatePullReplicationDefinitionCommand(FeatureTaskDefinition pullReplicationDefinition)
            {
                _pullReplicationDefinition = pullReplicationDefinition;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                        {
                            ctx.Write(stream, ctx.ReadObject(_pullReplicationDefinition.ToJson(), "update-pull-replication-definition"));
                        })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
