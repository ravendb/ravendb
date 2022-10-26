using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    public class RemoveNodeFromOrchestratorTopologyOperation : IServerOperation<ModifyOrchestratorTopologyResult>
    {
        private readonly string _databaseName;
        private readonly string _node;
        private readonly bool _force;

        public RemoveNodeFromOrchestratorTopologyOperation(string databaseName, string node, bool force = false)
        {
            _node = node;
            _databaseName = databaseName;
            _force = force;
        }

        public RavenCommand<ModifyOrchestratorTopologyResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RemoveNodeFromOrchestratorTopologyCommand(_databaseName, _node, _force);
        }

        private class RemoveNodeFromOrchestratorTopologyCommand : RavenCommand<ModifyOrchestratorTopologyResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly string _node;
            private readonly bool _force;

            public RemoveNodeFromOrchestratorTopologyCommand(string databaseName, string node, bool force = false)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                if (string.IsNullOrEmpty(node))
                    throw new ArgumentNullException(node);

                _databaseName = databaseName;
                _node = node;
                _force = force;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/orchestrator?name={Uri.EscapeDataString(_databaseName)}&node={Uri.EscapeDataString(_node)}&force={_force}";
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOrchestratorTopologyResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
