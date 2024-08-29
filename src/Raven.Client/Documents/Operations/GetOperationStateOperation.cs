using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// Retrieves the state of a specific operation by its ID, providing information such as the current status and progress.
    /// </summary>
    public sealed class GetOperationStateOperation : IMaintenanceOperation<OperationState>
    {
        private readonly long _id;
        private readonly string _nodeTag;

        /// <inheritdoc cref="GetOperationStateOperation"/>
        /// <param name="id">The ID of the requested operation.</param>
        public GetOperationStateOperation(long id)
        {
            _id = id;
        }

        /// <inheritdoc cref="GetOperationStateOperation(long)"/>
        /// <param name="nodeTag">The node tag specifying which node should be queried for the operation's state.</param>
        public GetOperationStateOperation(long id, string nodeTag)
        {
            _id = id;
            _nodeTag = nodeTag;
        }

        public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetOperationStateCommand(_id, _nodeTag);
        }

        internal sealed class GetOperationStateCommand : RavenCommand<OperationState>
        {
            public override bool IsReadRequest => true;

            private readonly long _id;

            public GetOperationStateCommand(long id, string nodeTag = null)
            {
                _id = id;
                SelectedNodeTag = nodeTag;
                Timeout = TimeSpan.FromSeconds(15);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/operations/state?id={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;
                
                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationState>(response);
            }
        }
    }
}
