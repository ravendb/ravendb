using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Operation to retrieve the names of indexes from the database.
    /// </summary>
    public sealed class GetIndexNamesOperation : IMaintenanceOperation<string[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        /// <inheritdoc cref="GetIndexNamesOperation"/>
        /// <param name="start">The starting position of the index names list.</param>
        /// <param name="pageSize">The maximum number of index names to retrieve.</param>
        public GetIndexNamesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<string[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexNamesCommand(_start, _pageSize);
        }

        internal sealed class GetIndexNamesCommand : RavenCommand<string[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetIndexNamesCommand(int start, int pageSize)
                : this(start, pageSize, nodeTag: null)
            {
            }

            internal GetIndexNamesCommand(int start, int pageSize, string nodeTag)
            {
                _start = start;
                _pageSize = pageSize;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?start={_start}&pageSize={_pageSize}&namesOnly=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexNamesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}
