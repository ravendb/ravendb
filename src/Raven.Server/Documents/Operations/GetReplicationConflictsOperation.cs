using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    internal class GetConflictsByEtagOperation : IMaintenanceOperation<GetConflictsResultByEtag>
    {
        private readonly long _etag;

        public GetConflictsByEtagOperation(long etag = 0)
        {
            _etag = etag;
        }

        public RavenCommand<GetConflictsResultByEtag> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetConflictsByEtagCommand(_etag);
        }

        internal class GetConflictsByEtagCommand : RavenCommand<GetConflictsResultByEtag>
        {
            private readonly long _etag;

            public override bool IsReadRequest => true;

            public GetConflictsByEtagCommand(long etag = default)
            {
                _etag = etag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/conflicts";

                if (_etag != default)
                    url += $"?etag={_etag}";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationServer.GetConflictResults(response);
            }
        }
    }
}
