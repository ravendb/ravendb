using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    internal class GetReplicationConflictsOperation : IMaintenanceOperation<BlittableArrayResult>
    {
        private readonly string _documentId;
        private readonly long _etag;

        public GetReplicationConflictsOperation(string documentId = null, long etag = default)
        {
            _documentId = documentId;
            _etag = etag;
        }

        public RavenCommand<BlittableArrayResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetReplicationConflictsCommand(_documentId, _etag);
        }

        internal class GetReplicationConflictsCommand : RavenCommand<BlittableArrayResult>
        {
            private readonly string _documentId;
            private readonly long _etag;

            public override bool IsReadRequest => true;

            public GetReplicationConflictsCommand(string documentId = null, long etag = default)
            {
                _documentId = documentId;
                _etag = etag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/conflicts";

                if (_documentId != null)
                    url += $"?docId={Uri.EscapeDataString(_documentId)}";

                else if (_etag != default)
                    url += $"?etag={_etag}";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationServer.BlittableArrayResult(response);
            }
        }
    }
}
