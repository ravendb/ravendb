using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class GetCounterValueOperation : IOperation<long?>
    {
        private readonly string _documentId;
        private readonly string _name;

        public GetCounterValueOperation(string documentId, string name)
        {
            _documentId = documentId;
            _name = name;
        }

        public RavenCommand<long?> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCounterValueCommand(_documentId, _name);
        }

        private class GetCounterValueCommand : RavenCommand<long?>
        {
            private readonly string _documentId;
            private readonly string _name;

            public GetCounterValueCommand(string documentId, string name)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters?doc={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                response.TryGet("Value", out long v);
                Result = v;
            }

            public override bool IsReadRequest => true;
        }
    }
}
