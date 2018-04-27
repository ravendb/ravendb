using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class GetCountersForDocumentOperation : IOperation<IEnumerable<string>>
    {
        private readonly string _documentId;

        public GetCountersForDocumentOperation(string documentId)
        {
            _documentId = documentId;
        }

        public RavenCommand<IEnumerable<string>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCountersForDocumentCommand(_documentId);
        }

        private class GetCountersForDocumentCommand : RavenCommand<IEnumerable<string>>
        {
            private readonly string _documentId;

            public GetCountersForDocumentCommand(string documentId)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));

                _documentId = documentId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters?doc={Uri.EscapeDataString(_documentId)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (!(response["Names"] is BlittableJsonReaderArray bjra))
                    return;

                var names = new List<string>(bjra.Length);
                foreach (var name in bjra)
                {
                    names.Add(name.ToString());   
                }

                Result = names;
            }

            public override bool IsReadRequest => true;
        }
    }
}
