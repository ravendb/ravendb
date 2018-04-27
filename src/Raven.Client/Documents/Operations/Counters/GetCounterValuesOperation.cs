using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class GetCounterValuesOperation : IOperation<Dictionary<string, long>>
    {
        private readonly string _documentId;
        private readonly string _name;

        public GetCounterValuesOperation(string documentId, string name)
        {
            _documentId = documentId;
            _name = name;
        }

        public RavenCommand<Dictionary<string, long>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCounterValuesCommand(_documentId, _name);
        }

        private class GetCounterValuesCommand : RavenCommand<Dictionary<string, long>>
        {
            private readonly string _documentId;
            private readonly string _name;

            public GetCounterValuesCommand(string documentId, string name)
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
                url = $"{node.Url}/databases/{node.Database}/counters?doc={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}&mode=all";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                response.TryGet("Values", out BlittableJsonReaderArray values);

                Result = new Dictionary<string, long>();

                foreach (BlittableJsonReaderObject v in values)
                {
                    v.TryGet("DbId", out string dbid);
                    v.TryGet("Value", out long value);
                    Result[dbid] = value;
                }
            }

            public override bool IsReadRequest => true;
        }
    }
}
