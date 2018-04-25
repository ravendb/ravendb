using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class IncrementCounterOperation : IOperation
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly long _value;

        public IncrementCounterOperation(string documentId, string name, long value = 0)
        {
            _documentId = documentId;
            _name = name;
            _value = value;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new IncrementCounterCommand(_documentId, _name, _value);
        }

        private class IncrementCounterCommand : RavenCommand
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly long _value;

            public IncrementCounterCommand(string documentId, string name, long value)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
                _value = value;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters/increment?id={_documentId}&name={_name}&val={_value}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}
