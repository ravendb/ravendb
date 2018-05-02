using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
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
            return new GetCounterValueCommand(_documentId, _name, conventions);
        }

        private class GetCounterValueCommand : RavenCommand<long?>
        {
            private readonly GetOrDeleteCounters _counters;
            private readonly DocumentConventions _conventions;

            public GetCounterValueCommand(string documentId, string name, DocumentConventions conventions)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _conventions = conventions;
                _counters = new GetOrDeleteCounters
                {
                    Counters = new List<CountersOperation>
                    {
                        new CountersOperation
                        {
                            DocumentId = documentId,
                            Counters = new[] {name}
                        }
                    }
                };
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_counters, _conventions, ctx);
                        ctx.Write(stream, config);
                    })
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                var details = JsonDeserializationClient.CountersDetail(response).Counters;
                if (details.Count == 0)
                    return;

                Result = details[0].TotalValue;

            }

            public override bool IsReadRequest => true;
        }
    }
}
