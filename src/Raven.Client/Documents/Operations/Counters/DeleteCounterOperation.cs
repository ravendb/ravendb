using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class DeleteCounterOperation : IOperation
    {
        private readonly string _documentId;
        private readonly string _name;

        public DeleteCounterOperation(string documentId, string name)
        {
            _documentId = documentId;
            _name = name;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteCounterCommand(_documentId, _name, conventions);
        }

        private class DeleteCounterCommand : RavenCommand
        {
            private readonly GetOrDeleteCounters _counters;
            private readonly DocumentConventions _conventions;

            public DeleteCounterCommand(string documentId, string name, DocumentConventions conventions)
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
                url = $"{node.Url}/databases/{node.Database}/counters/delete";

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
        }
    }
}
