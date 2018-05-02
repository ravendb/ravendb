using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CounterBatchOperation : IOperation
    {
        private readonly CounterBatch _counterBatch;

        public CounterBatchOperation(CounterBatch counterBatch)
        {
            _counterBatch = counterBatch;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CounterBatchCommand(_counterBatch, conventions);
        }

        public class CounterBatchCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;

            private readonly CounterBatch _counterBatch;

            public CounterBatchCommand(CounterBatch counterBatch, DocumentConventions conventions)
            {
                _counterBatch = counterBatch ?? throw new ArgumentNullException(nameof(counterBatch));
                _conventions = conventions;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters/batch";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,

                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_counterBatch, _conventions, ctx);
                        ctx.Write(stream, config);
                    })
                };
            }
        }
    }
}
