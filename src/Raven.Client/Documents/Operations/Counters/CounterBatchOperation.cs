using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CounterBatchOperation : IOperation<CountersDetail>
    {
        private readonly CounterBatch _counterBatch;

        public CounterBatchOperation(CounterBatch counterBatch)
        {
            _counterBatch = counterBatch;
        }

        public RavenCommand<CountersDetail> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CounterBatchCommand(_counterBatch);
        }

        private class CounterBatchCommand : RavenCommand<CountersDetail>
        {
            private readonly CounterBatch _counterBatch;

            public CounterBatchCommand(CounterBatch counterBatch)
            {
                _counterBatch = counterBatch ?? throw new ArgumentNullException(nameof(counterBatch));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/counters";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,

                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_counterBatch, ctx)).ConfigureAwait(false))
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.CountersDetail(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
