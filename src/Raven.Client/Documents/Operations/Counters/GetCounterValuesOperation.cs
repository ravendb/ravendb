using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Counters
{
    public class GetCounterValuesOperation : IOperation<CountersDetail>
    {
        private readonly GetOrDeleteCounters _counters;


        public GetCounterValuesOperation(GetOrDeleteCounters counters)
        {
            _counters = counters;
        }

        public RavenCommand<CountersDetail> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCounterValuesCommand(_counters, conventions);
        }

        private class GetCounterValuesCommand : RavenCommand<CountersDetail>
        {
            private readonly GetOrDeleteCounters _counters;
            private readonly DocumentConventions _conventions;


            public GetCounterValuesCommand(GetOrDeleteCounters counters, DocumentConventions conventions)
            {
                _counters = counters ?? throw new ArgumentNullException(nameof(counters));
                _conventions = conventions;
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

                Result = JsonDeserializationClient.CountersDetail(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
