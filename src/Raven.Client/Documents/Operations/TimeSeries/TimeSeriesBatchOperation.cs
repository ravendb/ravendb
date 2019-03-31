using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesBatchOperation : IOperation<TimeSeriesBatchOperation.Result>
    {
        public class Result
        {
            
        }
        private readonly TimeSeriesBatch _timeSeriesBatch;

        public TimeSeriesBatchOperation(TimeSeriesBatch timeSeriesBatch)
        {
            _timeSeriesBatch = timeSeriesBatch;
        }

        public RavenCommand<Result> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CounterBatchCommand(_timeSeriesBatch, conventions);
        }

        public class CounterBatchCommand : RavenCommand<Result>
        {
            private readonly DocumentConventions _conventions;
            private readonly TimeSeriesBatch _timeSeriesBatch;

            public CounterBatchCommand(TimeSeriesBatch counterBatch, DocumentConventions conventions)
            {
                _timeSeriesBatch = counterBatch ?? throw new ArgumentNullException(nameof(counterBatch));
                _conventions = conventions;
            }


            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,

                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_timeSeriesBatch, ctx);
                        ctx.Write(stream, config);
                    })
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = new Result();
            }

            public override bool IsReadRequest => false;

        }


    }
}
