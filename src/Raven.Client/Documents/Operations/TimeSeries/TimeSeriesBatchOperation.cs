using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesBatchOperation : IOperation
    {
        private readonly DocumentTimeSeriesOperation _timeSeriesBatch;

        public TimeSeriesBatchOperation(DocumentTimeSeriesOperation timeSeriesBatch)
        {
            _timeSeriesBatch = timeSeriesBatch;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new TimeSeriesBatchCommand(_timeSeriesBatch);
        }

        private class TimeSeriesBatchCommand : RavenCommand
        {
            private readonly DocumentTimeSeriesOperation _timeSeriesBatch;

            public TimeSeriesBatchCommand(DocumentTimeSeriesOperation tsBatch)
            {
                _timeSeriesBatch = tsBatch ?? throw new ArgumentNullException(nameof(tsBatch));
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

            public override bool IsReadRequest => false;

        }

    }
}
