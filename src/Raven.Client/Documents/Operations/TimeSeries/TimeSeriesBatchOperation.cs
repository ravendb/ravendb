using System;
using System.Collections.Generic;
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
        private readonly TimeSeriesBatch _timeSeriesBatch;

        internal TimeSeriesBatchOperation(TimeSeriesOperation operation)
            : this(new TimeSeriesBatch
            {
                Documents = new List<TimeSeriesOperation>
                {
                    operation
                }
            })
        {
            if (operation == null)
                throw new ArgumentNullException(nameof(operation));
        }

        public TimeSeriesBatchOperation(TimeSeriesBatch batch)
        {
            if (batch == null)
                throw new ArgumentNullException(nameof(batch));

            _timeSeriesBatch = batch;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new TimeSeriesBatchCommand(_timeSeriesBatch);
        }

        private class TimeSeriesBatchCommand : RavenCommand
        {
            private readonly TimeSeriesBatch _timeSeriesBatch;

            public TimeSeriesBatchCommand(TimeSeriesBatch batch)
            {
                _timeSeriesBatch = batch ?? throw new ArgumentNullException(nameof(batch));
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
