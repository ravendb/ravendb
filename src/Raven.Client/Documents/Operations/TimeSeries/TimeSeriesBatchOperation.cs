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
        private readonly TimeSeriesOperation _operation;

        public TimeSeriesBatchOperation(TimeSeriesOperation operation)
        {
            _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }


        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new TimeSeriesBatchCommand(_operation);
        }

        private class TimeSeriesBatchCommand : RavenCommand
        {
            private readonly TimeSeriesOperation _operation;

            public TimeSeriesBatchCommand(TimeSeriesOperation operation)
            {
                _operation = operation;

                if (_operation.Appends != null)
                {
                    var sorted = new SortedList<long, TimeSeriesOperation.AppendOperation>();

                    foreach (var append in _operation.Appends)
                    {
                        sorted.Add(append.Timestamp.Ticks, append);
                    }

                    _operation.Appends = new List<TimeSeriesOperation.AppendOperation>(sorted.Values);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,

                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_operation, ctx);
                        ctx.Write(stream, config);
                    })
                };
            }

            public override bool IsReadRequest => false;

        }

    }
}
