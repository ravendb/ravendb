using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesBatchOperation : IOperation
    {
        private readonly string _documentId;
        private readonly TimeSeriesOperation _operation;

        public TimeSeriesBatchOperation(string documentId, TimeSeriesOperation operation)
        {
            _documentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
            _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new TimeSeriesBatchCommand(_documentId, _operation);
        }

        private class TimeSeriesBatchCommand : RavenCommand
        {
            private readonly string _documentId;
            private readonly TimeSeriesOperation _operation;

            public TimeSeriesBatchCommand(string documentId, TimeSeriesOperation operation)
            {
                _documentId = documentId;
                _operation = operation;

                if (_operation.Appends != null)
                {
                    var sorted = new SortedList<long, TimeSeriesOperation.AppendOperation>();

                    foreach (var append in _operation.Appends)
                    {
                        append.Timestamp = append.Timestamp.EnsureUtc().EnsureMilliseconds();
                        sorted[append.Timestamp.Ticks] = append; // on duplicate values the last one overrides
                    }

                    _operation.Appends = new List<TimeSeriesOperation.AppendOperation>(sorted.Values);
                }

                if (_operation.Removals != null)
                {
                    foreach (var removal in _operation.Removals)
                    {
                        removal.To = removal.To?.EnsureUtc();
                        removal.From = removal.From?.EnsureUtc();
                    }
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries?docId={_documentId}";

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
