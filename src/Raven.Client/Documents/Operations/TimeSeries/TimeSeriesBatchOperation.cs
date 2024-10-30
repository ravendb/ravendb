using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public sealed class TimeSeriesBatchOperation : IOperation
    {
        private readonly string _documentId;
        private readonly TimeSeriesOperation _operation;

        /// <summary>
        /// Initializes a new instance of the <see cref="TimeSeriesBatchOperation"/> class, 
        /// performing batch operations on a time series, including appending or deleting entries.
        /// </summary>
        /// <param name="documentId">The ID of the document that holds the time series.</param>
        /// <param name="operation">The batch of time series operations to be applied.</param>
        public TimeSeriesBatchOperation(string documentId, TimeSeriesOperation operation)
        {
            _documentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
            _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new TimeSeriesBatchCommand(conventions, _documentId, _operation);
        }

        internal sealed class TimeSeriesBatchCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _documentId;
            private readonly TimeSeriesOperation _operation;

            public TimeSeriesBatchCommand(DocumentConventions conventions, string documentId, TimeSeriesOperation operation)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _documentId = documentId;
                _operation = operation;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries?docId={Uri.EscapeDataString(_documentId)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,

                    Content = new BlittableJsonContent(async stream =>
                    {
                        var op = ctx.ReadObject(_operation.ToJson(), "convert-time-series-operation");
                        await ctx.WriteAsync(stream, op).ConfigureAwait(false);
                    }, _conventions)
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}
