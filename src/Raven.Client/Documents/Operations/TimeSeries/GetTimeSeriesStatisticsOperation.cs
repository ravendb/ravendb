using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetTimeSeriesStatisticsOperation : IOperation<TimeSeriesStatistics>
    {
        private readonly string _documentId;
        /// <summary>
        /// Retrieve start, end and total number of entries for all time-series of a given document
        /// </summary>
        /// <param name="documentId"></param>
        public GetTimeSeriesStatisticsOperation(string documentId)
        {
            _documentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
        }

        
        public RavenCommand<TimeSeriesStatistics> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesStatisticsCommand(_documentId);
        }

        internal class GetTimeSeriesStatisticsCommand : RavenCommand<TimeSeriesStatistics>
        {
            private readonly string _documentId;

            public GetTimeSeriesStatisticsCommand(string documentId)
            {
                _documentId = documentId;
            }
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries/stats?docId={Uri.EscapeDataString(_documentId)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                Result = JsonDeserializationClient.GetTimeSeriesStatisticsResult(response);
            }
        }
    }
}
