using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetTimeSeriesOperation : IOperation<TimeSeriesDetails>
    {
        private readonly string _docId;
        private readonly string _timeseries;
        private readonly DateTime _from, _to;

        public GetTimeSeriesOperation(string docId, string timeseries, DateTime from, DateTime to)
        {
            _docId = docId;
            _timeseries = timeseries;
            _from = from;
            _to = to;
        }

        public RavenCommand<TimeSeriesDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _timeseries, _from, _to);
        }

        private class GetTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly string _timeseries;
            private readonly DateTime _from, _to;

            public GetTimeSeriesCommand(string docId, string timeseries, DateTime from, DateTime to)
            {
                _docId = docId;
                _timeseries = timeseries;
                _from = from;
                _to = to;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries?")
                    .Append("id=")
                    .Append(Uri.EscapeDataString(_docId))
                    .Append("&name=")
                    .Append(Uri.EscapeDataString(_timeseries))
                    .Append("&from=")
                    .Append(_from.ToString("o"))
                    .Append("&to=")
                    .Append(_to.ToString("o"));
                ;

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                url = pathBuilder.ToString();

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.TimeSeriesDetails(response);
            }

            public override bool IsReadRequest => true;
        }
    }

    public class TimeSeriesDetails
    {
        public string Id;
        public Dictionary<string, TimeSeriesRange> Values = new Dictionary<string, TimeSeriesRange>();
    }
}
