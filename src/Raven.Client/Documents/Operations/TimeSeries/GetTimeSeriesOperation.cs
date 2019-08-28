using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetTimeSeriesOperation : IOperation<TimeSeriesDetails>
    {
        private readonly string _docId;
        private readonly string _timeseries;
        private readonly List<(DateTime From, DateTime To)> _ranges;

        public GetTimeSeriesOperation(string docId, string timeseries, DateTime from, DateTime to)
        {
            _docId = docId;
            _timeseries = timeseries;
            _ranges = new List<(DateTime, DateTime)>
            {
                (from, to)
            };
        }

        public GetTimeSeriesOperation(string docId, string timeseries, List<(DateTime From, DateTime To)> ranges)
        {
            _docId = docId;
            _timeseries = timeseries;
            _ranges = ranges;
        }

        public RavenCommand<TimeSeriesDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _timeseries, _ranges);
        }

        private class GetTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly string _timeseries;
            private readonly List<(DateTime From, DateTime To)> _ranges;

            public GetTimeSeriesCommand(string docId, string timeseries, List<(DateTime From, DateTime To)> ranges)
            {
                _docId = docId ?? throw new ArgumentNullException(nameof(docId));
                _timeseries = timeseries ?? throw new ArgumentNullException(nameof(timeseries));
                _ranges = ranges;
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
                    .Append(Uri.EscapeDataString(_timeseries));

                if (_ranges != null)
                {
                    foreach (var range in _ranges)
                    {
                        var f = range.From.Kind == DateTimeKind.Local
                            ? DateTime.SpecifyKind(range.From, DateTimeKind.Unspecified)
                            : range.From;

                        var t = range.To.Kind == DateTimeKind.Local
                            ? DateTime.SpecifyKind(range.To, DateTimeKind.Unspecified)
                            : range.To;

                        pathBuilder.Append("&from=")
                            .Append(f.ToString("o"))
                            .Append("&to=")
                            .Append(t.ToString("o"));
                    }
                }

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
        public Dictionary<string, List<TimeSeriesRange>> Values = new Dictionary<string, List<TimeSeriesRange>>();
    }
}
