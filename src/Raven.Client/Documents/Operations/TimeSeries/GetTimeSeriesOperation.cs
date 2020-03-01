using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetTimeSeriesOperation : IOperation<TimeSeriesDetails>
    {
        private readonly string _docId;
        private readonly string _timeseries;
        private readonly IEnumerable<TimeSeriesRange> _ranges;
        private readonly int _start;
        private readonly int _pageSize;

        public GetTimeSeriesOperation(string docId, string timeseries, DateTime from, DateTime to, int start = 0, int pageSize = int.MaxValue)
            : this(docId, timeseries, start, pageSize)
        {
            _ranges = new List<TimeSeriesRange>
            {
                new TimeSeriesRange
                {
                    From = from,
                    To = to
                }
            };
        }

        public GetTimeSeriesOperation(string docId, string timeseries, IEnumerable<TimeSeriesRange> ranges, int start = 0, int pageSize = int.MaxValue) 
            : this(docId, timeseries, start, pageSize)
        {
            _ranges = ranges;
        }

        private GetTimeSeriesOperation(string docId, string timeseries, int start, int pageSize)
        {
            _docId = docId;
            _timeseries = timeseries;
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<TimeSeriesDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _timeseries, _ranges, _start, _pageSize);
        }

        private class GetTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly string _timeseries;
            private readonly IEnumerable<TimeSeriesRange> _ranges;
            private readonly int _start;
            private readonly int _pageSize;

            public GetTimeSeriesCommand(string docId, string timeseries, IEnumerable<TimeSeriesRange> ranges, int start, int pageSize)
            {
                _docId = docId ?? throw new ArgumentNullException(nameof(docId));
                _timeseries = timeseries ?? throw new ArgumentNullException(nameof(timeseries));
                _ranges = ranges;
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries");

                pathBuilder.Append("?id=")
                    .Append(Uri.EscapeDataString(_docId))
                    .Append("&name=")
                    .Append(Uri.EscapeDataString(_timeseries));

                if (_start > 0)
                {
                    pathBuilder.Append("&start=")
                        .Append(_start);
                }

                if (_pageSize < int.MaxValue)
                {
                    pathBuilder.Append("&pageSize=")
                        .Append(_pageSize);
                }

                if (_ranges != null)
                {
                    foreach (var range in _ranges)
                    {
                        pathBuilder.Append("&from=")
                            .Append(range.From.GetDefaultRavenFormat())
                            .Append("&to=")
                            .Append(range.To.GetDefaultRavenFormat());
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
        public Dictionary<string, List<TimeSeriesRangeResult>> Values = new Dictionary<string, List<TimeSeriesRangeResult>>();
        public int TotalResults;
    }
}
