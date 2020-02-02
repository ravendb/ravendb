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
        private readonly int? _skip;
        private readonly int? _take;

        public GetTimeSeriesOperation(string docId, string timeseries, DateTime from, DateTime to, int? skip = null, int? take = null)
            : this(docId, timeseries, skip, take)
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

        public GetTimeSeriesOperation(string docId, string timeseries, IEnumerable<TimeSeriesRange> ranges, int? skip = null, int? take = null) 
            : this(docId, timeseries, skip, take)
        {
            _ranges = ranges;
        }

        private GetTimeSeriesOperation(string docId, string timeseries, int? skip, int? take)
        {
            _docId = docId;
            _timeseries = timeseries;
            _skip = skip;
            _take = take;
        }

        public RavenCommand<TimeSeriesDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _timeseries, _ranges, _skip, _take);
        }

        private class GetTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly string _timeseries;
            private readonly IEnumerable<TimeSeriesRange> _ranges;
            private readonly int? _skip;
            private readonly int? _take;

            public GetTimeSeriesCommand(string docId, string timeseries, IEnumerable<TimeSeriesRange> ranges, int? skip, int? take)
            {
                _docId = docId ?? throw new ArgumentNullException(nameof(docId));
                _timeseries = timeseries ?? throw new ArgumentNullException(nameof(timeseries));
                _ranges = ranges;
                _skip = skip;
                _take = take;
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

                if (_skip.HasValue)
                {
                    pathBuilder.Append("&skip=")
                        .Append(_skip.Value);
                }

                if (_take.HasValue)
                {
                    pathBuilder.Append("&take=")
                        .Append(_take.Value);
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
    }
}
