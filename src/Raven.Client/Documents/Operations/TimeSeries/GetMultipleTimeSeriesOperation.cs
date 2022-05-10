using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetMultipleTimeSeriesOperation : IOperation<TimeSeriesDetails>
    {
        private readonly string _docId;
        private readonly IEnumerable<TimeSeriesRange> _ranges;
        private readonly int _start;
        private readonly int _pageSize;
        private readonly Action<ITimeSeriesIncludeBuilder> _includes;
        private readonly bool _returnFullResults;

        public GetMultipleTimeSeriesOperation(string docId, IEnumerable<TimeSeriesRange> ranges, int start = 0, int pageSize = int.MaxValue, bool returnFullResults = false)
            : this(docId, ranges, start, pageSize, includes: null, returnFullResults)
        {
        }

        internal GetMultipleTimeSeriesOperation(string docId, IEnumerable<TimeSeriesRange> ranges, int start, int pageSize, Action<ITimeSeriesIncludeBuilder> includes, bool returnFullResults = false)
        {
            _ranges = ranges ?? throw new ArgumentNullException(nameof(ranges));
            
            if (string.IsNullOrEmpty(docId))
                throw new ArgumentNullException(nameof(docId));
            
            _docId = docId;
            _start = start;
            _pageSize = pageSize;
            _includes = includes;
            _returnFullResults = returnFullResults;
        }

        public RavenCommand<TimeSeriesDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetMultipleTimeSeriesCommand(_docId, _ranges, _start, _pageSize, _includes);
        }

        internal class GetMultipleTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly IEnumerable<TimeSeriesRange> _ranges;
            private readonly int _start;
            private readonly int _pageSize;
            private readonly Action<ITimeSeriesIncludeBuilder> _includes;
            private readonly bool _returnFullResults;

            public GetMultipleTimeSeriesCommand(string docId, IEnumerable<TimeSeriesRange> ranges, int start, int pageSize, Action<ITimeSeriesIncludeBuilder> includes = null, bool returnFullResults = false)
            {
                _docId = docId ?? throw new ArgumentNullException(nameof(docId));
                _ranges = ranges;
                _start = start;
                _pageSize = pageSize;
                _includes = includes;
                _returnFullResults = returnFullResults;
            }
            
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries/ranges")
                    .Append("?docId=")
                    .Append(Uri.EscapeDataString(_docId));

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

                bool any = false;
                foreach (var range in _ranges)
                {
                    if (string.IsNullOrEmpty(range.Name))
                        throw new InvalidOperationException($"Missing '{nameof(TimeSeriesRange.Name)}' argument in '{nameof(TimeSeriesRange)}'. " +
                                                            $"'{nameof(TimeSeriesRange.Name)}' cannot be null or empty");
                    any = true;
                    pathBuilder.Append("&name=")
                        .Append(Uri.EscapeDataString(range.Name))
                        .Append("&from=")
                        .Append(range.From?.EnsureUtc().GetDefaultRavenFormat())
                        .Append("&to=")
                        .Append(range.To?.EnsureUtc().GetDefaultRavenFormat());
                }

                if (_returnFullResults)
                {
                    pathBuilder.Append("&full=").Append(true);
                }

                if (any == false)
                    throw new InvalidOperationException("Argument 'ranges' cannot be null or empty");

                if (_includes != null)
                {
                    GetTimeSeriesOperation.GetTimeSeriesCommand.AddIncludesToRequest(pathBuilder, _includes);
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
}
