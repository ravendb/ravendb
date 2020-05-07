using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class GetTimeSeriesOperation : IOperation<TimeSeriesRangeResult>
    {
        private readonly string _docId, _name;
        private readonly int _start, _pageSize;
        private readonly DateTime? _from, _to;

        public GetTimeSeriesOperation(string docId, string timeseries, DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue)
        {
            if (string.IsNullOrEmpty(docId))
                throw new ArgumentNullException(nameof(docId));

            if (string.IsNullOrEmpty(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            _docId = docId;
            _start = start;
            _pageSize = pageSize;
            _name = timeseries;
            _from = from;
            _to = to;
        }

        public RavenCommand<TimeSeriesRangeResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _name, _from, _to, _start, _pageSize);
        }

        internal class GetTimeSeriesCommand : RavenCommand<TimeSeriesRangeResult>
        {
            private readonly string _docId, _name;
            private readonly int _start, _pageSize;
            private readonly DateTime? _from, _to;

            public GetTimeSeriesCommand(string docId, string name, DateTime? from, DateTime? to, int start, int pageSize)
            {
                _docId = docId;
                _name = name;
                _start = start;
                _pageSize = pageSize;
                _from = from;
                _to = to;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries")
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

                pathBuilder.Append("&name=")
                    .Append(Uri.EscapeDataString(_name))
                    .Append("&from=")
                    .Append(_from?.EnsureUtc().GetDefaultRavenFormat())
                    .Append("&to=")
                    .Append(_to?.EnsureUtc().GetDefaultRavenFormat());


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

                Result = JsonDeserializationClient.TimeSeriesRangeResult(response);

                if (response.TryGet(nameof(TimeSeriesRangeResult.Entries), out BlittableJsonReaderArray entries) && 
                    entries == null)
                {
                    Result.Entries = null;
                }
            }

            public override bool IsReadRequest => true;
        }
    }
}
