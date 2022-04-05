using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    internal class GetRawTimeSeriesCommand : ShardedStreamCommand
    {
        private ShardedDatabaseRequestHandler _handler;
        private readonly string _docId, _name;
        private readonly int _start, _pageSize;
        private readonly DateTime? _from, _to;
        private readonly bool _includeDoc;
        private readonly bool _includeTags;
        private readonly bool _returnFullResults;

        public GetRawTimeSeriesCommand(ShardedDatabaseRequestHandler handler, string docId, string name, DateTime? @from, DateTime? to, int start, int pageSize,
            bool includeDoc, bool includeTags, bool returnFullResults = false) : base(handler, null)
        {
            _handler = handler;
            _docId = docId;
            _name = name;
            _start = start;
            _pageSize = pageSize;
            _from = from;
            _to = to;
            _includeDoc = includeDoc;
            _includeTags = includeTags;
            _returnFullResults = returnFullResults;
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
                .Append(Uri.EscapeDataString(_name));

            if (_from.HasValue)
            {
                pathBuilder.Append("&from=")
                    .Append(_from.Value.EnsureUtc().GetDefaultRavenFormat());
            }

            if (_to.HasValue)
            {
                pathBuilder.Append("&to=")
                    .Append(_to.Value.EnsureUtc().GetDefaultRavenFormat());
            }

            if (_includeDoc)
            {
                pathBuilder.Append("&includeDocument=").Append(true);
            }

            if (_includeTags)
            {
                pathBuilder.Append("&includeTags=").Append(true);
            }

            if (_returnFullResults)
            {
                pathBuilder.Append("&full=").Append(true);
            }

            url = pathBuilder.ToString();

            var request = new HttpRequestMessage { Method = HttpMethod.Get };

            return request;
        }

        public override async Task HandleStreamResponse(Stream responseStream)
        {
            await responseStream.CopyToAsync(_handler.ResponseBodyStream());
        }

        public override bool IsReadRequest => true;
    }
}
