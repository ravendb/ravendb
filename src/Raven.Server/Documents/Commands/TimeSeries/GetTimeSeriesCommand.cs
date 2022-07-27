using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.TimeSeries
{
    public class GetTimeSeriesCommand : RavenCommand
    {
        private readonly string _docId;
        private readonly string _name;
        private readonly DateTime? _from;
        private readonly DateTime? _to;
        private readonly TimeSpan? _offset;

        public GetTimeSeriesCommand(string docId, string name, DateTime? from, DateTime? to, TimeSpan? offset)
        {
            _docId = docId;
            _name = name;
            _from = @from;
            _to = to;
            _offset = offset;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/streams/timeseries?");

            sb.Append("docId=").Append(Uri.EscapeDataString(_docId)).Append('&');
            sb.Append("name=").Append(Uri.EscapeDataString(_name)).Append('&');

            if (_from.HasValue)
                sb.Append("from=").Append(_from.Value.EnsureUtc().GetDefaultRavenFormat()).Append('&');

            if (_to.HasValue)
                sb.Append("to=").Append(_to.Value.EnsureUtc().GetDefaultRavenFormat()).Append('&');

            if (_offset.HasValue)
                sb.Append("offset=").Append(_offset).Append('&');

            url = sb.ToString();
            return request;
        }
    }
}
