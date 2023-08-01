using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Utils.Metrics.Commands
{
    internal sealed class GetDatabaseMetricsCommand : RavenCommand
    {
        private readonly bool _putsOnly;
        private readonly bool _bytesOnly;
        private readonly bool? _filterEmpty;

        public GetDatabaseMetricsCommand(bool putsOnly = false, bool bytesOnly = false)
        {
            _putsOnly = putsOnly;
            _bytesOnly = bytesOnly;
        }

        public GetDatabaseMetricsCommand(bool putsOnly = false, bool bytesOnly = false, bool filterEmpty = true) : this (putsOnly, bytesOnly)
        {
            _filterEmpty = filterEmpty;
        }

        public GetDatabaseMetricsCommand(string nodeTag, bool putsOnly = false, bool bytesOnly = false, bool filterEmpty = true) : this(putsOnly, bytesOnly, filterEmpty)
        {
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder($"{node.Url}/databases/{node.Database}/metrics");

            if (_putsOnly)
                pathBuilder.Append("/puts");
            else if (_bytesOnly)
                pathBuilder.Append("/bytes");

            if (_filterEmpty.HasValue)
                pathBuilder.Append($"?empty={_filterEmpty.Value}");

            url = pathBuilder.ToString();

            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
    }
}
