using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Revisions
{
    public class GetRevisionsDebugCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly long? _start;
        private readonly int? _pageSize;

        public GetRevisionsDebugCommand(string nodeTag, long? start, int? pageSize)
        {
            SelectedNodeTag = nodeTag;
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/debug/documents/get-revisions");

            if (_start.HasValue && _pageSize.HasValue)
            {
                pathBuilder.Append("?start=").Append(_start.Value);
                pathBuilder.Append("&pageSize=").Append(_pageSize.Value);
            }
            else if (_pageSize.HasValue)
                pathBuilder.Append("?pageSize=").Append(_pageSize.Value);
            else if (_start.HasValue)
                pathBuilder.Append("?start=").Append(_start.Value);

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = response;
        }

        public override bool IsReadRequest => true;
    }
}
