using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionsCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly string _id;
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly bool _deleteDocumentsWithRevision;

        public GetRevisionsCommand(string id, int? start, int? pageSize, bool deleteDocumentsWithRevision = false)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _start = start;
            _pageSize = pageSize;
            _deleteDocumentsWithRevision = deleteDocumentsWithRevision;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append('/')
                .Append(_deleteDocumentsWithRevision ? "delete-documents-with-revisions" : "revisions")
                .Append("?id=")
                .Append(Uri.EscapeDataString(_id))
                ;
            if (_start.HasValue)
                pathBuilder.Append("&start=").Append(_start);
            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}