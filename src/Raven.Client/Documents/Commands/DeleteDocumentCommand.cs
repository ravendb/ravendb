using System;
using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class DeleteDocumentCommand : RavenCommand
    {
        private readonly string _id;
        private readonly long? _etag;

        public DeleteDocumentCommand(string id, long? etag)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _etag = etag;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            EnsureIsNotNullOrEmpty(_id, nameof(_id));

            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
            AddEtagIfNotNull(_etag, request);
            return request;
        }
    }
}