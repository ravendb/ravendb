using System;
using System.Net.Http;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class DeleteDocumentCommand : RavenCommand
    {
        private readonly string _id;
        private readonly string _changeVector;

        public DeleteDocumentCommand(string id, string changeVector)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _changeVector = changeVector;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            EnsureIsNotNullOrEmpty(_id, nameof(_id));

            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
            AddChangeVectorIfNotNull(_changeVector, request);
            return request;
        }
    }
}