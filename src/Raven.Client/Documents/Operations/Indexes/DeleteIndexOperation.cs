using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class DeleteIndexOperation : IAdminOperation
    {
        private readonly string _indexName;

        public DeleteIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteIndexCommand(_indexName);
        }

        private class DeleteIndexCommand : RavenCommand
        {
            private readonly string _indexName;

            public DeleteIndexCommand(string indexName)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}