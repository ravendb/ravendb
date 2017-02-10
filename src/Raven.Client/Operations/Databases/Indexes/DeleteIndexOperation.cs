using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
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

        private class DeleteIndexCommand : RavenCommand<object>
        {
            private readonly string _indexName;

            public DeleteIndexCommand(string indexName)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeUriString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}