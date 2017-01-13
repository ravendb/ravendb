using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class SetIndexLockOperation : IAdminOperation
    {
        private readonly string _indexName;
        private readonly IndexLockMode _mode;

        public SetIndexLockOperation(string indexName, IndexLockMode mode)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _mode = mode;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions)
        {
            return new SetIndexLockCommand(_indexName, _mode);
        }

        private class SetIndexLockCommand : RavenCommand<object>
        {
            private readonly string _indexName;
            private readonly IndexLockMode _mode;

            public SetIndexLockCommand(string indexName, IndexLockMode mode)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
                _mode = mode;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-lock?name={Uri.EscapeUriString(_indexName)}&mode={_mode}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}