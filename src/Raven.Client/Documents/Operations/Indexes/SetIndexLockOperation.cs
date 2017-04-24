using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class SetIndexLockOperation : IAdminOperation
    {
        private readonly string _indexName;
        private readonly IndexLockMode _mode;

        public SetIndexLockOperation(string indexName, IndexLockMode mode)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _mode = mode;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexLockCommand(_indexName, _mode);
        }

        private class SetIndexLockCommand : RavenCommand<object>
        {
            private readonly string _indexName;
            private readonly IndexLockMode _mode;

            public SetIndexLockCommand(string indexName, IndexLockMode mode)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _mode = mode;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-lock?name={Uri.EscapeDataString(_indexName)}&mode={_mode}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}