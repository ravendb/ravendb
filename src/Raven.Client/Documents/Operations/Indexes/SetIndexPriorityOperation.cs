using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class SetIndexPriorityOperation : IAdminOperation
    {
        private readonly string _indexName;
        private readonly IndexPriority _priority;

        public SetIndexPriorityOperation(string indexName, IndexPriority priority)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _priority = priority;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexPriorityCommand(_indexName, _priority);
        }

        private class SetIndexPriorityCommand : RavenCommand
        {
            private readonly string _indexName;
            private readonly IndexPriority _priority;

            public SetIndexPriorityCommand(string indexName, IndexPriority priority)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
                _priority = priority;
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-priority?name={Uri.EscapeDataString(_indexName)}&priority={_priority}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}