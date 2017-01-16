using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Data.Indexes;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
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

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new SetIndexPriorityCommand(_indexName, _priority);
        }

        private class SetIndexPriorityCommand : RavenCommand<object>
        {
            private readonly string _indexName;
            private readonly IndexPriority _priority;

            public SetIndexPriorityCommand(string indexName, IndexPriority priority)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
                _priority = priority;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-priority?name={Uri.EscapeUriString(_indexName)}&priority={_priority}";

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