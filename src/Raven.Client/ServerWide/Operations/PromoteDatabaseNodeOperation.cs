using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class PromoteDatabaseNodeOperation : IServerOperation<DatabasePutResult>
    {
        private readonly string _databaseName;
        private readonly string _node;

        public PromoteDatabaseNodeOperation(string databaseName, string node)
        {
            ResourceNameValidator.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _node = node;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new PromoteDatabaseNodeCommand(_databaseName, _node);
        }

        private class PromoteDatabaseNodeCommand : RavenCommand<DatabasePutResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly string _node;

            public PromoteDatabaseNodeCommand(string databaseName, string node)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                if (string.IsNullOrEmpty(node))
                    throw new ArgumentNullException(node);

                _databaseName = databaseName;
                _node = node;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/promote?name={_databaseName}&node={_node}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DatabasePutResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
