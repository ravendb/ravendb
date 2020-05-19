using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class AddDatabaseNodeOperation : IServerOperation<DatabasePutResult>
    {
        private readonly string _databaseName;
        private readonly string _node;

        public AddDatabaseNodeOperation(string databaseName, string node = null)
        {
            ResourceNameValidator.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _node = node;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddDatabaseNodeCommand(_databaseName, _node);
        }

        private class AddDatabaseNodeCommand : RavenCommand<DatabasePutResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly string _node;

            public AddDatabaseNodeCommand(string databaseName, string node)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                _databaseName = databaseName;
                _node = node;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/node?name={_databaseName}";
                if (string.IsNullOrEmpty(_node) == false)
                {
                    url += $"&node={_node}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
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
