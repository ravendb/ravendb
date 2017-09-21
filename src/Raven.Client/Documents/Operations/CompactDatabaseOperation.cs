using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class CompactDatabaseOperation : IOperation<OperationIdResult>
    {
        private readonly string _dbName;

        public CompactDatabaseOperation(string dbName)
        {
            _dbName = dbName ?? throw new ArgumentNullException(nameof(dbName));
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CompactDatabaseCommand(_dbName);
        }

        private class CompactDatabaseCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _dbName;

            public CompactDatabaseCommand(string dbName)
            {
                _dbName = dbName ?? throw new ArgumentNullException(nameof(dbName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/compact?name={_dbName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
