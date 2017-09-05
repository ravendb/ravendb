using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class SqlSchemaOperation : IOperation<SqlSchemaResult>
    {
        private readonly string _connectionString;

        public SqlSchemaOperation(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            _connectionString = connectionString;
        }

        public RavenCommand<SqlSchemaResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new SqlSchemaCommand(_connectionString);
        }

        public class SqlSchemaCommand : RavenCommand<SqlSchemaResult>
        {
            private readonly string _connectionString;
            public override bool IsReadRequest => false;

            public SqlSchemaCommand(string connectionString)
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentNullException(nameof(connectionString));

                _connectionString = connectionString;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/sql-schema";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Post,

                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName("ConnectionString");
                            writer.WriteString(_connectionString);

                            writer.WriteEndObject();
                        }
                    })
                };
            
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.SqlSchemaResult(response);
            }
        }
    }
}
