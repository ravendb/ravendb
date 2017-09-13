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
    public class SqlMigrationSchemaOperation : IOperation<SqlMigrationSchemaResult>
    {
        private readonly string _connectionStringName;
        private readonly string _sqlDatabaseName;

        public SqlMigrationSchemaOperation(string connectionStringName, string sqlDatabaseName)
        {
            if (string.IsNullOrWhiteSpace(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            if (string.IsNullOrWhiteSpace(sqlDatabaseName))
                throw new ArgumentNullException(nameof(sqlDatabaseName));

            _connectionStringName = connectionStringName;
            _sqlDatabaseName = sqlDatabaseName;
        }

        public RavenCommand<SqlMigrationSchemaResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new SqlMigrationSchemaCommand(_connectionStringName, _sqlDatabaseName);
        }

        public class SqlMigrationSchemaCommand : RavenCommand<SqlMigrationSchemaResult>
        {
            public readonly string ConnectionStringName;
            public readonly string SqlDatabaseName;
            public override bool IsReadRequest => false;

            public SqlMigrationSchemaCommand(string connectionStringName, string sqlDatabaseName)
            {
                if (string.IsNullOrWhiteSpace(connectionStringName))
                    throw new ArgumentNullException(nameof(connectionStringName));

                if (string.IsNullOrWhiteSpace(sqlDatabaseName))
                    throw new ArgumentNullException(nameof(sqlDatabaseName));

                ConnectionStringName = connectionStringName;
                SqlDatabaseName = sqlDatabaseName;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sql-migration/schema";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Post,

                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(ConnectionStringName));
                            writer.WriteString(ConnectionStringName);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(SqlDatabaseName));
                            writer.WriteString(SqlDatabaseName);

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
