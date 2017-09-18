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

        public SqlMigrationSchemaOperation(string connectionStringName)
        {
            if (string.IsNullOrWhiteSpace(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            _connectionStringName = connectionStringName;
        }

        public RavenCommand<SqlMigrationSchemaResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new SqlMigrationSchemaCommand(_connectionStringName);
        }

        public class SqlMigrationSchemaCommand : RavenCommand<SqlMigrationSchemaResult>
        {
            public readonly string ConnectionStringName;
            public override bool IsReadRequest => false;

            public SqlMigrationSchemaCommand(string connectionStringName)
            {
                if (string.IsNullOrWhiteSpace(connectionStringName))
                    throw new ArgumentNullException(nameof(connectionStringName));

                ConnectionStringName = connectionStringName;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sql-migration/schema?{nameof(ConnectionStringName)}={ConnectionStringName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get
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
