using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Migration
{
    public class SqlMigrationSchemaOperation : IMaintenanceOperation<SqlMigrationSchemaResult>
    {
        private readonly string _connectionStringName;

        public SqlMigrationSchemaOperation(string connectionStringName)
        {
            if (string.IsNullOrWhiteSpace(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            _connectionStringName = connectionStringName;
        }

        public RavenCommand<SqlMigrationSchemaResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SqlMigrationSchemaCommand(_connectionStringName);
        }

        private class SqlMigrationSchemaCommand : RavenCommand<SqlMigrationSchemaResult>
        {
            protected readonly string ConnectionStringName;
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

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.SqlSchemaResult(response);
            }
        }
    }
}
