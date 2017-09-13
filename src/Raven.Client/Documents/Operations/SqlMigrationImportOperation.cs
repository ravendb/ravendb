using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class SqlMigrationImportOperation : IOperation<SqlMigrationImportResult>
    {
        private readonly string _connectionString;
        private readonly string _sqlDatabaseName;
        private readonly bool _binaryToAttachment;
        private readonly bool _includeSchema;
        private readonly bool _trimStrings;
        private readonly bool _skipUnsupportedTypes;
        private readonly int _batchSize;
        private readonly List<SqlMigrationTable> _tables;

        public SqlMigrationImportOperation(string connectionString, string sqlDatabaseName, List<SqlMigrationTable> tables, bool binaryToAttachment, bool includeSchema, bool trimStrings, bool skipUnsupportedTypes, int batchSize)
        {
            _connectionString = connectionString;
            _sqlDatabaseName = sqlDatabaseName;
            _binaryToAttachment = binaryToAttachment;
            _includeSchema = includeSchema;
            _trimStrings = trimStrings;
            _skipUnsupportedTypes = skipUnsupportedTypes;
            _batchSize = batchSize;
            _tables = tables;
        }

        public RavenCommand<SqlMigrationImportResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new SqlMigrationImportCommand(_connectionString, _sqlDatabaseName, _tables, _binaryToAttachment, _includeSchema, _trimStrings, _skipUnsupportedTypes, _batchSize);
        }

        public class SqlMigrationImportCommand : RavenCommand<SqlMigrationImportResult>
        {
            public override bool IsReadRequest => false;

            public readonly string ConnectionStringName;
            public readonly string SqlDatabaseName;
            public readonly List<SqlMigrationTable> Tables;
            public readonly bool BinaryToAttachment;
            public readonly bool IncludeSchema;
            public readonly bool TrimStrings;
            public readonly bool SkipUnsupportedTypes;
            public readonly int BatchSize;

            public SqlMigrationImportCommand(string connectionStringName, string sqlDatabaseName, List<SqlMigrationTable> tables, bool binaryToAttachment, bool includeSchema, bool trimStrings, bool skipUnsupportedTypes, int batchSize)
            {
                ConnectionStringName = connectionStringName;
                SqlDatabaseName = sqlDatabaseName;
                Tables = tables;
                BinaryToAttachment = binaryToAttachment;
                IncludeSchema = includeSchema;
                TrimStrings = trimStrings;
                SkipUnsupportedTypes = skipUnsupportedTypes;
                BatchSize = batchSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sql-migration/import";

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
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(BinaryToAttachment));
                            writer.WriteBool(BinaryToAttachment);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(IncludeSchema));
                            writer.WriteBool(IncludeSchema);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(TrimStrings));
                            writer.WriteBool(TrimStrings);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(SkipUnsupportedTypes));
                            writer.WriteBool(SkipUnsupportedTypes);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(BatchSize));
                            writer.WriteInteger(BatchSize);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(Tables));
                            WriteTablesArray(Tables, writer);

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            private void WriteTablesArray(List<SqlMigrationTable> tables, BlittableJsonTextWriter writer)
            {
                writer.WriteStartArray();

                if (tables != null)
                foreach (var table in tables)
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(table.Name));
                    writer.WriteString(table.Name);

                    writer.WritePropertyName(nameof(table.Query));
                    writer.WriteString(table.Query);

                    writer.WritePropertyName(nameof(table.Patch));
                    writer.WriteString(table.Patch);

                    writer.WritePropertyName(nameof(table.Property));
                    writer.WriteString(table.Property);

                    writer.WritePropertyName(nameof(table.EmbeddedTables));
                    WriteTablesArray(table.EmbeddedTables, writer);

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.SqlMigrationResult(response);
            }
        }

        public class SqlMigrationTable
        {
            public string Name;
            public string Query;
            public string Patch;
            public string Property;
            public List<SqlMigrationTable> EmbeddedTables;

            public SqlMigrationTable()
            {
            }

            public SqlMigrationTable(string name)
            {
                Name = name;
            }

            public static SqlMigrationTable ManualDeserializationFunc(BlittableJsonReaderObject item)
            {
                if (item == null)
                    return null;

                var table = new SqlMigrationTable();

                item.TryGet(nameof(table.Name), out table.Name);
                item.TryGet(nameof(table.Query), out table.Query);
                item.TryGet(nameof(table.Patch), out table.Patch);
                item.TryGet(nameof(table.Property), out table.Property);

                if (item.TryGet(nameof(table.EmbeddedTables), out BlittableJsonReaderArray embeddedTables) == false)
                    return table;

                table.EmbeddedTables = new List<SqlMigrationTable>();

                foreach (BlittableJsonReaderObject embeddedTable in embeddedTables)
                    table.EmbeddedTables.Add(ManualDeserializationFunc(embeddedTable));


                return table;
            }
        }
    }
}
