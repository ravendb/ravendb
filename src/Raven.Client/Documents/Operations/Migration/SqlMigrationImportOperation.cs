using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Migration
{
    public class SqlMigrationImportOperation : IMaintenanceOperation<SqlMigrationImportResult>
    {
        private readonly string _connectionString;
        private readonly bool _binaryToAttachment;
        private readonly bool _trimStrings;
        private readonly bool _skipUnsupportedTypes;
        private readonly int _batchSize;
        private readonly List<SqlMigrationTable> _tables;

        public SqlMigrationImportOperation(string connectionString, List<SqlMigrationTable> tables, bool binaryToAttachment, bool trimStrings, bool skipUnsupportedTypes, int batchSize)
        {
            _connectionString = connectionString;
            _binaryToAttachment = binaryToAttachment;
            _trimStrings = trimStrings;
            _skipUnsupportedTypes = skipUnsupportedTypes;
            _batchSize = batchSize;
            _tables = tables;
        }

        public RavenCommand<SqlMigrationImportResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SqlMigrationImportCommand(_connectionString, _tables, _binaryToAttachment, _trimStrings, _skipUnsupportedTypes, _batchSize);
        }

        private class SqlMigrationImportCommand : RavenCommand<SqlMigrationImportResult>
        {
            public override bool IsReadRequest => false;

            protected readonly string ConnectionStringName;
            protected readonly List<SqlMigrationTable> Tables;
            protected readonly bool BinaryToAttachment;
            protected readonly bool TrimStrings;
            protected readonly bool SkipUnsupportedTypes;
            protected readonly int BatchSize;

            public SqlMigrationImportCommand(string connectionStringName, List<SqlMigrationTable> tables, bool binaryToAttachment, bool trimStrings, bool skipUnsupportedTypes, int batchSize)
            {
                ConnectionStringName = connectionStringName;
                Tables = tables;
                BinaryToAttachment = binaryToAttachment;
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

                            writer.WritePropertyName(nameof(BinaryToAttachment));
                            writer.WriteBool(BinaryToAttachment);
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

                            WriteTablesArray(nameof(Tables), Tables, writer);

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            private void WriteTablesArray(string name, List<SqlMigrationTable> tables, BlittableJsonTextWriter writer)
            {
                writer.WritePropertyName(name);
                writer.WriteStartArray();

                var first = true;

                if (tables != null)
                {
                    foreach (var table in tables)
                    {
                        if (first)
                            first = false;
                        else
                            writer.WriteComma();

                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(table.Name));
                        writer.WriteString(table.Name);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(table.Query));
                        writer.WriteString(table.Query);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(table.Patch));
                        writer.WriteString(table.Patch);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(table.NewName));
                        writer.WriteString(table.NewName);
                        writer.WriteComma();

                        WriteTablesArray(nameof(table.EmbeddedTables), table.EmbeddedTables, writer);

                        writer.WriteEndObject();
                    }
                }

                writer.WriteEndArray();
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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
            public string NewName;
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
                item.TryGet(nameof(table.NewName), out table.NewName);

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
