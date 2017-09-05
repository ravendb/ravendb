using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class SqlMigrationOperation : IOperation<SqlMigrationResult>
    {
        private readonly string _connectionString;
        private readonly bool _binaryToAttachment;
        private readonly bool _includeSchema;
        private readonly bool _trimStrings;
        private readonly bool _skipUnsupportedColumns;
        private readonly List<SqlMigrationTable> _tables;

        public SqlMigrationOperation(string connectionString, bool binaryToAttachment, bool includeSchema, bool trimStrings, bool skipUnsupportedColumns, List<SqlMigrationTable> tables)
        {
            _connectionString = connectionString;
            _binaryToAttachment = binaryToAttachment;
            _includeSchema = includeSchema;
            _trimStrings = trimStrings;
            _skipUnsupportedColumns = skipUnsupportedColumns;
            _tables = tables;
        }

        public RavenCommand<SqlMigrationResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new SqlMigrationCommand(_connectionString, _binaryToAttachment, _includeSchema, _trimStrings, _skipUnsupportedColumns, _tables);
        }

        public class SqlMigrationCommand : RavenCommand<SqlMigrationResult>
        {
            public override bool IsReadRequest => false;

            private readonly string _connectionString;
            private readonly bool _binaryToAttachment;
            private readonly bool _includeSchema;
            private readonly bool _trimStrings;
            private readonly bool _skipUnsupportedColumns;
            private readonly List<SqlMigrationTable> _tables;

            public SqlMigrationCommand(string connectionString, bool binaryToAttachment, bool includeSchema, bool trimStrings, bool skipUnsupportedColumns, List<SqlMigrationTable> tables)
            {
                _connectionString = connectionString;
                _binaryToAttachment = binaryToAttachment;
                _includeSchema = includeSchema;
                _trimStrings = trimStrings;
                _skipUnsupportedColumns = skipUnsupportedColumns;
                _tables = tables;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/sql-migration";

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
                            writer.WriteComma();

                            writer.WritePropertyName("BinaryToAttachment");
                            writer.WriteBool(_binaryToAttachment);
                            writer.WriteComma();

                            writer.WritePropertyName("IncludeSchema");
                            writer.WriteBool(_includeSchema);
                            writer.WriteComma();

                            writer.WritePropertyName("TrimStrings");
                            writer.WriteBool(_trimStrings);
                            writer.WriteComma();

                            writer.WritePropertyName("SkipUnsupportedTypes");
                            writer.WriteBool(_skipUnsupportedColumns);
                            writer.WriteComma();

                            writer.WritePropertyName("Tables");
                            WriteTablesArray(_tables, writer);

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            private void WriteTablesArray(List<SqlMigrationTable> tables, BlittableJsonTextWriter writer)
            {
                writer.WriteStartArray();

                foreach (var table in tables)
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Name");
                    writer.WriteString(table.Name);

                    writer.WritePropertyName("Query");
                    writer.WriteString(table.Query);

                    writer.WritePropertyName("Patch");
                    writer.WriteString(table.Patch);

                    writer.WritePropertyName("Property");
                    writer.WriteString(table.Property);

                    writer.WritePropertyName("Embedded");
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
    }

    public class SqlMigrationTable
    {
        public readonly string Name;
        public string Query { get; set; } 
        public string Patch { get; set; } 
        public string Property { get; set; }
        public List<SqlMigrationTable> EmbeddedTables { get; set; }

        public SqlMigrationTable(string name)
        {
            Name = name;
            Query = string.Empty;
            Patch = string.Empty;
            Property = string.Empty;
            EmbeddedTables = new List<SqlMigrationTable>();
        }
    }
}
