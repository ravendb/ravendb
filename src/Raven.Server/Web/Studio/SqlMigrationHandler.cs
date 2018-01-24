using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Migration;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class SqlMigrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sql-migration/schema", "GET", AuthorizationStatus.DatabaseAdmin)]
        public Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                DatabaseRecord databaseRecord;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                using (transactionOperationContext.OpenReadTransaction())
                {
                    databaseRecord = Server.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name);
                }

                string ConnectionStringName;
                ConnectionStringName = GetStringQueryString(nameof(ConnectionStringName));

                if (databaseRecord.SqlConnectionStrings.TryGetValue(ConnectionStringName, out var ConnectionString) == false)
                    throw new InvalidOperationException($"{nameof(ConnectionString)} with the name '{ConnectionStringName}' not found");

                IDbConnection connection;

                try
                {
                    connection = ConnectionFactory.OpenConnection(ConnectionString.ConnectionString);
                }
                catch (Exception e)
                {
                    WriteSchemaResponse(context, Error: "Cannot open connection using the given connection string. Error: " + e);
                    return Task.CompletedTask;
                }

                var database = new SqlDatabase(connection, ConnectionString.ConnectionString);

                var tableColumns = SqlDatabase.GetSchemaResultTablesColumns(connection);

                var tables = new List<SqlSchemaResultTable>();

                foreach (var table in database.GetAllTables())
                {
                    var columns = table.PrimaryKeys.Select(column => new SqlSchemaResultTable.Column
                        {
                            Name = column,
                            Type = SqlSchemaResultTable.Column.ColumnType.Primary
                        })
                        .ToList();

                    columns.AddRange(table.ForeignKeys.Keys.Select(column => new SqlSchemaResultTable.Column
                    {
                        Name = column,
                        Type = SqlSchemaResultTable.Column.ColumnType.Foreign
                    }));

                    foreach (var column in tableColumns[table.Name])
                    {
                        if (columns.Any(col => col.Name == column))
                            continue;

                        columns.Add(new SqlSchemaResultTable.Column
                        {
                            Name = column,
                            Type = SqlSchemaResultTable.Column.ColumnType.None
                        });
                    }

                    tables.Add(new SqlSchemaResultTable
                    {
                        Name = table.Name,
                        Columns = columns.ToArray(),
                        EmbeddedTables = table.ForeignKeys.Values.ToArray()
                    });
                }

                WriteSchemaResponse(context, tables.ToArray());
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/sql-migration/import", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                DatabaseRecord databaseRecord;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                using (transactionOperationContext.OpenReadTransaction())
                {
                    databaseRecord = Server.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name);
                }

                using (var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null))
                {
                    string ConnectionStringName;
                    if (sqlImportDoc.TryGet(nameof(ConnectionStringName), out ConnectionStringName) == false)
                        throw new InvalidOperationException($"'{nameof(ConnectionStringName)}' is a required field when asking for sql-migration");

                    if (databaseRecord.SqlConnectionStrings.TryGetValue(ConnectionStringName, out var ConnectionString) == false)
                        throw new InvalidOperationException($"{nameof(ConnectionString)} with the name '{ConnectionStringName}' not found");

                    IDbConnection connection;
                    
                    try
                    {
                        connection = ConnectionFactory.OpenConnection(ConnectionString.ConnectionString);
                    }
                    catch (Exception e)
                    {
                        WriteImportResponse(context, Errors: new SqlMigrationImportResult.Error
                        {
                            Type = SqlMigrationImportResult.Error.ErrorType.BadConnectionString,
                            Message = "Cannot open connection using the given connection string. Error: " + e
                        });
                        return;
                    }

                    BlittableJsonReaderArray Tables;
                    if (sqlImportDoc.TryGet(nameof(Tables), out Tables) == false)
                        throw new InvalidOperationException($"'{nameof(Tables)}' is a required field when asking for sql-migration");

                    var sqlMigrationTables = (from BlittableJsonReaderObject table in Tables.Items select JsonDeserializationServer.SqlMigrationTable(table)).ToList();
                    
                    var options = new SqlMigrationDocumentFactory.FactoryOptions();

                    bool BinaryToAttachment;
                    if (sqlImportDoc.TryGet(nameof(BinaryToAttachment), out BinaryToAttachment))
                        options.BinaryToAttachment = BinaryToAttachment;

                    bool TrimStrings;
                    if (sqlImportDoc.TryGet(nameof(TrimStrings), out TrimStrings))
                        options.TrimStrings = TrimStrings;

                    bool SkipUnsupportedTypes;
                    if (sqlImportDoc.TryGet(nameof(SkipUnsupportedTypes), out SkipUnsupportedTypes))
                        options.SkipUnsupportedTypes = SkipUnsupportedTypes;

                    int BatchSize;
                    if (sqlImportDoc.TryGet(nameof(BatchSize), out BatchSize))
                        options.BatchSize = BatchSize;

                    var factory = new SqlMigrationDocumentFactory(options);

                    var database = new SqlDatabase(connection, ConnectionString.ConnectionString, factory, context, sqlMigrationTables);

                    database.Validate(out var errors);

                    if (database.IsValid())
                    {
                        using (var writer = new SqlMigrationWriter(context, database))
                        {
                            await writer.WriteDatabase();
                        }
                    }

                    WriteImportResponse(context, errors.ToArray());
                }
            }
        }


        private void WriteSchemaResponse(DocumentsOperationContext context, SqlSchemaResultTable[] Tables = null, string Error = null)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                WriteTablesArray(nameof(Tables), Tables, writer);
                writer.WriteComma();

                writer.WritePropertyName(nameof(Error));
                writer.WriteString(Error);
                writer.WriteComma();

                var Success = Error == null;
                writer.WritePropertyName(nameof(Success));
                writer.WriteBool(Success);

                writer.WriteEndObject();
            }
        }

        private void WriteTablesArray(string name, SqlSchemaResultTable[] tables, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(name);
            writer.WriteStartArray();

            var first = true;

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

                WriteColumnsArray(nameof(table.Columns), table.Columns, writer);
                writer.WriteComma();

                writer.WriteArray(nameof(table.EmbeddedTables), table.EmbeddedTables);

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        private void WriteColumnsArray(string name, SqlSchemaResultTable.Column[] columns, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(name);
            writer.WriteStartArray();

            var first = true;

            foreach (var column in columns)
            {
                if (first)
                    first = false;
                else
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(column.Name));
                writer.WriteString(column.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(column.Type));
                writer.WriteString(column.Type.ToString());

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        private void WriteImportResponse(DocumentsOperationContext context, params SqlMigrationImportResult.Error[] Errors)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                WriteErrorsArray(nameof(Errors), Errors, writer);
              
                writer.WriteComma();

                var Success = Errors.Length == 0;

                writer.WritePropertyName(nameof(Success));
                writer.WriteBool(Success);

                writer.WriteEndObject();
            }
        }

        private void WriteErrorsArray(string name, SqlMigrationImportResult.Error[] errors, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(name);
            writer.WriteStartArray();

            var first = true;

            foreach (var error in errors)
            {
                if (first)
                    first = false;
                else
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(error.Type));
                writer.WriteString(error.Type.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(error.Message));
                writer.WriteString(error.Message);
                writer.WriteComma();

                writer.WritePropertyName(nameof(error.TableName));
                writer.WriteString(error.TableName);
                writer.WriteComma();

                writer.WritePropertyName(nameof(error.ColumnName));
                writer.WriteString(error.ColumnName);

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }
    }
}
