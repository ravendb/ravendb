using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
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
        [RavenAction("/databases/*/admin/sql-migration/schema", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var sourceSqlDatabaseBlittable = context.ReadForMemory(RequestBodyStream(), "source-database-info"))
            {
                var sourceSqlDatabase = JsonDeserializationServer.SourceSqlDatabase(sourceSqlDatabaseBlittable);

                var dbDriver = DatabaseDriverDispatcher.CreateDriver(sourceSqlDatabase.Provider, sourceSqlDatabase.ConnectionString);
                var schema = dbDriver.FindSchema();

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, schema.ToJson());
                }
            }

            return Task.CompletedTask;
        }
/* TODO
        [RavenAction("/databases//admin/sql-migration/import", "POST", AuthorizationStatus.DatabaseAdmin)]
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
        }*/
    }
}
