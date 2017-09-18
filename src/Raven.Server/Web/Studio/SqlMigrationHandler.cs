using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow.Json;

// ReSharper disable InconsistentNaming

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

                SqlConnection connection;

                try
                {
                    connection = (SqlConnection)ConnectionFactory.OpenConnection(ConnectionString.ConnectionString);
                }
                catch (Exception e)
                {
                    WriteSchemaResponse(context, Error: "Cannot open connection using the given connection string. Error: " + e);
                    return Task.CompletedTask;
                }

                var Tables = SqlDatabase.GetAllTablesNamesFromDatabase(connection);
                WriteSchemaResponse(context, Tables.ToArray());

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

                    SqlConnection connection;
                    
                    try
                    {
                        connection = (SqlConnection)ConnectionFactory.OpenConnection(ConnectionString.ConnectionString);
                    }
                    catch (Exception e)
                    {
                        WriteImportResponse(context, Errors: "Cannot open connection using the given connection string. Error: " + e);
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

                    bool IncludeSchema;
                    if (sqlImportDoc.TryGet(nameof(IncludeSchema), out IncludeSchema))
                        options.IncludeSchema = IncludeSchema;

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

                    var database = new SqlDatabase(connection, factory, sqlMigrationTables);

                    database.Validate(out var errors);

                    if (database.IsValid())
                    {
                        using (var writer = new SqlMigrationWriter(context, database))
                        {
                            try
                            {
                                await writer.WriteDatabase();
                            }
                            catch (Exception e)
                            {
                                errors.Add(e.Message);
                            }
                        }
                    }

                    WriteImportResponse(context, factory.ColumnsSkipped.ToArray(), errors.ToArray());
                }
            }
        }


        private void WriteSchemaResponse(DocumentsOperationContext context, string[] Tables = null, string Error = null)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(nameof(Tables), Tables);

                writer.WritePropertyName(nameof(Error));
                writer.WriteString(Error);

                var Success = Error == null;
                writer.WritePropertyName(nameof(Success));
                writer.WriteBool(Success);

                writer.WriteEndObject();
            }
        }

        private void WriteImportResponse(DocumentsOperationContext context, string[] ColumnsSkipped = null, params string[] Errors)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(nameof(Errors), Errors);
                writer.WriteArray(nameof(ColumnsSkipped), ColumnsSkipped);
              
                writer.WriteComma();

                var Success = Errors.Length == 0;

                writer.WritePropertyName(nameof(Success));
                writer.WriteBool(Success);

                writer.WriteEndObject();
            }
        }

    }
}
