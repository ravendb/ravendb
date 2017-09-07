using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class SqlMigrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sql-schema", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);
                if (sqlImportDoc.TryGet("ConnectionString", out string connectionString) == false)
                    throw new InvalidOperationException("ConnectionString is a required field when asking for sql-schema");

                if (!ValidateConnection(connectionString))
                {
                    WriteRespone(new List<string>{ "Cannot open connection using the given connection string" }, context);
                    return Task.CompletedTask;
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Tables");
                    writer.WriteStartArray();

                    var first = true;

                    var connection = ConnectionFactory.OpenConnection(connectionString);

                    foreach (var item in SqlDatabase.GetAllTablesNamesFromDatabase(connection))
                    {
                        if (first == false)
                            writer.WriteComma();
                        else
                            first = false;

                        writer.WriteString(item);
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }


        [RavenAction("/databases/*/admin/sql-migration", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);

                if (sqlImportDoc.TryGet("ConnectionString", out string connectionString) == false)
                    throw new InvalidOperationException("'ConnectionString' is a required field when asking for sql-migration");

                if (!ValidateConnection(connectionString))
                {
                    WriteRespone(new List<string> { "Cannot open connection using the given connection string" }, context);
                    return;
                }

                if (sqlImportDoc.TryGet("Tables", out BlittableJsonReaderArray tablesFromUser) == false)
                    throw new InvalidOperationException("'Tables' is a required field when asking for sql-migration");

                var options = new RavenDocumentFactory.WriteOptions();

                if (sqlImportDoc.TryGet("BinaryToAttachment", out bool binaryToAttachment))
                    options.BinaryToAttachment = binaryToAttachment;

                if (sqlImportDoc.TryGet("IncludeSchema", out bool includeSchema))
                    options.IncludeSchema = includeSchema;

                if (sqlImportDoc.TryGet("TrimStrings", out bool trimStrings))
                    options.TrimStrings = trimStrings;

                if (sqlImportDoc.TryGet("SkipUnsupportedTypes", out bool skipUnsopportedTypes))
                    options.SkipUnsopportedTypes = skipUnsopportedTypes;

                var factory = new RavenDocumentFactory(options);

                var database = new SqlDatabase(connectionString, Database, factory, tablesFromUser);

                database.Validate(out var errors);

                if (database.IsValid())
                {
                    using (var writer = new RavenWriter(context, database))
                    {
                        writer.OnTableWritten += OnTableWritten;

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

                WriteRespone(errors, context);
            }
        }

        private bool ValidateConnection(string connectionString)
        {
            SqlConnection connection;

            try
            {
                connection = (SqlConnection) ConnectionFactory.OpenConnection(connectionString);
            }
            catch
            {
                return false;
            }

            connection.Dispose();
            return true;
        }

        private void WriteRespone(List<string> errors, DocumentsOperationContext context)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Errors");
                writer.WriteStartArray();

                var first = true;

                foreach (var item in errors)
                {
                    if (!first)
                        writer.WriteComma();
                    else
                        first = false;

                    writer.WriteString(item);
                }

                writer.WriteEndArray();
                writer.WriteComma();

                writer.WritePropertyName("Success");
                writer.WriteBool(errors.Count == 0);

                writer.WriteEndObject();
            }
        }

        private void OnTableWritten(string tableName, double time)
        {
        }
    }
}
