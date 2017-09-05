using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
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
        [RavenAction("/databases/*/sql-schema", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);
                sqlImportDoc.TryGet("ConnectionString", out string connectionString);

                if (!ValidateConnection(connectionString, context))
                    return Task.CompletedTask;

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


        [RavenAction("/databases/*/sql-migration", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {
            Console.Clear();

            var sw = new Stopwatch();
            sw.Start();

            Console.WriteLine("Started...");
            Console.WriteLine();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);

                sqlImportDoc.TryGet("ConnectionString", out string connectionString);
                sqlImportDoc.TryGet("Tables", out BlittableJsonReaderArray tablesFromUser);
                sqlImportDoc.TryGet("BinaryToAttachment", out bool binaryToAttachment);
                sqlImportDoc.TryGet("IncludeSchema", out bool includeSchema);
                sqlImportDoc.TryGet("TrimStrings", out bool trimStrings);
                sqlImportDoc.TryGet("SkipUnsupportedTypes", out bool skipUnsopportedTypes);

                if (!ValidateConnection(connectionString, context))
                    return;

                var options = new RavenDocumentFactory.Options
                {
                    IncludeSchema = includeSchema,
                    BinaryToAttachment = binaryToAttachment,
                    TrimStrings = trimStrings,
                    SkipUnsopportedTypes = skipUnsopportedTypes
                };

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

                Console.WriteLine("Over all time: " + (double)sw.ElapsedMilliseconds / 1000);

                WriteRespone(errors, context);
            }
            SqlConnection.ClearAllPools();
        }

        private bool ValidateConnection(string connectionString, DocumentsOperationContext context)
        {
            SqlConnection connection;

            try
            {
                connection = (SqlConnection)ConnectionFactory.OpenConnection(connectionString);
            }
            catch
            {
                BadConnectionString(context);
                return false;
            }

            connection.Dispose();
            return true;
        }

        private void BadConnectionString(DocumentsOperationContext context)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Errors");
                writer.WriteStartArray();

                writer.WriteString("Cannot open connection using the given connection string");

                writer.WriteEndArray();

                writer.WriteComma();

                writer.WritePropertyName("Success");
                writer.WriteBool(false);

                writer.WriteEndObject();
            }
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
            Console.WriteLine($"'{tableName}' has written in {time}");
        }
    }
}
