using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class SqlMigrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sql-migration/schema", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var sourceSqlDatabaseBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "source-database-info"))
            {
                var sourceSqlDatabase = JsonDeserializationServer.SourceSqlDatabase(sourceSqlDatabaseBlittable);

                var dbDriver = DatabaseDriverDispatcher.CreateDriver(sourceSqlDatabase.Provider, sourceSqlDatabase.ConnectionString);
                var schema = dbDriver.FindSchema();

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, schema.ToJson());
                }
            }
        }

        [RavenAction("/databases/*/admin/sql-migration/import", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var sqlImportDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "sql-migration-request"))
                {
                    MigrationRequest migrationRequest;

                    // we can't use JsonDeserializationServer here as it doesn't support recursive processing
                    var serializer = DocumentConventions.DefaultForServer.Serialization.CreateDeserializer();
                    using (var blittableJsonReader = new BlittableJsonReader())
                    {
                        blittableJsonReader.Initialize(sqlImportDoc);
                        migrationRequest = serializer.Deserialize<MigrationRequest>(blittableJsonReader);
                    }

                    var operationId = Database.Operations.GetNextOperationId();

                    var sourceSqlDatabase = migrationRequest.Source;

                    var dbDriver = DatabaseDriverDispatcher.CreateDriver(sourceSqlDatabase.Provider, sourceSqlDatabase.ConnectionString);
                    var schema = dbDriver.FindSchema();
                    var token = CreateOperationToken();

                    var result = new MigrationResult(migrationRequest.Settings);

                    var collectionsCount = migrationRequest.Settings.Collections.Count;
                    var operationDescription = "Importing " + collectionsCount + " " + (collectionsCount == 1 ? "collection" : "collections") + " from SQL database: " + schema.CatalogName;

                    _ = Database.Operations.AddOperation(Database, operationDescription, Documents.Operations.Operations.OperationType.MigrationFromSql, onProgress =>
                    {
                        return Task.Run(async () =>
                        {
                            try
                            {
                                // allocate new context as we executed this async
                                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext migrationContext))
                                {
                                     await dbDriver.Migrate(migrationRequest.Settings, schema, Database, migrationContext, result, onProgress, token.Token);
                                }
                            }
                            catch (Exception e)
                            {
                                result.AddError($"Error occurred during import. Exception: {e.Message}");
                                onProgress.Invoke(result.Progress);
                                throw;
                            }

                             return (IOperationResult)result;
                        });
                    }, operationId, token: token);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }
                }
            }
        }

        [RavenAction("/databases/*/admin/sql-migration/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task TestSql()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var sqlImportTestDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "sql-migration-test-request"))
                {
                    MigrationTestRequest testRequest;

                    // we can't use JsonDeserializationServer here as it doesn't support recursive processing
                    var serializer = DocumentConventions.DefaultForServer.Serialization.CreateDeserializer();
                    using (var blittableJsonReader = new BlittableJsonReader())
                    {
                        blittableJsonReader.Initialize(sqlImportTestDoc);
                        testRequest = serializer.Deserialize<MigrationTestRequest>(blittableJsonReader);
                    }

                    var sourceSqlDatabase = testRequest.Source;

                    var dbDriver = DatabaseDriverDispatcher.CreateDriver(sourceSqlDatabase.Provider, sourceSqlDatabase.ConnectionString);
                    var schema = dbDriver.FindSchema();

                    var (testResultDocument, documentId) = dbDriver.Test(testRequest.Settings, schema, context);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName("DocumentId");
                        writer.WriteString(documentId);

                        writer.WriteComma();

                        writer.WritePropertyName("Document");
                        writer.WriteObject(testResultDocument);

                        writer.WriteEndObject();
                    }
                }
            }
        }
    }
}
