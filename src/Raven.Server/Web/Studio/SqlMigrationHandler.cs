using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
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
        
        [RavenAction("/databases/*/admin/sql-migration/import", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var sqlImportDoc = context.ReadForMemory(RequestBodyStream(), "sql-migration-request"))
                {
                    //TODO: progress + convert this to operation
                    MigrationRequest migrationRequest;
                    
                    // we can't use JsonDeserializationServer here as it doesn't support recursive processing
                    var serializer = DocumentConventions.Default.CreateSerializer();
                    using (var blittableJsonReader = new BlittableJsonReader())
                    {
                        blittableJsonReader.Init(sqlImportDoc);
                        migrationRequest = serializer.Deserialize<MigrationRequest>(blittableJsonReader);
                    }
                    
                    var sourceSqlDatabase = migrationRequest.Source;
                    
                    var dbDriver = DatabaseDriverDispatcher.CreateDriver(sourceSqlDatabase.Provider, sourceSqlDatabase.ConnectionString);
                    var schema = dbDriver.FindSchema();
                    
                    await dbDriver.Migrate(migrationRequest.Settings, schema, Database, context);
                    
                    NoContentStatus();
                }
            }
        }
    }
}
