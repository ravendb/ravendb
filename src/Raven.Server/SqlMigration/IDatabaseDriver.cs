using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json;

namespace Raven.Server.SqlMigration
{
    public interface IDatabaseDriver
    {
        DatabaseSchema FindSchema();
        
        (BlittableJsonReaderObject Document, string Id) Test(MigrationTestSettings settings, DatabaseSchema dbSchema, DocumentsOperationContext context);
        
        Task Migrate(MigrationSettings settings, DatabaseSchema schema, DocumentDatabase db, DocumentsOperationContext context,
            MigrationResult result = null, Action<IOperationProgress> onProgress = null, CancellationToken token = default);
    }
}
