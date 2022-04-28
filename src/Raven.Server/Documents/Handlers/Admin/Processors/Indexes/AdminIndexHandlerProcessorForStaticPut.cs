using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal class AdminIndexHandlerProcessorForStaticPut : AbstractAdminIndexHandlerProcessorForStaticPut<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForStaticPut([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexCreateController GetIndexCreateProcessor() => RequestHandler.Database.IndexStore.Create;

    protected override async ValueTask HandleLegacyIndexesAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
        await using (var stream = new ArrayStream(RequestHandler.RequestBodyStream(), nameof(DatabaseItemType.Indexes)))
        using (var source = new StreamSource(stream, jsonOperationContext, RequestHandler.DatabaseName))
        {
            var destination = new DatabaseDestination(RequestHandler.Database);
            var options = new DatabaseSmugglerOptionsServerSide
            {
                OperateOnTypes = DatabaseItemType.Indexes
            };

            var smuggler = SmugglerBase.GetDatabaseSmuggler(RequestHandler.Database, source, destination, RequestHandler.Database.Time, jsonOperationContext, options);
            await smuggler.ExecuteAsync();
        }
    }
}
