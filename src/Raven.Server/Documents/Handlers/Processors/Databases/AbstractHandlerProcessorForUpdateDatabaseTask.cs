using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Databases;

internal abstract class AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<object, TRequestHandler>
    where TRequestHandler : RequestHandler
{
    protected AbstractHandlerProcessorForUpdateDatabaseTask([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var databaseName = GetDatabaseName();

            await AssertCanExecuteAsync(databaseName);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                await Update(context, databaseName, writer);
            }
        }
    }
}
