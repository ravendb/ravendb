using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio;

internal sealed class StudioStatsHandlerProcessorForGetLicenseLimitsUsage<TOperationContext> : AbstractDatabaseHandlerProcessor<AbstractDatabaseRequestHandler<TOperationContext>, TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public StudioStatsHandlerProcessorForGetLicenseLimitsUsage([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            using (context.OpenReadTransaction())
            {
                var limits = DatabaseLicenseLimitsUsage.CreateFor(context, ServerStore, RequestHandler.DatabaseName);

                context.Write(writer, limits.ToJson());
            }
        }
    }
}
