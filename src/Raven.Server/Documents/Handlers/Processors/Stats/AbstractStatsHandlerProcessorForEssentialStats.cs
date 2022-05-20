using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats;

internal abstract class AbstractStatsHandlerProcessorForEssentialStats<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStatsHandlerProcessorForEssentialStats([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask<EssentialDatabaseStatistics> GetEssentialDatabaseStatisticsAsync(TOperationContext context);

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var stats = await GetEssentialDatabaseStatisticsAsync(context);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteEssentialDatabaseStatistics(context, stats);
        }
    }
}
