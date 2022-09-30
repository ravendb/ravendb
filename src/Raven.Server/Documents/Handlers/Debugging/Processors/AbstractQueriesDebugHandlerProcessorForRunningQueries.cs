using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.Processors;

internal abstract class AbstractQueriesDebugHandlerProcessorForRunningQueries<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractQueriesDebugHandlerProcessorForRunningQueries([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractQueryRunner GetQueryRunner();

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            var isFirst = true;
            foreach (var group in GetQueryRunner().CurrentlyRunningQueries.GroupBy(x => x.IndexName))
            {
                if (isFirst == false)
                    writer.WriteComma();
                isFirst = false;

                writer.WritePropertyName(group.Key);
                writer.WriteStartArray();

                var isFirstInternal = true;
                foreach (var query in group)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    query.Write(writer, context);
                }

                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }
    }
}
