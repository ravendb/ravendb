using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio;

internal abstract class AbstractStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    internal AbstractStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var grouped = GetResults(context);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                var firstCollection = true;
                foreach (var (collection, tasks) in grouped)
                {
                    if (firstCollection == false)
                        writer.WriteComma();
                    firstCollection = false;

                    writer.WritePropertyName(collection);
                    writer.WriteStartObject();

                    var firstTask = true;
                    foreach (var (taskName, paths) in tasks)
                    {
                        if (firstTask == false)
                            writer.WriteComma();
                        firstTask = false;

                        writer.WritePropertyName(taskName);
                        writer.WriteArrayValue(paths);
                    }

                    writer.WriteEndObject();
                }
                writer.WriteEndObject();
            }
        }
    }

    protected abstract IEnumerable<EmbeddingsGenerationConfiguration> GetEmbeddingsTasks(TOperationContext context);

    private Dictionary<string, Dictionary<string, IEnumerable<string>>> GetResults(TOperationContext context)
    {
        return GetEmbeddingsTasks(context)
            .GroupBy(x => x.Collection)
            .ToDictionary(
                g => g.Key,
                g => g.ToDictionary(
                    x => x.Identifier,
                    x => (IEnumerable<string>)(x.EmbeddingsPathConfigurations?.Select(p => p.Path) ?? Enumerable.Empty<string>())
                )
            );
    }
}
