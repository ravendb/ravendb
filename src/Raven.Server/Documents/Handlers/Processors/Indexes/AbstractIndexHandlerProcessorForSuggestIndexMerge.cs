using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.IndexMerging;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForSuggestIndexMerge<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForSuggestIndexMerge([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract Dictionary<string, IndexDefinition> GetIndexes();

    public override async ValueTask ExecuteAsync()
    {
        var indexes = GetIndexes();

        var indexMerger = new IndexMerger(indexes);

        var indexMergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();

        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, indexMergeSuggestions.ToJson());
        }
    }
}
