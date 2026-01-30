using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGenerateClassFromDocument<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForGenerateClassFromDocument([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetStringQueryString("id", required: false);
        var collection = RequestHandler.GetStringQueryString("collection", required: false);

        if(string.IsNullOrEmpty(id) && string.IsNullOrEmpty(collection))
            throw new InvalidOperationException("Either 'id' or 'collection' query parameters must be specified.");

        var lang = (RequestHandler.GetStringQueryString("lang", required: false) ?? "csharp")
            .Trim().ToLowerInvariant();

        await HandleClassGenerationAsync(id, collection, lang);
    }

    protected abstract ValueTask HandleClassGenerationAsync(string id, string collection, string lang);
}
