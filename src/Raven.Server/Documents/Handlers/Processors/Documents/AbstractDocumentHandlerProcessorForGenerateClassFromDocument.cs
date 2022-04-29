using System.Threading.Tasks;
using JetBrains.Annotations;
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
        var id = RequestHandler.GetStringQueryString("id");
        var lang = (RequestHandler.GetStringQueryString("lang", required: false) ?? "csharp")
            .Trim().ToLowerInvariant();

        await HandleClassGeneration(id, lang);
    }

    protected abstract ValueTask HandleClassGeneration(string id, string lang);
}
