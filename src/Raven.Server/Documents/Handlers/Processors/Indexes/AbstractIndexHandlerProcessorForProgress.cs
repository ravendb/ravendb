using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForProgress<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexProgress[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<IndexProgress[]> CreateCommandForNode(string nodeTag)
    {
        StringValues names = GetNames();
        StringValues excludedNames = GetExcludedNames();
        bool exact = IsExact();
        return new GetIndexesProgressCommand(nodeTag, names, exact, excludedNames);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);

    // the excluded indexes are skipped entirely, so they do not consume the shared time budget of the request
    protected StringValues GetExcludedNames() => RequestHandler.GetStringValuesQueryString("excludeName", required: false);

    protected bool IsExact() => RequestHandler.GetBoolValueQueryString("exact", required: false) ?? false;
}
