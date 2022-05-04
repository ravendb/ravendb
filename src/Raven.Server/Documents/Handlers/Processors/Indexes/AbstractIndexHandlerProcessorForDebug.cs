using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForDebug<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForDebug([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected string GetIndexName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

    protected string GetOperation() => RequestHandler.GetStringQueryString("op");

    protected StringValues GetDocIds() => RequestHandler.GetStringValuesQueryString("docId", required: false);

    protected string GetStartsWith() => RequestHandler.GetStringQueryString("startsWith", required: false);

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new GetIndexDebugCommand(GetIndexName(), GetOperation(), GetDocIds(), GetStartsWith(), RequestHandler.GetStart(), RequestHandler.GetPageSize(), nodeTag);
}
