using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.HiLo;

internal abstract class AbstractHiLoHandlerProcessorForReturnHiLo<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHiLoHandlerProcessorForReturnHiLo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected string GetTag() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");

    protected long GetEnd() => RequestHandler.GetLongQueryString("end");

    protected long GetLast() => RequestHandler.GetLongQueryString("last");

    protected override RavenCommand CreateCommandForNode(string nodeTag)
    {
        var tag = GetTag();
        var last = GetLast();
        var end = GetEnd();

        return new HiLoReturnCommand(tag, last, end);
    }
}
