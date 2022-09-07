using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.HiLo;

internal abstract class AbstractHiLoHandlerProcessorForReturnHiLo<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHiLoHandlerProcessorForReturnHiLo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected string GetTag() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");

    protected long GetEnd() => RequestHandler.GetLongQueryString("end");

    protected long GetLast() => RequestHandler.GetLongQueryString("last");
}
