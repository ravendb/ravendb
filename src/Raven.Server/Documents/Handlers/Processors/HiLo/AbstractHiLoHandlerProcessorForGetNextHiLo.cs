using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.HiLo;

internal abstract class AbstractHiLoHandlerProcessorForGetNextHiLo<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHiLoHandlerProcessorForGetNextHiLo([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleGetNextHiLoAsync(string tag);

    public override ValueTask ExecuteAsync()
    {
        var tag = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");

        return HandleGetNextHiLoAsync(tag);
    }
}
