using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.HiLo;

internal abstract class AbstractHiLoHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHiLoHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleHiLoAsync(string tag);

    public override ValueTask ExecuteAsync()
    {
        var tag = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");

        return HandleHiLoAsync(tag);
    }
}
