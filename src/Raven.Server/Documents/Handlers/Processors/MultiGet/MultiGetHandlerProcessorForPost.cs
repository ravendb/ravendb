using JetBrains.Annotations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Processors.MultiGet;

internal class MultiGetHandlerProcessorForPost : AbstractMultiGetHandlerProcessorForPost<DatabaseRequestHandler, DocumentsOperationContext>
{
    public MultiGetHandlerProcessorForPost([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override HandleRequest GetRequestHandler(RouteInformation routeInformation)
    {
        return routeInformation.GetRequestHandler();
    }

    protected override void FillRequestHandlerContext(RequestHandlerContext context)
    {
        context.Database = RequestHandler.Database;
    }
}
