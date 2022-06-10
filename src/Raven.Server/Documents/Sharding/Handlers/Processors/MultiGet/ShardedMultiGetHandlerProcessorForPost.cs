using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.MultiGet;

internal class ShardedMultiGetHandlerProcessorForPost : AbstractMultiGetHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedMultiGetHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override HandleRequest GetRequestHandler(RouteInformation routeInformation)
    {
        return routeInformation.GetShardedRequestHandler();
    }

    protected override void FillRequestHandlerContext(RequestHandlerContext context)
    {
        context.DatabaseContext = RequestHandler.DatabaseContext;
    }
}
