using System.Text;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;

namespace Raven.Server.Documents.Sharding
{
    public class ShardedRequestHandler : RequestHandler
    {
        public ShardedContext ShardedContext;
        public TransactionContextPool ContextPool;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            ShardedContext = context.ShardedContext;
            //TODO: We probably want to put it in the ShardedContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;
        }
    }
}
