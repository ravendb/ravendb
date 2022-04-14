using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Expiration
{
    internal abstract class AbstractExpirationHandlerProcessorForGet<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractExpirationHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ExpirationConfiguration GetExpirationConfiguration();

        public override async ValueTask ExecuteAsync()
        {
            var expirationConfig = GetExpirationConfiguration();

            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (expirationConfig != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer, expirationConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }
    }
}
