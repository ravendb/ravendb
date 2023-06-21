using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.DataArchival
{
    internal abstract class AbstractDataArchivalHandlerProcessorForGet<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractDataArchivalHandlerProcessorForGet([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract DataArchivalConfiguration GetDataArchivalConfiguration();

        public override async ValueTask ExecuteAsync()
        {
            var archivalConfig = GetDataArchivalConfiguration();

            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (archivalConfig != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer, archivalConfig.ToJson());
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
