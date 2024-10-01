using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Databases
{
    internal abstract class AbstractDatabaseHandlerProcessorForGetConfiguration<TRequestHandler, TOperationContext, TConfiguration> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TConfiguration : IDynamicJson
    {
        protected AbstractDatabaseHandlerProcessorForGetConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract TConfiguration GetConfiguration();

        public override async ValueTask ExecuteAsync()
        {
            var expirationConfig = GetConfiguration();

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
