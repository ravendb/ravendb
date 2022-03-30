using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Identities
{
    internal abstract class AbstractIdentityDebugHandlerProcessorForGetIdentities<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIdentityDebugHandlerProcessorForGetIdentities([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var identity in RequestHandler.ServerStore.Cluster.GetIdentitiesFromPrefix(context, GetDatabaseName(), start, pageSize))
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;
                        writer.WritePropertyName(identity.Prefix);
                        writer.WriteInteger(identity.Value);
                    }

                    writer.WriteEndObject();
                }
            }
        }
    }
}
