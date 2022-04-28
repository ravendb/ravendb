using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Identities
{
    internal abstract class AbstractIdentityDebugHandlerProcessorForGetIdentities<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractIdentityDebugHandlerProcessorForGetIdentities([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var identity in RequestHandler.ServerStore.Cluster.GetIdentitiesFromPrefix(context, RequestHandler.DatabaseName, start, pageSize))
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
