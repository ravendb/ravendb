using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForGetListHubAccess<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractPullReplicationHandlerProcessorForGetListHubAccess([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = GetDatabaseName();
            var hub = RequestHandler.GetStringQueryString("name", true);
            var filter = RequestHandler.GetStringQueryString("filter", false);
            int pageSize = RequestHandler.GetPageSize();
            var start = RequestHandler.GetStart();

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var results = RequestHandler.Server.ServerStore.Cluster.GetReplicationHubCertificateByHub(context, databaseName, hub, filter, start, pageSize);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(nameof(ReplicationHubAccessResult.Results), results);
                    writer.WriteEndObject();
                }
            }
        }
    }
}
