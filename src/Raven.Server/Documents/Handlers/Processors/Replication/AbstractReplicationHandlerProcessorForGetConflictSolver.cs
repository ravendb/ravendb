using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication;

internal abstract class AbstractReplicationHandlerProcessorForGetConflictSolver<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractReplicationHandlerProcessorForGetConflictSolver([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            ConflictSolver solverConfig;
            using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, GetDatabaseName()))
                solverConfig = rawRecord?.ConflictSolverConfiguration;

            if (solverConfig == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            var resolveByCollection = new DynamicJsonValue();
            foreach (var collection in solverConfig.ResolveByCollection)
                resolveByCollection[collection.Key] = collection.Value.ToJson();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(solverConfig.ResolveToLatest)] = solverConfig.ResolveToLatest,
                    [nameof(solverConfig.ResolveByCollection)] = resolveByCollection
                });
            }
        }
    }
}
