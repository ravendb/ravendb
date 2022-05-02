using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForResetEtl<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForResetEtl([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            var configurationName = RequestHandler.GetStringQueryString("configurationName"); // etl task name
            var transformationName = RequestHandler.GetStringQueryString("transformationName");

            return RequestHandler.ServerStore.RemoveEtlProcessState(context, RequestHandler.DatabaseName, configurationName, transformationName, raftRequestId);
        }
    }
}
