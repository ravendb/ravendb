using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForResetEtl<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractOngoingTasksHandlerProcessorForResetEtl([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, object _, string raftRequestId)
        {
            var configurationName = RequestHandler.GetStringQueryString("configurationName"); // etl task name
            var transformationName = RequestHandler.GetStringQueryString("transformationName");

            return RequestHandler.ServerStore.RemoveEtlProcessState(context, databaseName, configurationName, transformationName, raftRequestId);
        }
    }
}
