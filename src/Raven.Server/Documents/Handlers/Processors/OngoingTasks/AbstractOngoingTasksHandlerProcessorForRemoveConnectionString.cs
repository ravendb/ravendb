using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForRemoveConnectionString<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler>
        where TRequestHandler : RequestHandler
    {

        protected AbstractOngoingTasksHandlerProcessorForRemoveConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, object _, string raftRequestId)
        {
            var connectionStringName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            return await RequestHandler.ServerStore.RemoveConnectionString(databaseName, connectionStringName, type, raftRequestId);
        }
    }
}
