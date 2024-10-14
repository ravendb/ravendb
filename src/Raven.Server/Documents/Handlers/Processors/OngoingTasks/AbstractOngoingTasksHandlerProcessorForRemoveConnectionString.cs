using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForRemoveConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        protected AbstractOngoingTasksHandlerProcessorForRemoveConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            var connectionStringName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "DELETE", $"Connection string '{connectionStringName}'");
            }

            return await RequestHandler.ServerStore.RemoveConnectionString(RequestHandler.DatabaseName, connectionStringName, type, raftRequestId);
        }
    }
}
